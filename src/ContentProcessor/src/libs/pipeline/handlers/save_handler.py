# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import datetime
import json
import logging
import os

from libs.application.application_context import AppContext
from libs.models.content_process import ContentProcess, Step_Outputs
from libs.pipeline.entities.pipeline_file import ArtifactType, PipelineLogEntry
from libs.pipeline.entities.pipeline_message_context import MessageContext
from libs.pipeline.entities.pipeline_step_result import StepResult
from libs.pipeline.entities.schema import Schema
from libs.pipeline.handlers.logics.evaluate_handler.model import DataExtractionResult
from libs.pipeline.queue_handler_base import HandlerBase

logger = logging.getLogger(__name__)


class SaveHandler(HandlerBase):
    def __init__(self, appContext: AppContext, step_name: str, **data):
        super().__init__(appContext, step_name, **data)

    async def execute(self, context: MessageContext) -> StepResult:
        print(context.data_pipeline.get_previous_step_result(self.handler_name))

        #########################################################
        # Get Results from All Steps - Content Understanding
        #########################################################
        output_file_json_string_from_extract = self.download_output_file_to_json_string(
            processed_by="extract",
            artifact_type=ArtifactType.ExtractedContent,
        )

        ####################################################
        # Get the result from Map step handler - OpenAI
        ####################################################
        output_file_json_string_from_map = self.download_output_file_to_json_string(
            processed_by="map",
            artifact_type=ArtifactType.SchemaMappedData,
        )

        ##########################################################
        # Get the result from Evaluate step handler - Scored / Evaluated
        ##########################################################
        output_file_json_string_from_evaluate = (
            self.download_output_file_to_json_string(
                processed_by="evaluate",
                artifact_type=ArtifactType.ScoreMergedData,
            )
        )
        # Deserialize the result to ParsedChatCompletion
        evaluated_result = DataExtractionResult(
            **json.loads(output_file_json_string_from_evaluate)
        )

        ########################################################
        # Setup Output Result
        ########################################################
        def find_process_result(step_name: str):
            return next(
                (
                    result
                    for result in context.data_pipeline.pipeline_status.process_results
                    if result.step_name == step_name
                ),
                None,
            )

        process_outputs: list[Step_Outputs] = []
        process_outputs.append(
            Step_Outputs(
                step_name="extract",
                processed_time=find_process_result("extract").elapsed,
                step_result=json.loads(output_file_json_string_from_extract),
            )
        )
        process_outputs.append(
            Step_Outputs(
                step_name="map",
                processed_time=find_process_result("map").elapsed,
                step_result=json.loads(output_file_json_string_from_map),
            )
        )
        process_outputs.append(
            Step_Outputs(
                step_name="evaluate",
                processed_time=find_process_result("evaluate").elapsed,
                step_result=json.loads(output_file_json_string_from_evaluate),
            )
        )

        total_evaluated_fields_count = evaluated_result.confidence.get(
            "total_evaluated_fields_count", 0
        )
        schema_score = (
            0
            if total_evaluated_fields_count == 0
            else round(
                (
                    len(evaluated_result.comparison_result.items)
                    - evaluated_result.confidence["zero_confidence_fields_count"]
                )
                / len(evaluated_result.comparison_result.items),
                3,
            )
        )

        processed_result = ContentProcess(
            status=context.data_pipeline.pipeline_status.active_step,
            result=evaluated_result.extracted_result,
            process_id=context.data_pipeline.pipeline_status.process_id,
            processed_file_name=context.data_pipeline.get_source_files()[0].name,
            processed_file_mime_type=context.data_pipeline.get_source_files()[
                0
            ].mime_type,
            processed_time=self._summarize_processed_time(
                context.data_pipeline.pipeline_status.process_results
            ),
            imported_time=datetime.datetime.strptime(
                self._current_message_context.data_pipeline.pipeline_status.creation_time,
                "%Y-%m-%dT%H:%M:%S.%fZ",
            ),
            entity_score=evaluated_result.confidence["overall_confidence"],
            schema_score=schema_score,
            min_extracted_entity_score=evaluated_result.confidence[
                "min_extracted_field_confidence"
            ],
            prompt_tokens=evaluated_result.prompt_tokens,
            completion_tokens=evaluated_result.completion_tokens,
            target_schema=Schema.get_schema(
                schema_id=context.data_pipeline.pipeline_status.schema_id,
                connection_string=self.application_context.configuration.app_cosmos_connstr,
                database_name=self.application_context.configuration.app_cosmos_database,
                collection_name=self.application_context.configuration.app_cosmos_container_schema,
            ),
            confidence=evaluated_result.confidence,
            # process_output=process_outputs, # Mongo Document Size Limit, can't ship this result.
            extracted_comparison_data=evaluated_result.comparison_result,
            comment="",
        )

        # Save Result to Cosmos DB
        processed_result.update_status_to_cosmos(
            connection_string=self.application_context.configuration.app_cosmos_connstr,
            database_name=self.application_context.configuration.app_cosmos_database,
            collection_name=self.application_context.configuration.app_cosmos_container_process,
        )

        ##########################################################
        # HubSpot Sync (non-fatal — pipeline never breaks)
        ##########################################################
        self._sync_to_hubspot(processed_result, context)

        # save process_output to blob storage.
        processed_history = context.data_pipeline.add_file(
            file_name="step_outputs.json", artifact_type=ArtifactType.SavedContent
        )
        processed_history.log_entries.append(
            PipelineLogEntry(
                **{
                    "source": self.handler_name,
                    "message": "Process Output has been added. this file should be deserialized to Step_Outputs[]",
                }
            )
        )
        processed_history.upload_json_text(
            account_url=self.application_context.configuration.app_storage_blob_url,
            container_name=self.application_context.configuration.app_cps_processes,
            text=json.dumps([step.model_dump() for step in process_outputs]),
        )

        # Save Result as a file
        result_file = context.data_pipeline.add_file(
            file_name="save_output.json", artifact_type=ArtifactType.SavedContent
        )
        result_file.log_entries.append(
            PipelineLogEntry(
                **{
                    "source": self.handler_name,
                    "message": "Save Result has been added",
                }
            )
        )
        result_file.upload_json_text(
            account_url=self.application_context.configuration.app_storage_blob_url,
            container_name=self.application_context.configuration.app_cps_processes,
            text=processed_result.model_dump_json(),
        )

        return StepResult(
            process_id=context.data_pipeline.pipeline_status.process_id,
            step_name=self.handler_name,
            result={"result": result_file.name},
        )

    def _sync_to_hubspot(self, processed_result: ContentProcess, context: MessageContext):
        """
        Sync completed processing result to HubSpot.
        - FNOL documents   → HubSpot Ticket  (gated by HUBSPOT_ENABLE_FNOL_INTEGRATION=true)
        - All other docs   → HubSpot Deal    (existing underwriting pipeline)
        Non-fatal: any exception is logged and swallowed so the pipeline is never affected.
        """
        try:
            client_id     = os.environ.get("HUBSPOT_CLIENT_ID")
            client_secret = os.environ.get("HUBSPOT_CLIENT_SECRET")
            refresh_token = os.environ.get("HUBSPOT_REFRESH_TOKEN")

            if not all([client_id, client_secret, refresh_token]):
                logger.debug("HubSpot credentials not configured — skipping sync")
                return

            from libs.hubspot_integration import (
                sync_fnol_job_to_hubspot_ticket,
                sync_job_to_hubspot,
            )

            # Build job_data dict that hubspot_integration.py expects
            schema_name = ""
            if processed_result.target_schema:
                schema_name = getattr(processed_result.target_schema, "name", "") or ""

            job_data = {
                "id":              processed_result.process_id,
                "status":          "COMPLETE",
                "documentType":    schema_name,
                "insuranceType":   self._infer_insurance_type(schema_name),
                "extractedData":   processed_result.result,
                "analysis":        {},
                "analysisScoringJsonStr": json.dumps({
                    "total_score": 0
                }),
                "blobUrl":         None,
            }

            # Detect FNOL
            is_fnol = "FNOL" in schema_name.upper()
            enable_fnol = os.environ.get(
                "HUBSPOT_ENABLE_FNOL_INTEGRATION", "false"
            ).strip().lower() in ("1", "true", "yes")

            if is_fnol and enable_fnol:
                ticket_id = sync_fnol_job_to_hubspot_ticket(
                    job_data,
                    client_id,
                    client_secret,
                    refresh_token,
                    pipeline_id=os.environ.get("HUBSPOT_FNOL_PIPELINE_ID", "0"),
                    stage_id=os.environ.get("HUBSPOT_FNOL_STAGE_ID", "1"),
                )
                logger.info(f"HubSpot FNOL ticket created: {ticket_id}")
            else:
                stage_mapping = {
                    "COMPLETE": os.environ.get("HUBSPOT_UW_STAGE_COMPLETE", "1314763883"),
                    "FAILED":   os.environ.get("HUBSPOT_UW_STAGE_FAILED",   "1314763886"),
                }
                deal_id = sync_job_to_hubspot(
                    job_data,
                    client_id,
                    client_secret,
                    refresh_token,
                    pipeline_id=os.environ.get("HUBSPOT_UW_PIPELINE_ID", "877072527"),
                    stage_mapping=stage_mapping,
                )
                logger.info(f"HubSpot UW deal created/updated: {deal_id}")

        except Exception as e:
            logger.warning(f"HubSpot sync failed (non-fatal): {e}", exc_info=True)

    def _infer_insurance_type(self, schema_name: str) -> str:
        """Infer insurance type from schema name for HubSpot deal property."""
        schema_upper = schema_name.upper()
        if "LIFE" in schema_upper:
            return "life"
        if "MARINE" in schema_upper or "CARGO" in schema_upper or "FNOL" in schema_upper:
            return "marine"
        if "CYBER" in schema_upper:
            return "cyber"
        if "PROPERTY" in schema_upper or "COMMERCIAL" in schema_upper:
            return "property_casualty"
        return "other"

    def _summarize_processed_time(self, step_results: list[StepResult]) -> str:
        """
        Summarize the processed time of all steps in the pipeline.
        """
        total_processed_time = 0
        for step_result in step_results:
            step_processed_time = 0
            elapsed_time_parts = step_result.elapsed.split(":")
            if len(elapsed_time_parts) == 3:
                hours, minutes, seconds = map(float, elapsed_time_parts)
                step_processed_time = (
                    int(hours) * 3600 + int(minutes) * 60 + float(seconds)
                )
            else:
                step_processed_time = 0
            total_processed_time += step_processed_time

        total_hours = int(total_processed_time // 3600)
        total_minutes = int((total_processed_time % 3600) // 60)
        total_seconds = int(total_processed_time % 60)
        total_milliseconds = int(
            (total_processed_time - int(total_processed_time)) * 1000
        )
        formatted_elapsed_time = f"{total_hours:02}:{total_minutes:02}:{total_seconds:02}.{total_milliseconds:03}"
        return formatted_elapsed_time