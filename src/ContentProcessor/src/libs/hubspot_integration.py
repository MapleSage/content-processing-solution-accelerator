#!/usr/bin/env python3
"""
HubSpot Integration for Underwriting Workbench
Syncs completed underwriting jobs from Cosmos DB to HubSpot Deals
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HubSpotIntegration:
    """Manages HubSpot CRM integration for underwriting jobs"""
    
    def __init__(self, client_id: str, client_secret: str, refresh_token: str, 
                 pipeline_id: str, stage_mapping: Dict[str, str]):
        """
        Initialize HubSpot integration.
        
        Args:
            client_id: HubSpot OAuth client ID
            client_secret: HubSpot OAuth client secret
            refresh_token: HubSpot OAuth refresh token
            pipeline_id: ID of the Underwriting pipeline in HubSpot
            stage_mapping: Dict mapping job status to HubSpot stage IDs
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.access_token = None
        self.token_expires_at = 0
        self.pipeline_id = pipeline_id
        self.stage_mapping = stage_mapping
        self.base_url = "https://api.hubapi.com"
    
    def _get_access_token(self) -> str:
        """Get a valid access token, refreshing if necessary"""
        import time
        
        # Check if token is still valid (with 60 second buffer)
        if self.access_token and time.time() < (self.token_expires_at - 60):
            return self.access_token
        
        # Refresh the token
        try:
            response = requests.post(
                f"{self.base_url}/oauth/v1/token",
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data={
                    "grant_type": "refresh_token",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "refresh_token": self.refresh_token
                }
            )
            response.raise_for_status()
            token_data = response.json()
            
            self.access_token = token_data['access_token']
            self.token_expires_at = time.time() + token_data.get('expires_in', 1800)
            
            logger.info("HubSpot access token refreshed successfully")
            return self.access_token
            
        except Exception as e:
            logger.error(f"Error refreshing HubSpot token: {e}")
            raise
    
    @property
    def headers(self) -> Dict[str, str]:
        """Get headers with current access token"""
        return {
            "Authorization": f"Bearer {self._get_access_token()}",
            "Content-Type": "application/json"
        }
    
    def _calculate_risk_level(self, risk_score: float) -> str:
        """Calculate risk level from numeric score"""
        if risk_score >= 200:
            return "critical"
        elif risk_score >= 100:
            return "high"
        elif risk_score >= 50:
            return "moderate"
        else:
            return "low"
    
    def _extract_risk_flags(self, analysis: Dict[str, Any]) -> str:
        """Extract and format risk flags from analysis"""
        flags = []
        identified_risks = analysis.get('identified_risks', [])
        
        for risk in identified_risks:
            severity = risk.get('severity', 'unknown').upper()
            if severity in ['CRITICAL', 'HIGH']:
                description = risk.get('description', 'No description')
                flags.append(f"{severity}: {description}")
        
        return "\n".join(flags) if flags else "No critical or high risk flags"
    
    def create_or_update_deal(self, job_data: Dict[str, Any]) -> Optional[str]:
        """
        Create or update a HubSpot deal from job data with full data mining:
        - Extract and create Contact from document
        - Extract and create Company from document
        - Upload original PDF to HubSpot Files
        - Create Deal with all properties
        - Add detailed analysis notes
        - Associate Contact → Deal → Company
        
        Args:
            job_data: Job record from Cosmos DB
            
        Returns:
            Deal ID if successful, None otherwise
        """
        try:
            job_id = job_data.get('id')
            status = job_data.get('status', 'PROCESSING')
            
            # Parse analysis data
            analysis = job_data.get('analysis', {})
            if isinstance(analysis, str):
                try:
                    analysis = json.loads(analysis)
                except:
                    analysis = {}
            
            # Parse scoring data
            scoring_str = job_data.get('analysisScoringJsonStr', '{}')
            try:
                scoring = json.loads(scoring_str) if isinstance(scoring_str, str) else scoring_str
            except:
                scoring = {}
            
            # Parse extracted data
            extracted_data = job_data.get('extractedData', {})
            if isinstance(extracted_data, str):
                try:
                    extracted_data = json.loads(extracted_data)
                except:
                    extracted_data = {}
            
            risk_score = scoring.get('total_score', 0)
            risk_level = self._calculate_risk_level(risk_score)
            risk_flags = self._extract_risk_flags(analysis)
            
            # Map status to stage
            stage_id = self.stage_mapping.get(status, self.stage_mapping.get('PROCESSING'))
            
            # Map document type to lowercase for HubSpot enum
            doc_type = job_data.get('documentType', 'Application')
            doc_type_mapping = {
                'APPLICATION': 'application',
                'APS': 'aps',
                'MEDICAL_REPORT': 'aps',
                'LAB_REPORT': 'lab_report',
                'FNOL': 'fnol',
                'ACORD_FORM': 'application',
                'COMMERCIAL_PROPERTY_APPLICATION': 'application'
            }
            hubspot_doc_type = doc_type_mapping.get(doc_type.upper(), 'application')
            
            # STEP 1: Extract and create/update Contact
            contact_id = self._create_or_update_contact(extracted_data, job_id)
            
            # STEP 2: Extract and create/update Company
            company_id = self._create_or_update_company(extracted_data, job_id)
            
            # STEP 3: Upload original PDF to HubSpot Files
            file_id = self._upload_document_file(job_data)
            
            # STEP 4: Build deal properties
            properties = {
                "dealname": f"Underwriting - {job_id[:8]}",
                "pipeline": self.pipeline_id,
                "dealstage": stage_id,
                "risk_score": risk_score,
                "risk_level": risk_level,
                "insurance_type": job_data.get('insuranceType', 'life').lower(),
                "document_type": hubspot_doc_type,
                "risk_flags": risk_flags,
                "confidence_score": analysis.get('confidence_score', 0) * 100 if analysis.get('confidence_score') else 0,
                "source_job_id": job_id,
                "document_page_count": extracted_data.get('total_pages', 0)
            }
            
            # Check if deal already exists for this job
            existing_deal_id = self._find_deal_by_job_id(job_id)
            
            if existing_deal_id:
                # Update existing deal
                logger.info(f"Updating existing deal {existing_deal_id} for job {job_id}")
                response = requests.patch(
                    f"{self.base_url}/crm/v3/objects/deals/{existing_deal_id}",
                    headers=self.headers,
                    json={"properties": properties}
                )
                response.raise_for_status()
                deal_id = existing_deal_id
            else:
                # Create new deal
                logger.info(f"Creating new deal for job {job_id}")
                response = requests.post(
                    f"{self.base_url}/crm/v3/objects/deals",
                    headers=self.headers,
                    json={"properties": properties}
                )
                response.raise_for_status()
                deal_id = response.json()['id']
            
            # STEP 5: Create associations
            self._create_associations(deal_id, contact_id, company_id, file_id)
            
            # STEP 6: Add comprehensive analysis notes
            self._add_analysis_note(deal_id, job_data, analysis, scoring)
            
            logger.info(f"Successfully synced job {job_id} to HubSpot: Deal={deal_id}, Contact={contact_id}, Company={company_id}, File={file_id}")
            return deal_id
            
        except Exception as e:
            logger.error(f"Error creating/updating HubSpot deal: {e}")
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                logger.error(f"HubSpot API response: {e.response.text}")
            return None
    
    def _find_deal_by_job_id(self, job_id: str) -> Optional[str]:
        """Find existing deal by source_job_id"""
        try:
            # Search for deals with matching source_job_id
            response = requests.post(
                f"{self.base_url}/crm/v3/objects/deals/search",
                headers=self.headers,
                json={
                    "filterGroups": [{
                        "filters": [{
                            "propertyName": "source_job_id",
                            "operator": "EQ",
                            "value": job_id
                        }]
                    }],
                    "properties": ["source_job_id"],
                    "limit": 1
                }
            )
            response.raise_for_status()
            results = response.json().get('results', [])
            return results[0]['id'] if results else None
        except Exception as e:
            logger.warning(f"Error searching for existing deal: {e}")
            return None
    
    def _extract_contact_data(self, extracted_data: Dict[str, Any]) -> Dict[str, str]:
        """Extract contact information from document data"""
        contact_data = {}
        
        # Look through all pages for contact information
        pages = extracted_data.get('pages', [])
        for page in pages:
            key_values = page.get('key_values', {})
            
            # Common field mappings - includes broker/agent fields for P&C
            field_mappings = {
                'firstname': ['first_name', 'firstname', 'given_name', 'applicant_first_name', 'contact_first_name', 'broker_first_name', 'agent_first_name'],
                'lastname': ['last_name', 'lastname', 'surname', 'family_name', 'applicant_last_name', 'contact_last_name', 'broker_last_name', 'agent_last_name'],
                'email': ['email', 'email_address', 'applicant_email', 'contact_email', 'broker_email', 'agent_email'],
                'phone': ['phone', 'phone_number', 'telephone', 'mobile', 'applicant_phone', 'contact_phone', 'broker_phone', 'agent_phone'],
                'address': ['address', 'street_address', 'mailing_address', 'contact_address'],
                'city': ['city', 'town', 'contact_city'],
                'state': ['state', 'province', 'region', 'contact_state'],
                'zip': ['zip', 'zipcode', 'postal_code', 'postcode', 'contact_zip'],
                'date_of_birth': ['date_of_birth', 'dob', 'birth_date', 'birthdate'],
                'jobtitle': ['job_title', 'title', 'position', 'role', 'broker_title', 'agent_title']
            }
            
            for hs_field, possible_keys in field_mappings.items():
                if hs_field not in contact_data:  # Only set if not already found
                    for key in possible_keys:
                        # Case-insensitive search
                        for doc_key, value in key_values.items():
                            if key.lower() in doc_key.lower() and value:
                                contact_data[hs_field] = str(value)
                                break
                        if hs_field in contact_data:
                            break
            
            # Special handling for combined name fields (e.g., "Contact: Priya Sharma")
            for doc_key, value in key_values.items():
                doc_key_lower = doc_key.lower()
                value_str = str(value).strip()
                
                # Handle "Contact: FirstName LastName" or "Broker: FirstName LastName"
                if ('contact' in doc_key_lower or 'broker' in doc_key_lower or 'agent' in doc_key_lower) and value_str:
                    # Try to split into first and last name
                    if 'firstname' not in contact_data and 'lastname' not in contact_data:
                        parts = value_str.split()
                        if len(parts) >= 2:
                            contact_data['firstname'] = parts[0]
                            contact_data['lastname'] = ' '.join(parts[1:])
                
                # Handle email in combined format (e.g., "priya@techrisk.io")
                if 'email' not in contact_data and '@' in value_str and '.' in value_str:
                    # Basic email validation
                    if len(value_str.split('@')) == 2:
                        contact_data['email'] = value_str
        
        return contact_data
    
    def _create_or_update_contact(self, extracted_data: Dict[str, Any], job_id: str) -> Optional[str]:
        """Create or update contact from extracted document data"""
        try:
            contact_data = self._extract_contact_data(extracted_data)
            
            if not contact_data:
                logger.warning(f"No contact data found in extracted data for job {job_id}")
                return None
            
            # Search for existing contact by email
            email = contact_data.get('email')
            existing_contact_id = None
            
            if email:
                try:
                    response = requests.post(
                        f"{self.base_url}/crm/v3/objects/contacts/search",
                        headers=self.headers,
                        json={
                            "filterGroups": [{
                                "filters": [{
                                    "propertyName": "email",
                                    "operator": "EQ",
                                    "value": email
                                }]
                            }],
                            "properties": ["email"],
                            "limit": 1
                        }
                    )
                    response.raise_for_status()
                    results = response.json().get('results', [])
                    if results:
                        existing_contact_id = results[0]['id']
                except Exception as e:
                    logger.warning(f"Error searching for existing contact: {e}")
            
            if existing_contact_id:
                # Update existing contact
                logger.info(f"Updating existing contact {existing_contact_id}")
                response = requests.patch(
                    f"{self.base_url}/crm/v3/objects/contacts/{existing_contact_id}",
                    headers=self.headers,
                    json={"properties": contact_data}
                )
                response.raise_for_status()
                return existing_contact_id
            else:
                # Create new contact
                logger.info(f"Creating new contact from job {job_id}")
                response = requests.post(
                    f"{self.base_url}/crm/v3/objects/contacts",
                    headers=self.headers,
                    json={"properties": contact_data}
                )
                response.raise_for_status()
                contact_id = response.json()['id']
                logger.info(f"Created contact {contact_id}")
                return contact_id
                
        except Exception as e:
            logger.error(f"Error creating/updating contact: {e}", exc_info=True)
            return None
    
    def _extract_company_data(self, extracted_data: Dict[str, Any]) -> Dict[str, str]:
        """Extract company information from document data"""
        company_data = {}
        
        # Look through all pages for company information
        pages = extracted_data.get('pages', [])
        for page in pages:
            key_values = page.get('key_values', {})
            
            # Common field mappings - includes broker/agency fields for P&C
            field_mappings = {
                'name': ['company_name', 'business_name', 'organization', 'employer', 'company', 'broker', 'broker_name', 'agency', 'agency_name', 'brokerage', 'insured_name', 'applicant_company'],
                'domain': ['website', 'domain', 'company_website', 'broker_website', 'agency_website'],
                'phone': ['company_phone', 'business_phone', 'office_phone', 'broker_phone', 'agency_phone'],
                'address': ['company_address', 'business_address', 'office_address', 'broker_address', 'agency_address', 'insured_address', 'property_address'],
                'city': ['company_city', 'business_city', 'broker_city', 'agency_city', 'insured_city', 'property_city'],
                'state': ['company_state', 'business_state', 'broker_state', 'agency_state', 'insured_state', 'property_state'],
                'zip': ['company_zip', 'business_zip', 'broker_zip', 'agency_zip', 'insured_zip', 'property_zip'],
                'industry': ['industry', 'business_type', 'sector', 'business_classification', 'naics_code', 'sic_code']
            }
            
            for hs_field, possible_keys in field_mappings.items():
                if hs_field not in company_data:
                    for key in possible_keys:
                        for doc_key, value in key_values.items():
                            if key.lower() in doc_key.lower() and value:
                                company_data[hs_field] = str(value)
                                break
                        if hs_field in company_data:
                            break
            
            # Special handling for combined broker fields (e.g., "Broker: TechRisk Advisors LLC")
            for doc_key, value in key_values.items():
                doc_key_lower = doc_key.lower()
                value_str = str(value).strip()
                
                # Handle "Broker: Company Name" or "Agency: Company Name"
                if ('broker' in doc_key_lower or 'agency' in doc_key_lower or 'brokerage' in doc_key_lower) and value_str:
                    if 'name' not in company_data and len(value_str) > 3:  # Avoid single-word values
                        company_data['name'] = value_str
                
                # Handle email domain extraction for website
                if 'domain' not in company_data and '@' in value_str:
                    # Extract domain from email (e.g., "priya@techrisk.io" -> "techrisk.io")
                    parts = value_str.split('@')
                    if len(parts) == 2:
                        domain = parts[1].strip()
                        if '.' in domain:
                            company_data['domain'] = domain
        
        return company_data
    
    def _create_or_update_company(self, extracted_data: Dict[str, Any], job_id: str) -> Optional[str]:
        """Create or update company from extracted document data"""
        try:
            company_data = self._extract_company_data(extracted_data)
            
            if not company_data or 'name' not in company_data:
                logger.warning(f"No company data found in extracted data for job {job_id}")
                return None
            
            # Search for existing company by name
            company_name = company_data.get('name')
            existing_company_id = None
            
            try:
                response = requests.post(
                    f"{self.base_url}/crm/v3/objects/companies/search",
                    headers=self.headers,
                    json={
                        "filterGroups": [{
                            "filters": [{
                                "propertyName": "name",
                                "operator": "EQ",
                                "value": company_name
                            }]
                        }],
                        "properties": ["name"],
                        "limit": 1
                    }
                )
                response.raise_for_status()
                results = response.json().get('results', [])
                if results:
                    existing_company_id = results[0]['id']
            except Exception as e:
                logger.warning(f"Error searching for existing company: {e}")
            
            if existing_company_id:
                # Update existing company
                logger.info(f"Updating existing company {existing_company_id}")
                response = requests.patch(
                    f"{self.base_url}/crm/v3/objects/companies/{existing_company_id}",
                    headers=self.headers,
                    json={"properties": company_data}
                )
                response.raise_for_status()
                return existing_company_id
            else:
                # Create new company
                logger.info(f"Creating new company from job {job_id}")
                response = requests.post(
                    f"{self.base_url}/crm/v3/objects/companies",
                    headers=self.headers,
                    json={"properties": company_data}
                )
                response.raise_for_status()
                company_id = response.json()['id']
                logger.info(f"Created company {company_id}")
                return company_id
                
        except Exception as e:
            logger.error(f"Error creating/updating company: {e}", exc_info=True)
            return None
    
    def _upload_document_file(self, job_data: Dict[str, Any]) -> Optional[str]:
        """Upload original PDF document to HubSpot Files"""
        try:
            # Get blob URL from job data
            blob_url = job_data.get('blobUrl')
            if not blob_url:
                logger.warning("No blob URL found in job data")
                return None
            
            job_id = job_data.get('id')
            
            # Download PDF from blob storage
            import requests as req
            pdf_response = req.get(blob_url)
            pdf_response.raise_for_status()
            pdf_content = pdf_response.content
            
            # Upload to HubSpot Files API
            file_name = f"underwriting_{job_id[:8]}.pdf"
            
            # HubSpot Files API requires multipart/form-data
            files = {
                'file': (file_name, pdf_content, 'application/pdf')
            }
            
            data = {
                'options': json.dumps({
                    'access': 'PRIVATE',
                    'overwrite': False,
                    'duplicateValidationStrategy': 'NONE',
                    'duplicateValidationScope': 'EXACT_FOLDER'
                })
            }
            
            # Use different headers for file upload (no Content-Type, let requests set it)
            upload_headers = {
                "Authorization": f"Bearer {self._get_access_token()}"
            }
            
            response = requests.post(
                f"{self.base_url}/files/v3/files",
                headers=upload_headers,
                files=files,
                data=data
            )
            response.raise_for_status()
            
            file_id = response.json()['id']
            logger.info(f"Uploaded document to HubSpot Files: {file_id}")
            return file_id
            
        except Exception as e:
            logger.error(f"Error uploading document to HubSpot: {e}", exc_info=True)
            return None
    
    def _create_associations(self, deal_id: str, contact_id: Optional[str], 
                           company_id: Optional[str], file_id: Optional[str]):
        """Create associations between Deal, Contact, Company, and File"""
        try:
            # Associate Contact to Deal
            if contact_id:
                try:
                    response = requests.put(
                        f"{self.base_url}/crm/v4/objects/deals/{deal_id}/associations/contacts/{contact_id}",
                        headers=self.headers,
                        json=[{
                            "associationCategory": "HUBSPOT_DEFINED",
                            "associationTypeId": 3  # Deal to Contact
                        }]
                    )
                    response.raise_for_status()
                    logger.info(f"Associated contact {contact_id} to deal {deal_id}")
                except Exception as e:
                    logger.error(f"Error associating contact to deal: {e}")
            
            # Associate Company to Deal
            if company_id:
                try:
                    response = requests.put(
                        f"{self.base_url}/crm/v4/objects/deals/{deal_id}/associations/companies/{company_id}",
                        headers=self.headers,
                        json=[{
                            "associationCategory": "HUBSPOT_DEFINED",
                            "associationTypeId": 5  # Deal to Company
                        }]
                    )
                    response.raise_for_status()
                    logger.info(f"Associated company {company_id} to deal {deal_id}")
                except Exception as e:
                    logger.error(f"Error associating company to deal: {e}")
            
            # Associate Contact to Company
            if contact_id and company_id:
                try:
                    response = requests.put(
                        f"{self.base_url}/crm/v4/objects/contacts/{contact_id}/associations/companies/{company_id}",
                        headers=self.headers,
                        json=[{
                            "associationCategory": "HUBSPOT_DEFINED",
                            "associationTypeId": 1  # Contact to Company
                        }]
                    )
                    response.raise_for_status()
                    logger.info(f"Associated contact {contact_id} to company {company_id}")
                except Exception as e:
                    logger.error(f"Error associating contact to company: {e}")
            
            # Associate File to Deal (if file was uploaded)
            if file_id:
                try:
                    # Note: File associations may require different API endpoint
                    # This is a placeholder - HubSpot file associations work differently
                    logger.info(f"File {file_id} uploaded but association to deal requires manual linking in HubSpot UI")
                except Exception as e:
                    logger.error(f"Error associating file to deal: {e}")
                    
        except Exception as e:
            logger.error(f"Error creating associations: {e}", exc_info=True)
    
    def _add_analysis_note(self, deal_id: str, job_data: Dict[str, Any], 
                          analysis: Dict[str, Any], scoring: Dict[str, Any]):
        """Add comprehensive AI analysis as notes attached to the deal"""
        try:
            import time
            timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
            risk_score = scoring.get('total_score', 0)
            risk_level = self._calculate_risk_level(risk_score)
            
            # Parse action result for decision details
            action_result = job_data.get('actionResult', {})
            if isinstance(action_result, str):
                try:
                    action_result = json.loads(action_result)
                except:
                    action_result = {}
            
            # Format overall summary
            overall_summary = analysis.get('overall_summary', 'No summary available')
            
            # Format identified risks
            identified_risks = analysis.get('identified_risks', [])
            risks_text = []
            for risk in identified_risks:
                severity = risk.get('severity', 'unknown').upper()
                category = risk.get('category', 'unknown')
                description = risk.get('description', 'No description')
                risks_text.append(f"- [{severity}] {category}: {description}")
            
            risks_formatted = "\n".join(risks_text) if risks_text else "- No significant risks identified"
            
            # Format impairment scores
            impairment_scores = scoring.get('impairment_scores', [])
            scores_text = []
            for imp_score in impairment_scores[:10]:  # Top 10
                imp_name = imp_score.get('impairment', 'Unknown')
                score = imp_score.get('score', 0)
                reason = imp_score.get('reason', 'No reason provided')
                scores_text.append(f"- {imp_name}: {score} points\n  Reason: {reason}")
            
            scores_formatted = "\n".join(scores_text) if scores_text else "- No impairments scored"
            
            # Format discrepancies
            discrepancies = analysis.get('discrepancies', [])
            discrepancies_text = []
            for disc in discrepancies:
                field = disc.get('field', 'Unknown field')
                issue = disc.get('issue', 'No issue description')
                discrepancies_text.append(f"- {field}: {issue}")
            
            discrepancies_formatted = "\n".join(discrepancies_text) if discrepancies_text else "- No discrepancies found"
            
            # Format missing information
            missing_info = analysis.get('missing_information', [])
            missing_formatted = "\n".join([f"- {item}" for item in missing_info]) if missing_info else "- None"
            
            # Format final recommendation
            final_recommendation = analysis.get('final_recommendation', 'Manual review required')
            
            # Format underwriter decision
            decision = action_result.get('decision', 'pending')
            action = action_result.get('action', 'unknown')
            ineligibility_reason = action_result.get('ineligibility_reason', '')
            required_docs = action_result.get('required_documents', [])
            
            decision_text = f"Decision: {decision.upper()}\nAction: {action}"
            if ineligibility_reason:
                decision_text += f"\nIneligibility Reason: {ineligibility_reason}"
            if required_docs:
                decision_text += f"\nRequired Documents:\n" + "\n".join([f"  - {doc}" for doc in required_docs])
            
            # Build comprehensive note content
            note_content = f"""AI UNDERWRITING ANALYSIS
Generated: {timestamp}

═══════════════════════════════════════════════════════════════

RISK ASSESSMENT
Risk Score: {risk_score}
Risk Level: {risk_level.upper()}
Confidence: {analysis.get('confidence_score', 0) * 100:.1f}%

═══════════════════════════════════════════════════════════════

EXECUTIVE SUMMARY
{overall_summary}

═══════════════════════════════════════════════════════════════

IDENTIFIED RISKS
{risks_formatted}

═══════════════════════════════════════════════════════════════

IMPAIRMENT SCORING
{scores_formatted}

═══════════════════════════════════════════════════════════════

DISCREPANCIES
{discrepancies_formatted}

═══════════════════════════════════════════════════════════════

MISSING INFORMATION
{missing_formatted}

═══════════════════════════════════════════════════════════════

RECOMMENDATION
{final_recommendation}

═══════════════════════════════════════════════════════════════

UNDERWRITER DECISION
{decision_text}

═══════════════════════════════════════════════════════════════

Source Job ID: {job_data.get('id')}
Document Type: {job_data.get('documentType', 'Unknown')}
Insurance Type: {job_data.get('insuranceType', 'Unknown')}
Pages Processed: {job_data.get('extractedData', {}).get('total_pages', 0)}
"""
            
            # Create note - use current timestamp in milliseconds
            note_timestamp = int(time.time() * 1000)
            
            response = requests.post(
                f"{self.base_url}/crm/v3/objects/notes",
                headers=self.headers,
                json={
                    "properties": {
                        "hs_note_body": note_content,
                        "hs_timestamp": str(note_timestamp)
                    },
                    "associations": [{
                        "to": {"id": deal_id},
                        "types": [{
                            "associationCategory": "HUBSPOT_DEFINED",
                            "associationTypeId": 214  # Note to Deal association
                        }]
                    }]
                }
            )
            response.raise_for_status()
            logger.info(f"Added comprehensive analysis note to deal {deal_id}")
            
        except Exception as e:
            logger.error(f"Error adding note to deal: {e}", exc_info=True)

    def _extract_flat_key_values(self, extracted_data: Dict[str, Any]) -> Dict[str, str]:
        """Flatten extracted page key-values to a single lowercase map."""
        flat: Dict[str, str] = {}
        for page in extracted_data.get('pages', []):
            key_values = page.get('key_values', {}) or {}
            for key, value in key_values.items():
                if key is None or value is None:
                    continue
                key_normalized = str(key).strip().lower().replace(" ", "_")
                value_str = str(value).strip()
                if key_normalized and value_str and key_normalized not in flat:
                    flat[key_normalized] = value_str
        return flat

    def _first_value(self, flat_map: Dict[str, str], keys: list) -> Optional[str]:
        """Return the first present and non-empty value for candidate keys."""
        for key in keys:
            value = flat_map.get(key)
            if value:
                return value
        return None

    def _calculate_fnol_priority(self, risk_score: float) -> str:
        """Map risk score to HubSpot ticket priority."""
        if risk_score >= 200:
            return "HIGH"
        if risk_score >= 100:
            return "MEDIUM"
        return "LOW"

    def _associate_ticket_to_contact(self, ticket_id: str, contact_id: str):
        """Associate a ticket to a contact."""
        try:
            response = requests.put(
                f"{self.base_url}/crm/v3/objects/tickets/{ticket_id}/associations/contacts/{contact_id}/16",
                headers={"Authorization": f"Bearer {self._get_access_token()}"}
            )
            response.raise_for_status()
            logger.info(f"Associated ticket {ticket_id} with contact {contact_id}")
        except Exception as e:
            logger.error(f"Error associating ticket {ticket_id} to contact {contact_id}: {e}")

    def _add_fnol_extraction_note(self, ticket_id: str, job_data: Dict[str, Any], fnol_fields: Dict[str, str]):
        """Attach extraction/audit details to a FNOL ticket."""
        try:
            note_data = {
                "engagement": {
                    "active": True,
                    "type": "NOTE",
                    "timestamp": int(datetime.utcnow().timestamp() * 1000)
                },
                "associations": {
                    "ticketIds": [int(ticket_id)]
                },
                "metadata": {
                    "body": (
                        "FNOL Document Extraction Results\n\n"
                        f"Job ID: {job_data.get('id', '')}\n"
                        f"Document Type: {job_data.get('documentType', '')}\n"
                        f"Insurance Type: {job_data.get('insuranceType', '')}\n"
                        f"Incident Date: {fnol_fields.get('incident_date', 'N/A')}\n"
                        f"Policy Number: {fnol_fields.get('policy_number', 'N/A')}\n"
                        f"Estimated Loss: {fnol_fields.get('estimated_loss_amount', 'N/A')}\n\n"
                        "Raw key values (truncated):\n"
                        f"{json.dumps(fnol_fields, indent=2)[:2000]}"
                    )
                }
            }
            response = requests.post(
                f"{self.base_url}/engagements/v1/engagements",
                headers=self.headers,
                json=note_data
            )
            response.raise_for_status()
            logger.info(f"Added FNOL extraction note to ticket {ticket_id}")
        except Exception as e:
            logger.warning(f"Failed to add FNOL note to ticket {ticket_id}: {e}")

    def create_fnol_ticket(self, job_data: Dict[str, Any], pipeline_id: str, stage_id: str) -> Optional[str]:
        """
        Create a HubSpot ticket from FNOL-style extracted data.

        Returns:
            Ticket ID if successful, else None.
        """
        try:
            job_id = job_data.get('id', '')
            extracted_data = job_data.get('extractedData', {})
            if isinstance(extracted_data, str):
                try:
                    extracted_data = json.loads(extracted_data)
                except Exception:
                    extracted_data = {}

            analysis = job_data.get('analysis', {})
            if isinstance(analysis, str):
                try:
                    analysis = json.loads(analysis)
                except Exception:
                    analysis = {}

            scoring = job_data.get('analysisScoringJsonStr', {})
            if isinstance(scoring, str):
                try:
                    scoring = json.loads(scoring)
                except Exception:
                    scoring = {}

            flat = self._extract_flat_key_values(extracted_data)

            policy_number = self._first_value(flat, ["policy_number", "policy_no", "policy_id"])
            fnol_reference = self._first_value(flat, ["fnol_reference", "claim_reference", "claim_number"])
            incident_date = self._first_value(flat, ["incident_date", "date_of_loss", "loss_date"])
            estimated_loss_amount = self._first_value(flat, ["estimated_loss", "estimated_loss_amount", "loss_amount"])
            incident_type = self._first_value(flat, ["incident_type", "loss_type", "cause_of_loss"])
            location = self._first_value(flat, ["location", "incident_location", "loss_location"])

            risk_score = scoring.get('total_score', 0)
            priority = self._calculate_fnol_priority(float(risk_score or 0))

            subject_ref = fnol_reference or policy_number or (job_id[:8] if job_id else "new-claim")
            ticket_content = (
                "FNOL Intake\n\n"
                f"Job ID: {job_id}\n"
                f"Policy Number: {policy_number or 'N/A'}\n"
                f"FNOL Reference: {fnol_reference or 'N/A'}\n"
                f"Incident Date: {incident_date or 'N/A'}\n"
                f"Incident Type: {incident_type or 'N/A'}\n"
                f"Location: {location or 'N/A'}\n"
                f"Estimated Loss: {estimated_loss_amount or 'N/A'}\n"
                f"Risk Score: {risk_score}\n"
                f"Confidence: {analysis.get('confidence_score', 0) * 100:.1f}%\n"
                f"Source Job: {job_id}"
            )

            base_properties = {
                "subject": f"FNOL - {subject_ref}",
                "content": ticket_content,
                "hs_pipeline": pipeline_id,
                "hs_pipeline_stage": stage_id,
                "hs_ticket_priority": priority
            }

            optional_properties = {
                "source_job_id": job_id,
                "policy_number": policy_number,
                "fnol_reference": fnol_reference,
                "incident_date": incident_date,
                "incident_type": incident_type,
                "estimated_loss_amount": estimated_loss_amount,
                "loss_location": location
            }
            full_properties = {
                **base_properties,
                **{k: v for k, v in optional_properties.items() if v not in (None, "")}
            }

            response = requests.post(
                f"{self.base_url}/crm/v3/objects/tickets",
                headers=self.headers,
                json={"properties": full_properties}
            )

            if response.status_code >= 400:
                logger.warning(
                    "FNOL ticket create with custom properties failed (%s), retrying with base properties",
                    response.status_code
                )
                response = requests.post(
                    f"{self.base_url}/crm/v3/objects/tickets",
                    headers=self.headers,
                    json={"properties": base_properties}
                )

            response.raise_for_status()
            ticket_id = response.json().get('id')
            if not ticket_id:
                logger.error("HubSpot ticket create response missing id")
                return None

            contact_id = self._create_or_update_contact(extracted_data, job_id)
            if contact_id:
                self._associate_ticket_to_contact(ticket_id, contact_id)

            self._add_fnol_extraction_note(ticket_id, job_data, flat)
            logger.info(f"Successfully synced FNOL job {job_id} to HubSpot ticket {ticket_id}")
            return ticket_id

        except Exception as e:
            logger.error(f"Error creating FNOL ticket: {e}", exc_info=True)
            return None


def sync_job_to_hubspot(job_data: Dict[str, Any], 
                        client_id: str,
                        client_secret: str,
                        refresh_token: str,
                        pipeline_id: str,
                        stage_mapping: Dict[str, str]) -> Optional[str]:
    """
    Sync a single job to HubSpot.
    
    Args:
        job_data: Job record from Cosmos DB
        client_id: HubSpot OAuth client ID
        client_secret: HubSpot OAuth client secret
        refresh_token: HubSpot OAuth refresh token
        pipeline_id: Underwriting pipeline ID
        stage_mapping: Status to stage ID mapping
        
    Returns:
        Deal ID if successful, None otherwise
    """
    integration = HubSpotIntegration(client_id, client_secret, refresh_token, 
                                     pipeline_id, stage_mapping)
    return integration.create_or_update_deal(job_data)


def sync_fnol_job_to_hubspot_ticket(job_data: Dict[str, Any],
                                    client_id: str,
                                    client_secret: str,
                                    refresh_token: str,
                                    pipeline_id: str,
                                    stage_id: str) -> Optional[str]:
    """
    Sync a FNOL job to HubSpot Ticket pipeline.

    Returns:
        Ticket ID if successful, None otherwise.
    """
    integration = HubSpotIntegration(
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
        pipeline_id=pipeline_id,
        stage_mapping={}
    )
    return integration.create_fnol_ticket(job_data, pipeline_id=pipeline_id, stage_id=stage_id)


# Example usage
if __name__ == "__main__":
    # Configuration (these would come from environment variables)
    HUBSPOT_ACCESS_TOKEN = os.getenv('HUBSPOT_ACCESS_TOKEN')
    PIPELINE_ID = '877072527'  # Underwriting pipeline
    
    # Stage mapping (job status → HubSpot stage ID)
    STAGE_MAPPING = {
        'PROCESSING': '1314763881',      # Submitted
        'CLASSIFYING': '1314763882',     # Under Review
        'EXTRACTING': '1314763882',      # Under Review
        'DETECTING': '1314763882',       # Under Review
        'ANALYZING': '1314763882',       # Under Review
        'SCORING': '1314763882',         # Under Review
        'ACTING': '1314763882',          # Under Review
        'COMPLETE': '1314763883',        # Risk Scored
        'FAILED': '1314763886'           # Declined
    }
    
    # Example: Sync a job
    # job_data = {...}  # From Cosmos DB
    # deal_id = sync_job_to_hubspot(job_data, HUBSPOT_ACCESS_TOKEN, PIPELINE_ID, STAGE_MAPPING)
    # print(f"Synced to deal: {deal_id}")
