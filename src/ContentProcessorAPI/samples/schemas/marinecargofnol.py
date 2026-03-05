from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field


class Address(BaseModel):
    """
    A class representing an address.

    Attributes:
        street: Street address
        city: City
        state: State
        postal_code: Postal code
        country: Country
    """

    street: Optional[str] = Field(description="Street address")
    city: Optional[str] = Field(description="City")
    state: Optional[str] = Field(description="State or province")
    postal_code: Optional[str] = Field(description="Postal code")
    country: Optional[str] = Field(description="Country")

    @staticmethod
    def example():
        return Address(street="", city="", state="", postal_code="", country="")

    def to_dict(self):
        return {
            "street": self.street,
            "city": self.city,
            "state": self.state,
            "postal_code": self.postal_code,
            "country": self.country,
        }


class InsuredParty(BaseModel):
    """
    Information about the insured party.

    Attributes:
        company_name: Name of the insured company
        contact_person: Contact person name
        telephone: Telephone number
        email: Email address
        address: Mailing address
    """

    company_name: Optional[str] = Field(description="Name of the insured company")
    contact_person: Optional[str] = Field(description="Contact person name")
    telephone: Optional[str] = Field(description="Telephone number")
    email: Optional[str] = Field(description="Email address")
    address: Optional[Address] = Field(description="Mailing address")

    @staticmethod
    def example():
        return InsuredParty(
            company_name="",
            contact_person="",
            telephone="",
            email="",
            address=Address.example(),
        )

    def to_dict(self):
        return {
            "company_name": self.company_name,
            "contact_person": self.contact_person,
            "telephone": self.telephone,
            "email": self.email,
            "address": self.address.to_dict() if self.address else None,
        }


class PolicyInfo(BaseModel):
    """
    Marine cargo policy information.

    Attributes:
        policy_number: Policy number
        certificate_number: Certificate number if applicable
        policy_effective_date: Policy effective date
        policy_expiration_date: Policy expiration date
        coverage_type: Type of coverage (e.g., All Risk, Named Perils)
        deductible: Deductible amount
        deductible_currency: Currency of deductible
    """

    policy_number: Optional[str] = Field(description="Policy number")
    certificate_number: Optional[str] = Field(
        description="Certificate number if applicable"
    )
    policy_effective_date: Optional[str] = Field(
        description="Policy effective date, e.g. 2023-01-01"
    )
    policy_expiration_date: Optional[str] = Field(
        description="Policy expiration date, e.g. 2024-01-01"
    )
    coverage_type: Optional[str] = Field(
        description="Type of coverage (e.g., All Risk, Named Perils, Institute Cargo Clauses)"
    )
    deductible: Optional[float] = Field(description="Deductible amount")
    deductible_currency: Optional[str] = Field(
        description="Currency of deductible, e.g. USD"
    )

    @staticmethod
    def example():
        return PolicyInfo(
            policy_number="",
            certificate_number="",
            policy_effective_date="",
            policy_expiration_date="",
            coverage_type="",
            deductible=0.0,
            deductible_currency="USD",
        )

    def to_dict(self):
        return {
            "policy_number": self.policy_number,
            "certificate_number": self.certificate_number,
            "policy_effective_date": self.policy_effective_date,
            "policy_expiration_date": self.policy_expiration_date,
            "coverage_type": self.coverage_type,
            "deductible": self.deductible,
            "deductible_currency": self.deductible_currency,
        }


class ShipmentDetails(BaseModel):
    """
    Details about the cargo shipment.

    Attributes:
        vessel_name: Name of vessel or flight number
        voyage_number: Voyage or trip number
        bill_of_lading: Bill of lading number
        container_numbers: Container numbers (comma-separated)
        port_of_loading: Port of loading
        port_of_discharge: Port of discharge
        final_destination: Final destination
        date_of_shipment: Date of shipment
        commodity_description: Description of goods
        declared_value: Declared value of cargo
        declared_value_currency: Currency of declared value
        packaging_type: Type of packaging (e.g., container, pallet, crate)
        number_of_packages: Number of packages
        weight: Total weight
        weight_unit: Unit of weight (kg, lbs, tons)
    """

    vessel_name: Optional[str] = Field(
        description="Name of vessel, aircraft, or vehicle"
    )
    voyage_number: Optional[str] = Field(description="Voyage or trip number")
    bill_of_lading: Optional[str] = Field(description="Bill of lading number")
    container_numbers: Optional[str] = Field(
        description="Container numbers (comma-separated)"
    )
    port_of_loading: Optional[str] = Field(description="Port of loading")
    port_of_discharge: Optional[str] = Field(description="Port of discharge")
    final_destination: Optional[str] = Field(description="Final destination")
    date_of_shipment: Optional[str] = Field(
        description="Date of shipment, e.g. 2023-01-01"
    )
    commodity_description: Optional[str] = Field(description="Description of goods")
    declared_value: Optional[float] = Field(description="Declared value of cargo")
    declared_value_currency: Optional[str] = Field(
        description="Currency of declared value, e.g. USD"
    )
    packaging_type: Optional[str] = Field(
        description="Type of packaging (e.g., container, pallet, crate, bulk)"
    )
    number_of_packages: Optional[int] = Field(description="Number of packages")
    weight: Optional[float] = Field(description="Total weight")
    weight_unit: Optional[str] = Field(
        description="Unit of weight (kg, lbs, tons, MT)"
    )

    @staticmethod
    def example():
        return ShipmentDetails(
            vessel_name="",
            voyage_number="",
            bill_of_lading="",
            container_numbers="",
            port_of_loading="",
            port_of_discharge="",
            final_destination="",
            date_of_shipment="",
            commodity_description="",
            declared_value=0.0,
            declared_value_currency="USD",
            packaging_type="",
            number_of_packages=0,
            weight=0.0,
            weight_unit="kg",
        )

    def to_dict(self):
        return {
            "vessel_name": self.vessel_name,
            "voyage_number": self.voyage_number,
            "bill_of_lading": self.bill_of_lading,
            "container_numbers": self.container_numbers,
            "port_of_loading": self.port_of_loading,
            "port_of_discharge": self.port_of_discharge,
            "final_destination": self.final_destination,
            "date_of_shipment": self.date_of_shipment,
            "commodity_description": self.commodity_description,
            "declared_value": self.declared_value,
            "declared_value_currency": self.declared_value_currency,
            "packaging_type": self.packaging_type,
            "number_of_packages": self.number_of_packages,
            "weight": self.weight,
            "weight_unit": self.weight_unit,
        }


class LossDetails(BaseModel):
    """
    Details about the loss or damage.

    Attributes:
        fnol_reference: FNOL reference number
        date_of_loss: Date when loss occurred
        time_of_loss: Time when loss occurred
        location_of_loss: Location where loss occurred
        cause_of_loss: Cause of loss (e.g., water damage, theft, collision)
        nature_of_damage: Nature of damage or loss
        extent_of_damage: Extent of damage (e.g., total loss, partial loss)
        estimated_loss_amount: Estimated loss amount
        estimated_loss_currency: Currency of estimated loss
        salvage_value: Salvage value if applicable
        salvage_value_currency: Currency of salvage value
        survey_required: Whether survey is required
        surveyor_name: Name of surveyor if appointed
        police_report_filed: Whether police report was filed
        police_report_number: Police report number if applicable
    """

    fnol_reference: Optional[str] = Field(description="FNOL reference number")
    date_of_loss: Optional[str] = Field(
        description="Date when loss occurred, e.g. 2023-01-01"
    )
    time_of_loss: Optional[str] = Field(
        description="Time when loss occurred, e.g. 14:30"
    )
    location_of_loss: Optional[str] = Field(description="Location where loss occurred")
    cause_of_loss: Optional[str] = Field(
        description="Cause of loss (e.g., water damage, theft, collision, fire, storm)"
    )
    nature_of_damage: Optional[str] = Field(description="Nature of damage or loss")
    extent_of_damage: Optional[str] = Field(
        description="Extent of damage (e.g., total loss, partial loss, complete destruction)"
    )
    estimated_loss_amount: Optional[float] = Field(description="Estimated loss amount")
    estimated_loss_currency: Optional[str] = Field(
        description="Currency of estimated loss, e.g. USD"
    )
    salvage_value: Optional[float] = Field(description="Salvage value if applicable")
    salvage_value_currency: Optional[str] = Field(
        description="Currency of salvage value, e.g. USD"
    )
    survey_required: Optional[bool] = Field(description="Whether survey is required")
    surveyor_name: Optional[str] = Field(description="Name of surveyor if appointed")
    police_report_filed: Optional[bool] = Field(
        description="Whether police report was filed"
    )
    police_report_number: Optional[str] = Field(
        description="Police report number if applicable"
    )

    @staticmethod
    def example():
        return LossDetails(
            fnol_reference="",
            date_of_loss="",
            time_of_loss="",
            location_of_loss="",
            cause_of_loss="",
            nature_of_damage="",
            extent_of_damage="",
            estimated_loss_amount=0.0,
            estimated_loss_currency="USD",
            salvage_value=0.0,
            salvage_value_currency="USD",
            survey_required=False,
            surveyor_name="",
            police_report_filed=False,
            police_report_number="",
        )

    def to_dict(self):
        return {
            "fnol_reference": self.fnol_reference,
            "date_of_loss": self.date_of_loss,
            "time_of_loss": self.time_of_loss,
            "location_of_loss": self.location_of_loss,
            "cause_of_loss": self.cause_of_loss,
            "nature_of_damage": self.nature_of_damage,
            "extent_of_damage": self.extent_of_damage,
            "estimated_loss_amount": self.estimated_loss_amount,
            "estimated_loss_currency": self.estimated_loss_currency,
            "salvage_value": self.salvage_value,
            "salvage_value_currency": self.salvage_value_currency,
            "survey_required": self.survey_required,
            "surveyor_name": self.surveyor_name,
            "police_report_filed": self.police_report_filed,
            "police_report_number": self.police_report_number,
        }


class Signature(BaseModel):
    """
    Signature information.

    Attributes:
        signatory: Name of the person who signed
        is_signed: Indicates if the document is signed
    """

    signatory: Optional[str] = Field(description="Name of the person who signed")
    is_signed: Optional[bool] = Field(
        description="Indicates if the document is signed. Check for signature in image files."
    )

    @staticmethod
    def example():
        return Signature(signatory="", is_signed=False)

    def to_dict(self):
        return {"signatory": self.signatory, "is_signed": self.is_signed}


class Declaration(BaseModel):
    """
    Declaration and signature section.

    Attributes:
        declaration_text: Declaration text
        claimant_signature: Signature of the claimant
        date_signed: Date when signed
        title_position: Title or position of signatory
    """

    declaration_text: Optional[str] = Field(description="Declaration text")
    claimant_signature: Optional[Signature] = Field(
        description="Signature of the claimant"
    )
    date_signed: Optional[str] = Field(
        description="Date when signed, e.g. 2023-01-01"
    )
    title_position: Optional[str] = Field(description="Title or position of signatory")

    @staticmethod
    def example():
        return Declaration(
            declaration_text="",
            claimant_signature=Signature.example(),
            date_signed="",
            title_position="",
        )

    def to_dict(self):
        return {
            "declaration_text": self.declaration_text,
            "claimant_signature": self.claimant_signature.to_dict()
            if self.claimant_signature
            else None,
            "date_signed": self.date_signed,
            "title_position": self.title_position,
        }


class MarineCargoFNOL(BaseModel):
    """
    A class representing a Marine Cargo First Notice of Loss form.

    Attributes:
        insured_party: Information about the insured party
        policy_info: Marine cargo policy information
        shipment_details: Details about the cargo shipment
        loss_details: Details about the loss or damage
        declaration: Declaration and signature section
    """

    insured_party: Optional[InsuredParty] = Field(
        description="Information about the insured party"
    )
    policy_info: Optional[PolicyInfo] = Field(
        description="Marine cargo policy information"
    )
    shipment_details: Optional[ShipmentDetails] = Field(
        description="Details about the cargo shipment"
    )
    loss_details: Optional[LossDetails] = Field(
        description="Details about the loss or damage"
    )
    declaration: Optional[Declaration] = Field(
        description="Declaration and signature section"
    )

    @staticmethod
    def example():
        return MarineCargoFNOL(
            insured_party=InsuredParty.example(),
            policy_info=PolicyInfo.example(),
            shipment_details=ShipmentDetails.example(),
            loss_details=LossDetails.example(),
            declaration=Declaration.example(),
        )

    def to_dict(self):
        return {
            "insured_party": self.insured_party.to_dict()
            if self.insured_party
            else None,
            "policy_info": self.policy_info.to_dict() if self.policy_info else None,
            "shipment_details": self.shipment_details.to_dict()
            if self.shipment_details
            else None,
            "loss_details": self.loss_details.to_dict() if self.loss_details else None,
            "declaration": self.declaration.to_dict() if self.declaration else None,
        }
