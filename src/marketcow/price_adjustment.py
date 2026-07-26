from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .market_data_contracts import DecimalString, number, utc


PriceAdjustment = Literal["raw", "qfq", "hfq"]
FactorApplicability = Literal["applicable", "not_applicable"]


class PriceAdjustmentContract(BaseModel):
    """Unambiguous price/factor semantics for one persisted market bar."""

    model_config = ConfigDict(extra="forbid")

    adjustment: PriceAdjustment
    factor_applicability: FactorApplicability
    corporate_action_factor: Optional[DecimalString] = None
    applied_adjustment_multiplier: DecimalString
    adjustment_reference_date: Optional[str] = None
    reference_factor: Optional[DecimalString] = None
    factor_source: Optional[str] = Field(default=None, min_length=1)
    factor_artifact_id: Optional[str] = Field(default=None, min_length=1)
    factor_as_of: Optional[str] = None

    @field_validator("adjustment_reference_date")
    @classmethod
    def valid_reference_date(cls, value: Optional[str]) -> Optional[str]:
        if value is not None:
            date.fromisoformat(value)
        return value

    @field_validator("factor_as_of")
    @classmethod
    def valid_factor_as_of(cls, value: Optional[str]) -> Optional[str]:
        return None if value is None else utc(value)

    @model_validator(mode="after")
    def consistent(self):
        multiplier = number(self.applied_adjustment_multiplier)
        if multiplier <= 0:
            raise ValueError("applied_adjustment_multiplier must be positive")

        factor_fields = (
            self.corporate_action_factor,
            self.factor_source,
            self.factor_artifact_id,
            self.factor_as_of,
        )
        if self.factor_applicability == "not_applicable":
            if any(value is not None for value in factor_fields):
                raise ValueError(
                    "not_applicable bars must not carry corporate-action factor data"
                )
            if self.adjustment != "raw" or multiplier != Decimal("1"):
                raise ValueError(
                    "not_applicable bars must be raw with multiplier equal to one"
                )
            if self.adjustment_reference_date is not None or self.reference_factor is not None:
                raise ValueError("not_applicable bars must not carry a reference factor")
            return self

        if any(value is None for value in factor_fields):
            raise ValueError(
                "applicable bars require factor, source, artifact, and as-of provenance"
            )
        factor = number(self.corporate_action_factor or "")
        if factor <= 0:
            raise ValueError("corporate_action_factor must be positive")

        if self.adjustment == "raw":
            if multiplier != Decimal("1"):
                raise ValueError("raw bars must have multiplier equal to one")
            if self.adjustment_reference_date is not None or self.reference_factor is not None:
                raise ValueError("raw bars must not carry an adjustment reference")
        elif self.adjustment == "qfq":
            if self.adjustment_reference_date is None or self.reference_factor is None:
                raise ValueError("qfq bars require a reference date and reference factor")
            reference = number(self.reference_factor)
            if reference <= 0:
                raise ValueError("reference_factor must be positive")
            if multiplier != factor / reference:
                raise ValueError(
                    "qfq multiplier must equal factor divided by reference_factor"
                )
        elif self.adjustment == "hfq":
            if self.adjustment_reference_date is not None or self.reference_factor is not None:
                raise ValueError("hfq bars must not carry an adjustment reference")
            if multiplier != factor:
                raise ValueError("hfq multiplier must equal corporate_action_factor")
        return self
