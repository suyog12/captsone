from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class CustomerResponse(BaseModel):
    """Full customer record."""

    cust_id: int
    customer_name: Optional[str] = None
    specialty_code: Optional[str] = None
    market_code: Optional[str] = None
    segment: Optional[str] = None
    supplier_profile: Optional[str] = None
    assigned_seller_id: Optional[int] = None
    assigned_at: Optional[datetime] = None
    created_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class CustomerSearchResult(BaseModel):
    """A single hit in a customer search response."""

    cust_id: int
    customer_name: Optional[str] = None
    specialty_code: Optional[str] = None
    market_code: Optional[str] = None
    segment: Optional[str] = None
    assigned_seller_id: Optional[int] = None

    model_config = {"from_attributes": True}
