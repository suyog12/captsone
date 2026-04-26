from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field


# -------- Single assignment / reassignment --------

class AssignmentChangeRequest(BaseModel):
    """Body for PATCH /customers/{cust_id}/assignment.

    seller_id is optional:
      - integer: assign or reassign to that seller
      - null:    unassign (removes the current assignment)
    notes is an optional admin comment recorded on the history row.
    """

    seller_id: Optional[int] = Field(
        None,
        description="The user_id of the target seller, or null to unassign.",
    )
    notes: Optional[str] = Field(
        None,
        max_length=500,
        description="Optional comment recorded in the assignment history.",
    )


class AssignmentChangeResponse(BaseModel):
    """Response from PATCH /customers/{cust_id}/assignment."""

    cust_id: int
    previous_seller_id: Optional[int] = None
    new_seller_id: Optional[int] = None
    change_reason: str
    changed_by_user_id: int
    changed_at: datetime
    history_id: int


# -------- Seller claim --------

class ClaimRequest(BaseModel):
    """Body for POST /customers/{cust_id}/claim. No fields required.

    Sellers cannot pass a seller_id. The endpoint always assigns to the
    currently logged-in seller.
    """

    notes: Optional[str] = Field(
        None,
        max_length=500,
        description="Optional comment recorded in the assignment history.",
    )


# -------- Bulk assign --------

class BulkAssignRequest(BaseModel):
    """Body for POST /customers/assignments/bulk."""

    seller_id: int = Field(
        ...,
        description="The seller to assign all listed customers to.",
    )
    cust_ids: list[int] = Field(
        ...,
        min_length=1,
        max_length=500,
        description="List of customer IDs to assign.",
    )
    notes: Optional[str] = Field(
        None,
        max_length=500,
        description="Optional comment recorded for each assignment.",
    )


class BulkAssignResponse(BaseModel):
    """Response from POST /customers/assignments/bulk."""

    seller_id: int
    requested_count: int
    assigned_count: int
    skipped_count: int
    skipped_reasons: dict[int, str] = Field(
        default_factory=dict,
        description="Mapping cust_id -> reason for any customers that could not be assigned.",
    )


# -------- Assignment history --------

class AssignmentHistoryEntry(BaseModel):
    """One row from customer_assignment_history."""

    history_id: int
    cust_id: int
    previous_seller_id: Optional[int] = None
    previous_seller_username: Optional[str] = None
    new_seller_id: Optional[int] = None
    new_seller_username: Optional[str] = None
    changed_by_user_id: int
    changed_by_username: Optional[str] = None
    change_reason: str
    notes: Optional[str] = None
    changed_at: datetime


class AssignmentHistoryResponse(BaseModel):
    """Response from GET /customers/{cust_id}/assignment-history."""

    cust_id: int
    total_changes: int
    items: list[AssignmentHistoryEntry]


# -------- Seller's customers --------

class SellerCustomerListResponse(BaseModel):
    """Response from GET /sellers/{user_id}/customers."""

    seller_id: int
    seller_username: Optional[str] = None
    total: int
    items: list[dict]  # reuses CustomerSearchResult shape via dicts


# -------- Deactivation summary (extends UserResponse) --------

class SellerDeactivationResponse(BaseModel):
    """Response from DELETE /users/{user_id} when target is a seller.

    Adds context about how many customers were auto-unassigned by the
    deactivation.
    """

    user_id: int
    username: str
    role: str
    is_active: bool
    customers_unassigned: int = Field(
        0,
        description="Count of customers whose assignment was cleared by this deactivation.",
    )
    message: str
