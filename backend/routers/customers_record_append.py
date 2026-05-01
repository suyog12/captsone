"""
Append this endpoint to backend/routers/customers.py.

Imports needed at the top of customers.py (add to existing imports):

    from backend.schemas.customer import (
        ...,
        CustomerRecordCreateRequest,
    )
    from backend.services import user_service

Place the endpoint anywhere in the router file - convention: near the
other write endpoints, but it can be appended to the bottom and FastAPI
will register it normally.
"""

# /customers/record


@router.post(
    "/record",
    response_model=CustomerResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a customer record (no login). Sellers auto-assign to themselves.",
)
async def create_customer_record(
    body: CustomerRecordCreateRequest,
    user: User = Depends(require_seller_or_admin),
    db: AsyncSession = Depends(get_db),
) -> CustomerResponse:
    """
    Creates a Customer row in recdash.customers without a User login.
    Useful for sellers adding new accounts they're already in contact
    with - they don't need to invent a password and the customer can be
    issued a login later by an admin.

    Auto-assignment rules:
      - Seller calling this endpoint: assigned_seller_id is forced to
        user.user_id. Any non-null value in the body is rejected with 403.
      - Admin: respects body.assigned_seller_id (may be None for an
        unassigned customer).
    """
    if user.role == "seller":
        if (
            body.assigned_seller_id is not None
            and body.assigned_seller_id != user.user_id
        ):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    "Sellers cannot assign customers to another seller. "
                    "Leave assigned_seller_id blank - it will be set to "
                    "your user_id automatically."
                ),
            )
        assigned_seller_id = user.user_id
    else:
        # admin
        assigned_seller_id = body.assigned_seller_id

    try:
        customer = await user_service.create_customer_record_only(
            db,
            customer_business_name=body.customer_business_name,
            market_code=body.market_code,
            size_tier=body.size_tier,
            specialty_code=body.specialty_code,
            assigned_seller_id=assigned_seller_id,
            actor_user_id=user.user_id,
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail=str(e)
        )

    resp = CustomerResponse.model_validate(customer)
    resp.is_assigned_to_me = (
        assigned_seller_id is not None and assigned_seller_id == user.user_id
    )
    return resp
