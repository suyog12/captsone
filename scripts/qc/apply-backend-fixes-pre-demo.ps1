# apply-backend-fixes-pre-demo.ps1
# Three small fixes to the backend:
# 1. customers.py - "status" param shadows fastapi.status -> use 400 literal
# 2. user_service.py - append create_customer_record_only if missing
# 3. products.py - mention only (no fix; sort_by isn't implemented at all,
#    leaving as-is is safer than introducing new behavior right before demo)

$ErrorActionPreference = "Stop"
Set-Location C:\Users\maina\Desktop\Capstone

$ts = Get-Date -Format "yyyyMMdd-HHmmss"
$backupDir = "scripts\qc\backup-pre-demo-$ts"
New-Item -ItemType Directory -Force -Path $backupDir | Out-Null

Write-Host ""
Write-Host "Backup directory: $backupDir" -ForegroundColor Cyan
Write-Host ""

# ----------------------------------------------------------------------------
# Fix 1: customers.py - status param shadows fastapi.status
# ----------------------------------------------------------------------------
$customers = "backend\routers\customers.py"
Copy-Item $customers "$backupDir\customers.py.bak" -Force
Write-Host "[1/2] Patching $customers" -ForegroundColor Yellow

$content = Get-Content $customers -Raw

$old1 = @'
    # Validate scope value
    if scope not in ("mine", "all"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="scope must be 'mine' or 'all'.",
        )
'@

$new1 = @'
    # Validate scope value.
    # NOTE: the local "status" query parameter shadows the imported
    # fastapi.status module here, so use the integer literal 400.
    if scope not in ("mine", "all"):
        raise HTTPException(
            status_code=400,
            detail="scope must be 'mine' or 'all'.",
        )
'@

if ($content.Contains($old1)) {
    $content = $content.Replace($old1, $new1)
    Set-Content -Path $customers -Value $content -NoNewline
    Write-Host "      OK - scope validation now returns 400 instead of crashing" -ForegroundColor Green
} else {
    Write-Host "      SKIP - already patched or text not found" -ForegroundColor DarkGray
}

# ----------------------------------------------------------------------------
# Fix 2: user_service.py - check create_customer_record_only is present
# ----------------------------------------------------------------------------
$userSvc = "backend\services\user_service.py"
Write-Host ""
Write-Host "[2/2] Checking $userSvc for create_customer_record_only" -ForegroundColor Yellow

$svcContent = Get-Content $userSvc -Raw
if ($svcContent -match "async def create_customer_record_only") {
    Write-Host "      OK - function already present" -ForegroundColor Green
} else {
    Write-Host "      MISSING - need to append the helper" -ForegroundColor Red
    Copy-Item $userSvc "$backupDir\user_service.py.bak" -Force

    $appendBody = @'


# ============================================================================
# Create customer record only - no login (used by POST /customers/record)
# ============================================================================

async def create_customer_record_only(
    db: AsyncSession,
    *,
    customer_business_name: str,
    market_code: str,
    size_tier: str,
    specialty_code: Optional[str] = None,
    assigned_seller_id: Optional[int] = None,
    actor_user_id: int,
) -> Customer:
    """Create only a recdash.customers row, no User login.

    Used when a seller adds a customer via the dashboard (auto-assigned to
    that seller) or when an admin creates a record without a login.
    """
    next_cust_id = await _next_cust_id(db)

    segment = None
    if market_code and size_tier:
        segment = f"{market_code}_{size_tier}"

    customer = Customer(
        cust_id=next_cust_id,
        customer_name=customer_business_name,
        specialty_code=specialty_code,
        market_code=market_code,
        segment=segment,
        status="cold_start",
        archetype="other",
        assigned_seller_id=assigned_seller_id,
        assigned_at=datetime.utcnow() if assigned_seller_id else None,
        created_at=datetime.utcnow(),
    )
    db.add(customer)
    await db.flush()

    if assigned_seller_id is not None:
        await assignment_service._record_history(
            db,
            cust_id=next_cust_id,
            previous_seller_id=None,
            new_seller_id=assigned_seller_id,
            change_reason="customer_created",
            changed_by_user_id=actor_user_id,
            notes=None,
        )

    await db.commit()
    await db.refresh(customer)
    return customer
'@

    Add-Content -Path $userSvc -Value $appendBody
    Write-Host "      APPENDED - reload uvicorn to pick it up" -ForegroundColor Green
}

Write-Host ""
Write-Host "All fixes applied. Backups in: $backupDir" -ForegroundColor Cyan
Write-Host ""
Write-Host "Now restart your backend (Ctrl+C in the uvicorn window, then re-run):" -ForegroundColor Yellow
Write-Host "  uvicorn backend.main:app --reload --host 127.0.0.1 --port 8000" -ForegroundColor White
Write-Host ""
Write-Host "Then re-run tests:" -ForegroundColor Yellow
Write-Host "  python scripts\qc\run_api_tests.py --output reports\post_fix_run.xlsx" -ForegroundColor White
Write-Host ""
