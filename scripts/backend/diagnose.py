from __future__ import annotations
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import text
from scripts.backend.db_connection import get_engine, get_schema

schema = get_schema()
engine = get_engine()

with engine.connect() as conn:
    # Which database are we actually connected to?
    db = conn.execute(text("SELECT current_database()")).scalar()
    user = conn.execute(text("SELECT current_user")).scalar()
    sp = conn.execute(text("SHOW search_path")).scalar()
    
    # How many products does the seeder see?
    n_products = conn.execute(text(
        f"SELECT COUNT(*) FROM {schema}.products"
    )).scalar()
    
    # Does item 21969 exist from the seeder's perspective?
    exists_21969 = conn.execute(text(
        f"SELECT COUNT(*) FROM {schema}.products WHERE item_id = 21969"
    )).scalar()
    
    # What's the schema actually called in the FK constraint?
    fk_info = conn.execute(text("""
        SELECT 
            tc.table_schema, 
            tc.table_name, 
            ccu.table_schema AS foreign_table_schema,
            ccu.table_name AS foreign_table_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.constraint_column_usage ccu
            ON tc.constraint_name = ccu.constraint_name
        WHERE tc.constraint_name = 'inventory_item_id_fkey'
    """)).fetchone()

print(f"Connected database  : {db}")
print(f"Connected user      : {user}")
print(f"search_path         : {sp}")
print(f"Schema (from .env)  : {schema}")
print(f"Products visible    : {n_products}")
print(f"Item 21969 visible  : {exists_21969}")
print(f"FK constraint info  : {fk_info}")