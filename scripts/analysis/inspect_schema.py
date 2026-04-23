from __future__ import annotations
from pathlib import Path
import duckdb

ROOT        = Path(__file__).resolve().parent.parent.parent
MERGED_FILE = ROOT / "data_clean" / "serving" / "merged_dataset.parquet"
PROD_FILE   = ROOT / "data_clean" / "product"  / "products_clean.parquet"
CUST_FILE   = ROOT / "data_clean" / "customer" / "customers_clean.parquet"

def inspect(path: Path, label: str) -> None:
    if not path.exists():
        print(f"\n{label}: NOT FOUND at {path}")
        return

    con = duckdb.connect()
    desc = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{path}') LIMIT 0"
    ).fetchdf()

    sample = con.execute(
        f"SELECT * FROM read_parquet('{path}') LIMIT 3"
    ).fetchdf()
    con.close()

    print(f"\n{'='*70}")
    print(f"  {label}")
    print(f"  {path.name}  |  {len(desc)} columns")
    print(f"{'='*70}")
    print(f"  {'#':<4} {'Column':<40} {'Type':<20} {'Sample value'}")
    print(f"  {''*4} {''*40} {''*20} {''*30}")
    for i, row in desc.iterrows():
        col  = row["column_name"]
        typ  = row["column_type"]
        samp = sample[col].dropna().iloc[0] if col in sample and len(sample[col].dropna()) > 0 else "NULL"
        samp_str = str(samp)[:45]
        print(f"  {i+1:<4} {col:<40} {typ:<20} {samp_str}")

def main():
    print("\nSchema inspection — McKesson Capstone data files")
    inspect(MERGED_FILE, "merged_dataset  (transaction-level serving file)")
    inspect(PROD_FILE,   "products_clean  (product dimension)")
    inspect(CUST_FILE,   "customers_clean (customer dimension)")

if __name__ == "__main__":
    main()