import duckdb

con = duckdb.connect()

print("=" * 70)
print("product_specialty.parquet schema")
print("=" * 70)
df = con.execute("""
    DESCRIBE SELECT * FROM read_parquet('data_clean/serving/precomputed/product_specialty.parquet')
""").fetchdf()
print(df.to_string())

print()
print("=" * 70)
print("product_specialty first 3 rows")
print("=" * 70)
df = con.execute("""
    SELECT * FROM read_parquet('data_clean/serving/precomputed/product_specialty.parquet') LIMIT 3
""").fetchdf()
print(df.to_string())

print()
print("=" * 70)
print("product_segments.parquet schema")
print("=" * 70)
df = con.execute("""
    DESCRIBE SELECT * FROM read_parquet('data_clean/serving/precomputed/product_segments.parquet')
""").fetchdf()
print(df.to_string())

print()
print("=" * 70)
print("product_segments first 2 rows")
print("=" * 70)
df = con.execute("""
    SELECT * FROM read_parquet('data_clean/serving/precomputed/product_segments.parquet') LIMIT 2
""").fetchdf()
print(df.to_string())