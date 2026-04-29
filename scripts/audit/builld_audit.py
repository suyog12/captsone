# from your project root
import duckdb
con = duckdb.connect()
# pick whichever raw or clean file has size_tier
result = con.execute("""
    SELECT * FROM read_parquet('data_raw/v_dim_cust_curr_revised/*.parquet')
    LIMIT 1
""").df()
print([c for c in result.columns if 'size' in c.lower() or 'tier' in c.lower()])