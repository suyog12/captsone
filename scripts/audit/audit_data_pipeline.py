"""
Audit script v2 for the Capstone Recommendation Dashboard project.

Handles PARTITIONED parquet datasets where one logical table is stored
as many .snappy.parquet shards inside a folder, e.g.:

    data_raw/v_fct_sales_2425_revised/
        sales_data_2425_revised_0_0_0.snappy.parquet
        sales_data_2425_revised_0_0_1.snappy.parquet
        ...
    data_raw/v_dim_cust_curr_revised/
        v_dim_cust_curr_revised_0_0_0.snappy.parquet
        ...

For partitioned datasets, the script reads ALL shards together via
DuckDB glob patterns (read_parquet('folder/*.parquet')) and reports
schema + stats for the unified logical table — not per-shard.

Walks three data layers and produces a single Markdown report:
  1. RAW client data        - what McKesson gave us
  2. CLEAN intermediate     - data after cleaning, before signal generation
  3. PRECOMPUTED serving    - final parquets read by the FastAPI backend

Detects "logical tables" automatically:
  - Single .parquet file in folder = 1 logical table
  - Folder containing multiple .parquet files = 1 logical table (partitioned)
  - Folder of folders = each subfolder is a logical table

Usage:
    cd C:\\Users\\maina\\Desktop\\Capstone
    conda activate CTBA
    python audit_data_pipeline_v2.py

    # Or with explicit paths:
    python audit_data_pipeline_v2.py --raw data_raw --clean data_clean

    # Or scan everything:
    python audit_data_pipeline_v2.py --crawl C:\\Users\\maina\\Desktop\\Capstone

Output:
    audit_report.md           - full markdown report (per-logical-table)
    audit_columns.csv         - flat CSV of (layer, table, column, dtype, null_count)
"""

# ---------- imports ----------
import argparse
import os
import sys
import time
from collections import defaultdict
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("ERROR: duckdb is not installed. Run: pip install duckdb")
    sys.exit(1)

try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas is not installed. Run: pip install pandas")
    sys.exit(1)


# ---------- config ----------
LIKELY_RAW_PATHS = ["data_raw", "data/raw", "raw_data", "data_source"]
LIKELY_CLEAN_PATHS = ["data_clean", "data/clean", "data_processed"]
LIKELY_PRECOMPUTED_PATHS = [
    "data_clean/serving/precomputed",
    "data_clean/serving",
    "data_clean/precomputed",
]

# how many sample rows to include per logical table
SAMPLE_ROWS = 3

# truncate string column samples to this many chars
STR_TRUNC = 80


# ---------- discover logical tables ----------
def discover_logical_tables(root):
    """
    Walk root and group .parquet files by their containing folder.
    Each folder containing one or more .parquet files = one logical table.

    Returns list of dicts: { name, folder_path, shard_count, glob_pattern }

    Skips folders that contain BOTH .parquet files AND subfolders with .parquet
    (those get split into multiple logical tables).
    """
    if not root or not os.path.exists(root):
        return []

    tables = []
    # walk: at each directory, check if it directly contains .parquet files
    for dirpath, _, filenames in os.walk(root):
        parquet_files = [f for f in filenames if f.lower().endswith(".parquet")]
        if not parquet_files:
            continue

        folder_name = Path(dirpath).name
        # use relative path for nicer table name when nested
        try:
            rel = Path(dirpath).relative_to(root)
            display_name = str(rel) if str(rel) != "." else folder_name
        except ValueError:
            display_name = folder_name

        # if only one shard, treat the file as the table; otherwise the folder is the table
        if len(parquet_files) == 1:
            single_path = Path(dirpath) / parquet_files[0]
            tables.append({
                "name": parquet_files[0].replace(".parquet", "").replace(".snappy", ""),
                "folder_path": dirpath,
                "shard_count": 1,
                "shards": [str(single_path)],
                "glob_pattern": str(single_path).replace("\\", "/"),
                "is_partitioned": False,
                "total_size_mb": round(os.path.getsize(single_path) / (1024 * 1024), 2),
            })
        else:
            shards = sorted(str(Path(dirpath) / f) for f in parquet_files)
            total_size = sum(os.path.getsize(s) for s in shards)
            tables.append({
                "name": display_name,
                "folder_path": dirpath,
                "shard_count": len(parquet_files),
                "shards": shards,
                "glob_pattern": (str(Path(dirpath)) + "/*.parquet").replace("\\", "/"),
                "is_partitioned": True,
                "total_size_mb": round(total_size / (1024 * 1024), 2),
            })

    return sorted(tables, key=lambda t: t["name"])


def autodetect_paths(base_dir):
    found = {"raw": None, "clean": None, "precomputed": None}
    for p in LIKELY_RAW_PATHS:
        candidate = Path(base_dir) / p
        if candidate.exists():
            found["raw"] = str(candidate)
            break
    for p in LIKELY_CLEAN_PATHS:
        candidate = Path(base_dir) / p
        if candidate.exists():
            found["clean"] = str(candidate)
            break
    for p in LIKELY_PRECOMPUTED_PATHS:
        candidate = Path(base_dir) / p
        if candidate.exists():
            found["precomputed"] = str(candidate)
            break
    return found


# ---------- profile a logical table ----------
def profile_logical_table(con, table):
    """
    Profile one logical table (which may be partitioned across many shards).
    Returns dict with row count, columns, sample, stats.
    """
    info = {
        "name": table["name"],
        "folder_path": table["folder_path"],
        "shard_count": table["shard_count"],
        "is_partitioned": table["is_partitioned"],
        "total_size_mb": table["total_size_mb"],
        "n_rows": None,
        "n_cols": None,
        "columns": [],
        "sample": None,
        "error": None,
    }

    # use the glob pattern so DuckDB reads all shards as one logical dataset
    src = f"read_parquet('{table['glob_pattern']}')"

    try:
        # row count across all shards
        n_rows = con.execute(f"SELECT COUNT(*) FROM {src}").fetchone()[0]
        info["n_rows"] = n_rows

        # schema (all shards must have same schema; DuckDB will fail otherwise)
        schema_df = con.execute(
            f"DESCRIBE SELECT * FROM {src} LIMIT 0"
        ).df()
        info["n_cols"] = len(schema_df)

        col_meta = []
        for _, row in schema_df.iterrows():
            col_meta.append({
                "name": row["column_name"],
                "type": row["column_type"],
                "null_count": None,
                "stats": {},
            })

        if n_rows > 0:
            for c in col_meta:
                cname = c["name"]
                ctype = c["type"].upper()
                # null count
                try:
                    nc = con.execute(
                        f'SELECT COUNT(*) FROM {src} WHERE "{cname}" IS NULL'
                    ).fetchone()[0]
                    c["null_count"] = nc
                except Exception:
                    c["null_count"] = "?"

                # numeric stats
                if any(t in ctype for t in [
                    "INT", "DECIMAL", "DOUBLE", "FLOAT", "NUMERIC", "BIGINT", "SMALLINT", "TINYINT"
                ]):
                    try:
                        stats_row = con.execute(f"""
                            SELECT
                                MIN("{cname}") AS min_v,
                                MAX("{cname}") AS max_v,
                                AVG("{cname}") AS mean_v,
                                MEDIAN("{cname}") AS median_v
                            FROM {src}
                            WHERE "{cname}" IS NOT NULL
                        """).fetchone()
                        if stats_row:
                            c["stats"] = {
                                "min": stats_row[0],
                                "max": stats_row[1],
                                "mean": (
                                    round(stats_row[2], 4)
                                    if stats_row[2] is not None else None
                                ),
                                "median": stats_row[3],
                            }
                    except Exception as e:
                        c["stats"] = {"error": str(e)[:80]}

                # string-like stats
                elif any(t in ctype for t in ["VARCHAR", "TEXT", "STRING"]):
                    try:
                        distinct = con.execute(
                            f'SELECT COUNT(DISTINCT "{cname}") FROM {src}'
                        ).fetchone()[0]
                        # only fetch top 5 for columns with reasonable cardinality
                        if distinct < 100000:
                            top5 = con.execute(f"""
                                SELECT "{cname}" AS v, COUNT(*) AS n
                                FROM {src}
                                WHERE "{cname}" IS NOT NULL
                                GROUP BY 1 ORDER BY n DESC LIMIT 5
                            """).df()
                            c["stats"] = {
                                "distinct": distinct,
                                "top5": [
                                    (str(r["v"])[:STR_TRUNC], int(r["n"]))
                                    for _, r in top5.iterrows()
                                ],
                            }
                        else:
                            c["stats"] = {
                                "distinct": distinct,
                                "top5": [("(skipped — high cardinality)", 0)],
                            }
                    except Exception as e:
                        c["stats"] = {"error": str(e)[:80]}

                # date / timestamp stats
                elif any(t in ctype for t in ["DATE", "TIMESTAMP", "TIME"]):
                    try:
                        stats_row = con.execute(f"""
                            SELECT MIN("{cname}") AS min_v, MAX("{cname}") AS max_v
                            FROM {src}
                            WHERE "{cname}" IS NOT NULL
                        """).fetchone()
                        if stats_row:
                            c["stats"] = {
                                "min": str(stats_row[0]),
                                "max": str(stats_row[1]),
                            }
                    except Exception as e:
                        c["stats"] = {"error": str(e)[:80]}

        info["columns"] = col_meta

        # sample rows
        try:
            sample_df = con.execute(
                f"SELECT * FROM {src} LIMIT {SAMPLE_ROWS}"
            ).df()
            info["sample"] = sample_df
        except Exception as e:
            info["sample"] = None
            info["error"] = f"sample read failed: {e}"

    except Exception as e:
        info["error"] = str(e)

    return info


# ---------- formatting ----------
def format_table_section(info, layer):
    lines = []
    partitioned_note = ""
    if info["is_partitioned"]:
        partitioned_note = f" _(partitioned dataset, {info['shard_count']} shards)_"

    lines.append(f"### `{info['name']}`{partitioned_note}")
    lines.append("")
    lines.append(f"- **Folder:** `{info['folder_path']}`")
    lines.append(f"- **Total size:** {info['total_size_mb']} MB")

    if info.get("error"):
        lines.append(f"- **ERROR:** {info['error']}")
        lines.append("")
        return "\n".join(lines)

    lines.append(f"- **Rows:** {info['n_rows']:,}")
    lines.append(f"- **Columns:** {info['n_cols']}")
    lines.append("")
    lines.append("**Columns:**")
    lines.append("")
    lines.append("| # | Column | Type | Nulls | Stats |")
    lines.append("|---|---|---|---|---|")
    for i, c in enumerate(info["columns"], 1):
        nulls = c["null_count"]
        if isinstance(nulls, int) and info["n_rows"]:
            null_pct = round(100 * nulls / info["n_rows"], 1)
            null_str = f"{nulls:,} ({null_pct}%)"
        else:
            null_str = str(nulls)

        s = c.get("stats", {})
        if "error" in s:
            stat_str = f"err: {s['error']}"
        elif "min" in s and "max" in s and "mean" in s:
            stat_str = f"min={s['min']}, max={s['max']}, mean={s['mean']}, median={s['median']}"
        elif "min" in s and "max" in s:
            stat_str = f"min={s['min']}, max={s['max']}"
        elif "distinct" in s:
            top_str = ", ".join(
                f"`{v}` ({n:,})" if n > 0 else f"{v}"
                for v, n in s.get("top5", [])
            )
            stat_str = f"distinct={s['distinct']:,}; top: {top_str}"
        else:
            stat_str = ""

        lines.append(f"| {i} | `{c['name']}` | {c['type']} | {null_str} | {stat_str} |")
    lines.append("")

    if info["sample"] is not None and len(info["sample"]) > 0:
        lines.append("**Sample (first 3 rows):**")
        lines.append("")
        try:
            md_table = info["sample"].to_markdown(index=False)
            lines.append(md_table)
        except Exception:
            lines.append("```")
            lines.append(info["sample"].to_string(index=False))
            lines.append("```")
        lines.append("")

    return "\n".join(lines)


# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(
        description="Audit raw + clean + precomputed parquet pipeline (handles partitioned datasets)."
    )
    parser.add_argument("--raw", help="Path to raw client parquet folder (e.g. data_raw)")
    parser.add_argument("--clean", help="Path to cleaned/intermediate parquet folder (e.g. data_clean)")
    parser.add_argument("--precomputed", help="Path to precomputed serving parquet folder")
    parser.add_argument("--crawl", help="If set, scan this single folder recursively and bucket by path keyword")
    parser.add_argument("--out", default="audit_report.md", help="Output markdown file")
    parser.add_argument("--csv", default="audit_columns.csv", help="Output CSV file")
    args = parser.parse_args()

    base_dir = os.getcwd()
    if not (args.raw or args.clean or args.precomputed or args.crawl):
        print(f"No paths given. Autodetecting under {base_dir}...")
        detected = autodetect_paths(base_dir)
        args.raw = args.raw or detected["raw"]
        args.clean = args.clean or detected["clean"]
        args.precomputed = args.precomputed or detected["precomputed"]
        for layer, path in detected.items():
            print(f"  {layer}: {path or '(not found)'}")
        print()

    # discover logical tables in each layer
    layer_tables = {}

    if args.crawl:
        # find all logical tables under crawl root, then bucket by path keyword
        all_tables = discover_logical_tables(args.crawl)
        print(f"Crawling {args.crawl} found {len(all_tables)} logical tables")
        raw_t, clean_t, pre_t, other_t = [], [], [], []
        for t in all_tables:
            ps = t["folder_path"].lower().replace("\\", "/")
            if "precomputed" in ps or "serving" in ps:
                pre_t.append(t)
            elif "raw" in ps or "source" in ps:
                raw_t.append(t)
            elif "clean" in ps or "processed" in ps:
                clean_t.append(t)
            else:
                other_t.append(t)
        layer_tables = {
            "RAW (client-provided)": raw_t,
            "CLEAN (intermediate)": clean_t,
            "PRECOMPUTED (serving)": pre_t,
            "UNCATEGORIZED": other_t,
        }
    else:
        layer_tables = {
            "RAW (client-provided)": discover_logical_tables(args.raw) if args.raw else [],
            "CLEAN (intermediate)": discover_logical_tables(args.clean) if args.clean else [],
            "PRECOMPUTED (serving)": discover_logical_tables(args.precomputed) if args.precomputed else [],
        }

        # important: if precomputed is a subfolder of clean, dedupe so we don't audit twice
        if args.clean and args.precomputed:
            clean_root = os.path.abspath(args.clean)
            pre_root = os.path.abspath(args.precomputed)
            if pre_root.startswith(clean_root):
                # remove tables from CLEAN that are inside PRECOMPUTED's path
                pre_paths = {os.path.abspath(t["folder_path"]) for t in layer_tables["PRECOMPUTED (serving)"]}
                layer_tables["CLEAN (intermediate)"] = [
                    t for t in layer_tables["CLEAN (intermediate)"]
                    if os.path.abspath(t["folder_path"]) not in pre_paths
                    and not any(os.path.abspath(t["folder_path"]).startswith(p) for p in pre_paths)
                ]

    total_tables = sum(len(v) for v in layer_tables.values())
    if total_tables == 0:
        print("\nNo parquet tables found. Check paths and try again.")
        print("Use --crawl <folder> to scan an entire project tree.")
        sys.exit(1)

    print(f"Total logical tables to audit: {total_tables}")
    for layer, tables in layer_tables.items():
        print(f"  {layer}: {len(tables)} logical tables")
        for t in tables:
            shard_note = f"  [{t['shard_count']} shards]" if t["is_partitioned"] else ""
            print(f"    - {t['name']}{shard_note}")
    print()

    # one duckdb connection
    con = duckdb.connect(database=":memory:")

    all_layers_data = {}
    csv_rows = []
    started = time.time()

    for layer, tables in layer_tables.items():
        all_layers_data[layer] = []
        for t in tables:
            shard_note = f" ({t['shard_count']} shards)" if t["is_partitioned"] else ""
            print(f"  profiling [{layer}]: {t['name']}{shard_note} ...", end=" ", flush=True)
            t0 = time.time()
            info = profile_logical_table(con, t)
            print(f"done ({round(time.time() - t0, 2)}s)")
            all_layers_data[layer].append(info)
            for c in info["columns"]:
                csv_rows.append({
                    "layer": layer,
                    "table": info["name"],
                    "folder": info["folder_path"],
                    "column": c["name"],
                    "dtype": c["type"],
                    "null_count": c["null_count"],
                })

    elapsed = round(time.time() - started, 2)
    print(f"\nProfiling complete in {elapsed}s. Building report...\n")

    # ---------- detect derived columns ----------
    raw_cols = set()
    for info in all_layers_data.get("RAW (client-provided)", []):
        for c in info["columns"]:
            raw_cols.add(c["name"].lower())

    clean_only = set()
    for info in all_layers_data.get("CLEAN (intermediate)", []):
        for c in info["columns"]:
            if c["name"].lower() not in raw_cols:
                clean_only.add(c["name"])

    pre_only = set()
    for info in all_layers_data.get("PRECOMPUTED (serving)", []):
        for c in info["columns"]:
            if c["name"].lower() not in raw_cols:
                pre_only.add(c["name"])

    # also: which raw columns survive into clean / precomputed?
    clean_cols = set()
    for info in all_layers_data.get("CLEAN (intermediate)", []):
        for c in info["columns"]:
            clean_cols.add(c["name"].lower())
    pre_cols = set()
    for info in all_layers_data.get("PRECOMPUTED (serving)", []):
        for c in info["columns"]:
            pre_cols.add(c["name"].lower())

    raw_used_in_clean = raw_cols & clean_cols
    raw_used_in_pre = raw_cols & pre_cols
    raw_dropped = raw_cols - clean_cols - pre_cols

    # ---------- write markdown ----------
    out = []
    out.append("# Capstone Data Pipeline Audit Report (v2)")
    out.append("")
    out.append(f"_Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}_")
    out.append("")
    out.append("## Summary")
    out.append("")
    out.append("| Layer | Logical tables | Total rows | Total size MB |")
    out.append("|---|---|---|---|")
    for layer, data in all_layers_data.items():
        total_rows = sum((d["n_rows"] or 0) for d in data)
        total_mb = sum(d["total_size_mb"] for d in data)
        out.append(f"| {layer} | {len(data)} | {total_rows:,} | {round(total_mb, 1)} |")
    out.append("")

    out.append("## Logical tables by layer")
    out.append("")
    for layer, data in all_layers_data.items():
        if not data:
            continue
        out.append(f"### {layer}")
        out.append("")
        out.append("| Table | Shards | Rows | Cols | Size MB |")
        out.append("|---|---|---|---|---|")
        for d in data:
            out.append(f"| `{d['name']}` | {d['shard_count']} | {(d['n_rows'] or 0):,} | {d['n_cols']} | {d['total_size_mb']} |")
        out.append("")

    # ---------- derivation analysis ----------
    out.append("## Derivation Analysis")
    out.append("")
    out.append("Compares column names across layers. Auto-detection by name match — renamed columns may show up as 'derived' even if they're really cleaned versions of raw columns. Always cross-check with feature engineering scripts before defense.")
    out.append("")
    out.append(f"### RAW columns (extracted from client data): {len(raw_cols)} unique")
    out.append("")
    out.append(", ".join(f"`{c}`" for c in sorted(raw_cols)) if raw_cols else "_(no raw layer)_")
    out.append("")
    out.append(f"### Raw columns USED in clean layer: {len(raw_used_in_clean)}")
    out.append("")
    out.append(", ".join(f"`{c}`" for c in sorted(raw_used_in_clean)) if raw_used_in_clean else "_(none)_")
    out.append("")
    out.append(f"### Raw columns USED in precomputed layer: {len(raw_used_in_pre)}")
    out.append("")
    out.append(", ".join(f"`{c}`" for c in sorted(raw_used_in_pre)) if raw_used_in_pre else "_(none)_")
    out.append("")
    out.append(f"### Raw columns DROPPED (not in clean or precomputed): {len(raw_dropped)}")
    out.append("")
    out.append(", ".join(f"`{c}`" for c in sorted(raw_dropped)) if raw_dropped else "_(none — all raw columns survive)_")
    out.append("")
    out.append(f"### NEW columns in CLEAN (not in raw): {len(clean_only)} candidates")
    out.append("")
    out.append(", ".join(f"`{c}`" for c in sorted(clean_only)) if clean_only else "_(none)_")
    out.append("")
    out.append(f"### NEW columns in PRECOMPUTED (not in raw): {len(pre_only)} candidates")
    out.append("")
    out.append(", ".join(f"`{c}`" for c in sorted(pre_only)) if pre_only else "_(none)_")
    out.append("")

    # ---------- per-table detail ----------
    for layer, data in all_layers_data.items():
        if not data:
            continue
        out.append("---")
        out.append("")
        out.append(f"## Layer detail: {layer}")
        out.append("")
        for info in data:
            out.append(format_table_section(info, layer))

    with open(args.out, "w", encoding="utf-8") as f:
        f.write("\n".join(out))
    print(f"Report written: {args.out}")

    if csv_rows:
        df = pd.DataFrame(csv_rows)
        df.to_csv(args.csv, index=False)
        print(f"Column CSV written: {args.csv}")

    print(f"\nLogical tables audited: {total_tables}")
    print("Done.")


if __name__ == "__main__":
    main()