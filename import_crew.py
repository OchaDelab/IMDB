import csv, sqlite3, sys
from pathlib import Path

TSV = "originalDataset/20251030/title.crew.tsv"
DB  = "imdb.db"

con = sqlite3.connect(DB)
cur = con.cursor()
# Speed pragmas for bulk load
cur.execute("PRAGMA journal_mode=OFF;")
cur.execute("PRAGMA synchronous=OFF;")
cur.execute("PRAGMA temp_store=MEMORY;")
cur.execute("PRAGMA cache_size=-20000000;")

cur.execute("""
CREATE TABLE IF NOT EXISTS crew(
  title_id VARCHAR PRIMARY KEY,
  directors_id VARCHAR,
  writers_id VARCHAR
)
""")

insert_sql = """
INSERT INTO crew
     (title_id, directors_id, writers_id)
      VALUES (?, ?, ?)
"""

expected = 3
bad_path = Path("out/bad_crew_lines.tsv").open("w", encoding="utf-8", newline="")

def normalize_row(row):
    # IMDb TSVs: treat quotes literally; \N = NULL
    # Ensure exactly 3 fields
    if len(row) != expected:
        return None  # let caller log it

    # Map \N -> None
    row = [None if x == r"\N" else x for x in row]

    return tuple(row)

batch, BATCH = [], 100000

with open(TSV, encoding="utf-8", newline="") as f:
    # IMPORTANT: ignore CSV quoting rules for IMDb TSVs
    reader = csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE)

    header = next(reader, None)
    # If you want to verify header width:
    # print("Header cols:", len(header), header)

    for raw in reader:
        norm = normalize_row(raw)
        if norm is None:
            # log the exact offending line to inspect later
            bad_path.write("\t".join(raw) + "\n")
            continue
        batch.append(norm)
        if len(batch) >= BATCH:
            cur.executemany(insert_sql, batch)
            con.commit()
            batch.clear()

# flush remainder
if batch:
    cur.executemany(insert_sql, batch)
    con.commit()

# Add indexes after loading
cur.execute("CREATE INDEX IF NOT EXISTS ix_crew_title_id ON crew(title_id);")
con.commit()

# Restore safer settings (optional)
cur.execute("PRAGMA synchronous=FULL;")
cur.execute("PRAGMA journal_mode=WAL;")

bad_path.close()
con.close()
