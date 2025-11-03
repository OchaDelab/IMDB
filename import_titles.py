import csv, sqlite3, sys
from pathlib import Path

TSV = "originalDataset/20251030/title.basics.tsv"
#TSV = "originalDataset/20251030/title.basics.test.tsv"
DB  = "imdb.db"

con = sqlite3.connect(DB)
cur = con.cursor()
# Speed pragmas for bulk load
cur.execute("PRAGMA journal_mode=OFF;")
cur.execute("PRAGMA synchronous=OFF;")
cur.execute("PRAGMA temp_store=MEMORY;")
cur.execute("PRAGMA cache_size=-20000000;")

cur.execute("""
CREATE TABLE IF NOT EXISTS titles(
  title_id VARCHAR PRIMARY KEY,
  type VARCHAR,
  primary_title VARCHAR,
  original_title VARCHAR,
  is_adult INTEGER,
  start_year INTEGER,
  end_year INTEGER,
  runtime_minutes INTEGER,
  genres VARCHAR
)
""")

insert_sql = """
INSERT INTO titles
(title_id, type, primary_title, original_title, is_adult, start_year, end_year, runtime_minutes, genres)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

expected = 9
bad_path = Path("bad_titles_lines.tsv").open("w", encoding="utf-8", newline="")

def normalize_row(row):
    # IMDb TSVs: treat quotes literally; \N = NULL
    # Ensure exactly 9 fields
    if len(row) != expected:
        return None  # let caller log it

    # Map \N -> None
    row = [None if x == r"\N" else x for x in row]

    # Cast numeric fields safely (is_adult, premiered, ended, runtime_minutes)
    def as_int(x):
        try:
            return int(x) if x is not None else None
        except ValueError:
            return None

    row[4] = as_int(row[4])  # is_adult
    row[5] = as_int(row[5])  # premiered (start_year)
    row[6] = as_int(row[6])  # ended (end_year)
    row[7] = as_int(row[7])  # runtime_minutes

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
cur.execute("CREATE INDEX IF NOT EXISTS ix_title_type ON titles(type);")
cur.execute("CREATE INDEX IF NOT EXISTS ix_titles_primary_title ON titles(primary_title);")
cur.execute("CREATE INDEX IF NOT EXISTS ix_titles_original_title ON titles(original_title);")
con.commit()

# Restore safer settings (optional)
cur.execute("PRAGMA synchronous=FULL;")
cur.execute("PRAGMA journal_mode=WAL;")

bad_path.close()
con.close()
