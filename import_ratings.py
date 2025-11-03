import csv, sqlite3, sys
from pathlib import Path

TSV = "originalDataset/20251030/title.ratings.tsv"
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
CREATE TABLE IF NOT EXISTS ratings (
 title_id VARCHAR PRIMARY KEY,
 rating FLOAT,
 votes INTEGER
)
""")

insert_sql = """INSERT INTO ratings
      (title_id, rating, votes)
      VALUES (?, ?, ?)
"""

expected = 3
bad_path = Path("out/bad_ratings_lines.tsv").open("w", encoding="utf-8", newline="")

def normalize_row(row):
    # IMDb TSVs: treat quotes literally; \N = NULL
    # Ensure exactly 3 fields
    if len(row) != expected:
        return None  # let caller log it

    # Map \N -> None
    row = [None if x == r"\N" else x for x in row]

    # Cast numeric fields safely (rating, votes)
    def as_int(x):
        try:
            return int(x) if x is not None else None
        except ValueError:
            return None
    def as_float(x):
        try:
            return float(x) if x is not None else None
        except ValueError:
            return None

    row[1] = as_float(row[1])  # rating
    row[2] = as_int(row[2])  # votes

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


# Restore safer settings (optional)
cur.execute("PRAGMA synchronous=FULL;")
cur.execute("PRAGMA journal_mode=WAL;")

bad_path.close()
con.close()
