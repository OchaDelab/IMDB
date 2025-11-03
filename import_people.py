import csv, sqlite3, sys
from pathlib import Path

TSV = "originalDataset/20251030/name.basics.tsv"
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
CREATE TABLE IF NOT EXISTS people (
  person_id VARCHAR PRIMARY KEY,
  primary_name VARCHAR,
  birth_year INTEGER,
  death_year INTEGER,
  primary_profession VARCHAR,
  known_for_titles VARCHAR
)
""")

insert_sql = """INSERT INTO people
      (person_id, primary_name, birth_year, death_year, primary_profession, known_for_titles)
      VALUES (?, ?, ?, ?, ?, ?)
"""

expected = 6
bad_path = Path("out/bad_people_lines.tsv").open("w", encoding="utf-8", newline="")

def normalize_row(row):
    # IMDb TSVs: treat quotes literally; \N = NULL
    # Ensure exactly 6 fields
    if len(row) != expected:
        return None  # let caller log it

    # Map \N -> None
    row = [None if x == r"\N" else x for x in row]

    # Cast numeric fields safely (born, died)
    def as_int(x):
        try:
            return int(x) if x is not None else None
        except ValueError:
            return None

    row[2] = as_int(row[2])  # born (birth_year)
    row[3] = as_int(row[3])  # died (death_year)

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
cur.execute("CREATE INDEX IF NOT EXISTS ix_people_name ON people(primary_name);")
con.commit()

# Restore safer settings (optional)
cur.execute("PRAGMA synchronous=FULL;")
cur.execute("PRAGMA journal_mode=WAL;")

bad_path.close()
con.close()
