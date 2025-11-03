import csv, sqlite3, sys
from pathlib import Path

TSV = "originalDataset/20251030/title.episode.tsv"
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
CREATE TABLE IF NOT EXISTS episode (
  episode_title_id VARCHAR,
  show_title_id VARCHAR,
  season_number INTEGER,
  episode_number INTEGER
)
""")

insert_sql = """INSERT INTO episode
  (episode_title_id, show_title_id, season_number, episode_number)
  VALUES (?, ?, ?, ?)
"""

expected = 4
bad_path = Path("bad_episode_lines.tsv").open("w", encoding="utf-8", newline="")

def normalize_row(row):
    # IMDb TSVs: treat quotes literally; \N = NULL
    # Ensure exactly 4 fields
    if len(row) != expected:
        return None  # let caller log it

    # Map \N -> None
    row = [None if x == r"\N" else x for x in row]

    # Cast numeric fields safely (season_number, episode_number)
    def as_int(x):
        try:
            return int(x) if x is not None else None
        except ValueError:
            return None

    row[2] = as_int(row[2])  # season_number
    row[3] = as_int(row[3])  # episode_number

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
cur.execute("CREATE INDEX IF NOT EXISTS ix_episode_title_id ON episode(episode_title_id);")
cur.execute("CREATE INDEX IF NOT EXISTS ix_episode_show_title_id ON episode(show_title_id);")
con.commit()

# Restore safer settings (optional)
cur.execute("PRAGMA synchronous=FULL;")
cur.execute("PRAGMA journal_mode=WAL;")

bad_path.close()
con.close()
