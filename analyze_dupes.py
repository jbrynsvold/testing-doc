"""
backfill_spike_library_columns.py
----------------------------------
Backfills missing columns on spike_library:
  - sale_count_30d_at_spike
  - days_since_last_sale_at_spike
  - avg_price_3d_at_spike      (already stored as peak_spike_price, skip)
  - avg_price_90d_at_spike     (already stored as pre_spike_price, skip)
  - pct_of_52w_range_at_spike  (SKIPPED - insufficient price history for early spikes)

Runs in batches of 500 spike IDs to avoid timeouts.

ENV VARS:
  DB_CONNECTION -- Supabase session pooler connection string
"""

import psycopg2
import time
import os
import subprocess
import sys

subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary"])

DB_CONNECTION = os.environ["DB_CONNECTION"]
BATCH_SIZE = 500


def get_connection():
    conn = psycopg2.connect(DB_CONNECTION)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SET statement_timeout = 0;")
    return conn, cur


def get_spike_id_range(cur):
    cur.execute("SELECT MIN(id), MAX(id) FROM spike_library WHERE sale_count_30d_at_spike IS NULL;")
    return cur.fetchone()


def backfill_batch(cur, id_from, id_to):
    """
    Backfills sale_count_30d_at_spike and days_since_last_sale_at_spike
    for a batch of spike IDs.

    Uses spike_start_date as the anchor point — looks at the 30 days
    before the spike started to reconstruct market conditions at that time.
    """
    sql = """
    UPDATE spike_library sl
    SET
        sale_count_30d_at_spike = sub.sale_count_30d,
        days_since_last_sale_at_spike = sub.days_since_last_sale
    FROM (
        SELECT
            sl2.id,
            -- 30d sale count ending at spike_start_date
            COALESCE((
                SELECT SUM(cph.sale_count)
                FROM card_price_history cph
                WHERE cph.card_id = sl2.card_id
                  AND cph.grade = sl2.grade
                  AND cph.sale_date >= sl2.spike_start_date - INTERVAL '30 days'
                  AND cph.sale_date < sl2.spike_start_date
            ), 0) AS sale_count_30d,
            -- Days since last sale before spike started
            COALESCE((
                SELECT (sl2.spike_start_date - MAX(cph.sale_date))
                FROM card_price_history cph
                WHERE cph.card_id = sl2.card_id
                  AND cph.grade = sl2.grade
                  AND cph.sale_date < sl2.spike_start_date
            ), 999) AS days_since_last_sale
        FROM spike_library sl2
        WHERE sl2.id BETWEEN %s AND %s
          AND sl2.sale_count_30d_at_spike IS NULL
    ) sub
    WHERE sl.id = sub.id;
    """
    cur.execute(sql, (id_from, id_to))
    return cur.rowcount


def main():
    start_total = time.time()
    print("Starting spike_library column backfill")
    print("=" * 60)

    conn, cur = get_connection()

    min_id, max_id = get_spike_id_range(cur)
    if min_id is None:
        print("Nothing to backfill — all rows already populated.")
        return

    print(f"Spike ID range to fill: {min_id} to {max_id}")
    print(f"Batch size: {BATCH_SIZE}")

    total_updated = 0
    batch_num = 1
    current = min_id
    total_batches = ((max_id - min_id) // BATCH_SIZE) + 1

    while current <= max_id:
        id_to = min(current + BATCH_SIZE - 1, max_id)
        start = time.time()

        try:
            updated = backfill_batch(cur, current, id_to)
            elapsed = round(time.time() - start, 1)
            total_updated += updated
            print(f"  [OK] Batch {batch_num}/{total_batches}: "
                  f"IDs {current}-{id_to} | {updated} rows updated ({elapsed}s)")
        except Exception as e:
            print(f"  [FAIL] Batch {batch_num}/{total_batches}: "
                  f"IDs {current}-{id_to} | {e}")

        current += BATCH_SIZE
        batch_num += 1

    cur.close()
    conn.close()

    elapsed_total = round(time.time() - start_total, 1)
    print(f"\nDone. {total_updated} rows updated in {elapsed_total}s")


if __name__ == "__main__":
    main()
