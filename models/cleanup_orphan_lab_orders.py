"""
ONE-TIME cleanup for databases that ran the ETL before the etl/orders.py fix
(load_order() used to `TRUNCATE orders` and rebuild it with fresh auto-increment
ids on every --load, which orphaned labtest_test/labtest_sample/labtest_attribute
rows keyed to the old ids, plus the seed order if its hardcoded encounter no
longer exists). Not needed on a database that has never been loaded with the
old (truncating) version of load_order().

Deletes are batched (BATCH_SIZE), child-first, with FOREIGN_KEY_CHECKS left ON
(DELETE IGNORE skips any row still referenced instead of orphaning something
else). Safe to interrupt and re-run - each batch commits, and once nothing is
orphaned any further run does nothing.

Usage:
    python models/cleanup_orphan_lab_orders.py
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.config import BATCH_SIZE
from config.database import get_target_engine
from models.schema_models import text
from utils.logger import info


def _batched_delete(conn, table, pk, where_orphan_sql):
    total = 0
    last = 0
    while True:
        rows = conn.execute(text(f"""
            SELECT {pk} FROM {table} t
            {where_orphan_sql}
            AND t.{pk} > :last
            ORDER BY t.{pk} LIMIT {BATCH_SIZE}
        """), {"last": last}).fetchall()
        if not rows:
            break
        ids = [r[0] for r in rows]
        last = ids[-1]
        id_list = ",".join(str(i) for i in ids)
        conn.execute(text(f"DELETE IGNORE FROM {table} WHERE {pk} IN ({id_list})"))
        conn.commit()
        total += len(ids)
        info(f"  {table}: deleted {total} so far (last {pk}={last})")
    info(f"{table}: TOTAL deleted {total}")
    return total


def cleanup_orphan_lab_orders():
    start_time = time.time()
    with get_target_engine().connect() as conn:
        info("=== labtest_attribute (children of orphan labtest_test) ===")
        _batched_delete(conn, "labtest_attribute", "test_attribute_id",
            "LEFT JOIN labtest_test lt ON lt.test_order_id = t.test_order_id "
            "LEFT JOIN orders o ON o.order_id = lt.test_order_id "
            "WHERE lt.test_order_id IS NULL OR o.order_id IS NULL")

        info("=== labtest_sample (children of orphan labtest_test) ===")
        _batched_delete(conn, "labtest_sample", "test_sample_id",
            "LEFT JOIN labtest_test lt ON lt.test_order_id = t.test_order_id "
            "LEFT JOIN orders o ON o.order_id = lt.test_order_id "
            "WHERE lt.test_order_id IS NULL OR o.order_id IS NULL")

        info("=== labtest_test (orphans: test_order_id not in orders) ===")
        _batched_delete(conn, "labtest_test", "test_order_id",
            "LEFT JOIN orders o ON o.order_id = t.test_order_id WHERE o.order_id IS NULL")

        info("=== orders (orphans: encounter_id not in encounter) ===")
        _batched_delete(conn, "orders", "order_id",
            "LEFT JOIN encounter e ON e.encounter_id = t.encounter_id WHERE e.encounter_id IS NULL")

    info(f"Cleanup completed (Total Time: {time.time() - start_time:.2f} seconds)")


if __name__ == "__main__":
    cleanup_orphan_lab_orders()
