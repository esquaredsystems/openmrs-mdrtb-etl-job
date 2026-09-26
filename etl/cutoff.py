import time
from pathlib import Path

from config.config import BATCH_SIZE, CUTOFF_DATE
from config.database import get_target_engine
from models.schema_models import text
from utils.logger import info

PROCEDURES_SQL = Path(__file__).resolve().parent.parent / "models" / "sp_apply_data_cutoff.sql"


def install_cutoff_procedures():
    lines = [l for l in PROCEDURES_SQL.read_text(encoding="utf-8").splitlines()
             if not l.strip().upper().startswith("DELIMITER") and not l.strip().startswith("--")]
    statements = [s.strip() for s in "\n".join(lines).split("$$") if s.strip()]
    with get_target_engine().connect() as conn:
        for statement in statements:
            conn.execute(text(statement.replace(":", "\\:")))
        conn.commit()
    info(f"Installed {len(statements)} cutoff stored procedures")


def _procedures_installed():
    with get_target_engine().connect() as conn:
        return conn.execute(text("""
            SELECT COUNT(*) FROM information_schema.routines
            WHERE routine_schema = DATABASE() AND routine_name IN ('sp_apply_data_cutoff', 'sp_cutoff_reconcile_patients')
        """)).scalar() == 2


def _already_applied():
    with get_target_engine().connect() as conn:
        if not conn.execute(text("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '_cutoff_log'")).scalar():
            return False
        return conn.execute(text("SELECT COUNT(*) FROM _cutoff_log WHERE step = 'apply_complete'")).scalar() > 0


def _log_last_run(conn):
    rows = conn.execute(text("""
        SELECT step, table_name, rows_affected FROM _cutoff_log
        WHERE run_id = (SELECT run_id FROM _cutoff_log ORDER BY id DESC LIMIT 1) ORDER BY id
    """)).fetchall()
    for row in rows:
        info(f"[cutoff] {row.step:<40} {row.table_name:<36} {row.rows_affected}")


def run_data_cutoff(dry_run=True, force=False):
    """One-time purge (or its dry run) of encounters/obs/orders/lab rows before CUTOFF_DATE, then patient reconcile."""
    start_time = time.time()
    if not dry_run and not force and _already_applied():
        info("Data cutoff was already applied to this database (_cutoff_log has 'apply_complete'). "
             "Refusing to run again; pass --force-cutoff to override.")
        return
    install_cutoff_procedures()
    mode = "DRY RUN" if dry_run else "APPLY"
    info(f"Running sp_apply_data_cutoff ({mode}) with cutoff {CUTOFF_DATE}...")
    with get_target_engine().connect() as conn:
        conn.execute(text("CALL sp_apply_data_cutoff(:cutoff, :dry_run, :batch)"),
                     {"cutoff": CUTOFF_DATE, "dry_run": 1 if dry_run else 0, "batch": BATCH_SIZE})
        conn.commit()
        _log_last_run(conn)
    info(f"Data cutoff ({mode}) completed (Total Time: {time.time() - start_time:.2f} seconds)")


def run_cutoff_reconcile():
    """Idempotent step run after every load: void patients without activity, un-void ones that became active."""
    start_time = time.time()
    if not _procedures_installed():
        install_cutoff_procedures()
    info(f"Reconciling patients against cutoff {CUTOFF_DATE}...")
    with get_target_engine().connect() as conn:
        conn.execute(text("CALL sp_cutoff_reconcile_patients(UUID(), :cutoff, 0, :batch, 0)"),
                     {"cutoff": CUTOFF_DATE, "batch": BATCH_SIZE})
        conn.commit()
        _log_last_run(conn)
    info(f"Cutoff reconcile completed (Total Time: {time.time() - start_time:.2f} seconds)")
