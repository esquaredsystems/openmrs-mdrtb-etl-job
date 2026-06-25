from sqlalchemy import text
from tests.conftest import within_tolerance

_MONTH_YEAR_QUERY = """
    SELECT YEAR(date_created) AS yr, MONTH(date_created) AS mo, MIN(patient_id) AS patient_id
    FROM patient
    GROUP BY YEAR(date_created), MONTH(date_created)
    ORDER BY yr, mo
"""

def test_person_count(source_conn, target_conn):
    src = source_conn.execute(text("SELECT COUNT(*) FROM person")).scalar()
    tgt = target_conn.execute(text("SELECT COUNT(*) FROM person")).scalar()
    assert within_tolerance(src, tgt), f"person count mismatch: source={src}, target={tgt}"


def test_patient_count(source_conn, target_conn):
    src = source_conn.execute(text("SELECT COUNT(*) FROM patient")).scalar()
    tgt = target_conn.execute(text("SELECT COUNT(*) FROM patient")).scalar()
    assert within_tolerance(src, tgt), f"patient count mismatch: source={src}, target={tgt}"


def test_encounters_by_month_year(source_conn, target_conn):
    month_year_patients = source_conn.execute(text(_MONTH_YEAR_QUERY)).fetchall()

    failures = []
    for row in month_year_patients:
        yr, mo, pid = row.yr, row.mo, row.patient_id
        src_count = source_conn.execute(
            text("SELECT COUNT(*) FROM encounter WHERE patient_id = :pid"), {"pid": pid}
        ).scalar()
        tgt_count = target_conn.execute(
            text("SELECT COUNT(*) FROM encounter WHERE patient_id = :pid"), {"pid": pid}
        ).scalar()
        if not within_tolerance(src_count, tgt_count):
            failures.append(f"{yr}-{mo:02d} (patient_id={pid}): source={src_count}, target={tgt_count}")

    assert not failures, "Encounter count mismatches:\n" + "\n".join(failures)


def test_obs_by_encounter_by_month_year(source_conn, target_conn):
    month_year_patients = source_conn.execute(text(_MONTH_YEAR_QUERY)).fetchall()

    failures = []
    for row in month_year_patients:
        yr, mo, pid = row.yr, row.mo, row.patient_id
        encounters = source_conn.execute(
            text("SELECT encounter_id FROM encounter WHERE patient_id = :pid"), {"pid": pid}
        ).fetchall()

        for enc_row in encounters:
            eid = enc_row.encounter_id
            src_obs = source_conn.execute(
                text("SELECT COUNT(*) FROM obs WHERE encounter_id = :eid"), {"eid": eid}
            ).scalar()
            tgt_obs = target_conn.execute(
                text("SELECT COUNT(*) FROM obs WHERE encounter_id = :eid"), {"eid": eid}
            ).scalar()
            if not within_tolerance(src_obs, tgt_obs):
                failures.append(
                    f"{yr}-{mo:02d} patient_id={pid} encounter_id={eid}: source obs={src_obs}, target obs={tgt_obs}"
                )

    assert not failures, "Obs count mismatches:\n" + "\n".join(failures)
