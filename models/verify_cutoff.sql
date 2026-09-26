-- ============================================================================
-- Verification for the data cutoff (keep year > 2020, i.e. on/after 2021-01-01).
-- Run in DBeaver against the TARGET db AFTER `python main.py --apply-cutoff` (or after CALL sp_apply_data_cutoff(..., 0, ...)).
-- "Expect 0" queries must return 0 / no rows. Others are informational.
-- ============================================================================
SET @c = '2021-01-01';

-- 1) Encounters that should have been deleted: all four dates before the cutoff AND no obs created/voided since.   Expect: 0
SELECT COUNT(*) AS encounters_before_cutoff FROM encounter e
WHERE (e.date_created IS NULL OR e.date_created < @c) AND (e.date_changed IS NULL OR e.date_changed < @c)
  AND (e.date_voided IS NULL OR e.date_voided < @c) AND (e.encounter_datetime IS NULL OR e.encounter_datetime < @c)
  AND NOT EXISTS (SELECT 1 FROM obs o WHERE o.encounter_id = e.encounter_id AND (o.date_created >= @c OR o.date_voided >= @c));

-- 2) Obs whose encounter is gone (encounter-less obs are allowed).                                              Expect: 0
SELECT COUNT(*) AS obs_with_missing_encounter FROM obs o
LEFT JOIN encounter e ON e.encounter_id = o.encounter_id
WHERE o.encounter_id IS NOT NULL AND e.encounter_id IS NULL;

-- 3) Orphans in tables that hang off encounter / orders.                                      Expect: all 0
SELECT 'orders'             AS tbl, COUNT(*) AS orphans FROM orders o             LEFT JOIN encounter e ON e.encounter_id = o.encounter_id WHERE e.encounter_id IS NULL
UNION ALL SELECT 'encounter_provider', COUNT(*) FROM encounter_provider ep LEFT JOIN encounter e ON e.encounter_id = ep.encounter_id WHERE e.encounter_id IS NULL
UNION ALL SELECT 'drug_order',         COUNT(*) FROM drug_order d          LEFT JOIN orders o ON o.order_id = d.order_id WHERE o.order_id IS NULL
UNION ALL SELECT 'labtest_test',       COUNT(*) FROM labtest_test t        LEFT JOIN orders o ON o.order_id = t.test_order_id WHERE o.order_id IS NULL
UNION ALL SELECT 'labtest_sample',     COUNT(*) FROM labtest_sample s      LEFT JOIN labtest_test t ON t.test_order_id = s.test_order_id WHERE t.test_order_id IS NULL
UNION ALL SELECT 'labtest_attribute',  COUNT(*) FROM labtest_attribute a   LEFT JOIN labtest_test t ON t.test_order_id = a.test_order_id WHERE t.test_order_id IS NULL
UNION ALL SELECT 'obs.order_id',       COUNT(*) FROM obs o                 LEFT JOIN orders r ON r.order_id = o.order_id WHERE o.order_id IS NOT NULL AND r.order_id IS NULL
UNION ALL SELECT 'obs.obs_group_id',   COUNT(*) FROM obs o                 LEFT JOIN obs g ON g.obs_id = o.obs_group_id WHERE o.obs_group_id IS NOT NULL AND g.obs_id IS NULL
UNION ALL SELECT 'obs.previous_version', COUNT(*) FROM obs o               LEFT JOIN obs g ON g.obs_id = o.previous_version WHERE o.previous_version IS NOT NULL AND g.obs_id IS NULL
UNION ALL SELECT 'patient_state.encounter_id', COUNT(*) FROM patient_state ps LEFT JOIN encounter e ON e.encounter_id = ps.encounter_id WHERE ps.encounter_id IS NOT NULL AND e.encounter_id IS NULL;

-- 4) Patients that should have been voided but are still active (same rule as the procedure).  Expect: 0
SELECT COUNT(*) AS active_patients_without_activity FROM patient pt
WHERE pt.voided = 0 AND pt.patient_id <> 1
  AND (pt.date_created IS NULL OR pt.date_created < @c) AND (pt.date_changed IS NULL OR pt.date_changed < @c) AND (pt.date_voided IS NULL OR pt.date_voided < @c)
  AND NOT EXISTS (SELECT 1 FROM users u WHERE u.person_id = pt.patient_id)
  AND NOT EXISTS (SELECT 1 FROM provider p WHERE p.person_id = pt.patient_id)
  AND NOT EXISTS (SELECT 1 FROM encounter e WHERE e.patient_id = pt.patient_id)
  AND NOT EXISTS (SELECT 1 FROM obs o WHERE o.person_id = pt.patient_id AND o.encounter_id IS NULL);

-- 5) Voided-by-cutoff patients must not have any encounter left.                              Expect: 0
SELECT COUNT(*) AS voided_patients_with_encounters FROM patient pt
WHERE pt.void_reason LIKE 'Data cutoff:%' AND EXISTS (SELECT 1 FROM encounter e WHERE e.patient_id = pt.patient_id);

-- 6) Children of voided patients still active.                                                Expect: all 0
SELECT 'person' AS tbl, COUNT(*) AS still_active FROM person x JOIN patient pt ON pt.patient_id = x.person_id WHERE pt.void_reason LIKE 'Data cutoff:%' AND x.voided = 0
UNION ALL SELECT 'person_name',       COUNT(*) FROM person_name x       JOIN patient pt ON pt.patient_id = x.person_id  WHERE pt.void_reason LIKE 'Data cutoff:%' AND x.voided = 0
UNION ALL SELECT 'person_address',    COUNT(*) FROM person_address x    JOIN patient pt ON pt.patient_id = x.person_id  WHERE pt.void_reason LIKE 'Data cutoff:%' AND x.voided = 0
UNION ALL SELECT 'person_attribute',  COUNT(*) FROM person_attribute x  JOIN patient pt ON pt.patient_id = x.person_id  WHERE pt.void_reason LIKE 'Data cutoff:%' AND x.voided = 0
UNION ALL SELECT 'patient_identifier',COUNT(*) FROM patient_identifier x JOIN patient pt ON pt.patient_id = x.patient_id WHERE pt.void_reason LIKE 'Data cutoff:%' AND x.voided = 0
UNION ALL SELECT 'patient_program',   COUNT(*) FROM patient_program x   JOIN patient pt ON pt.patient_id = x.patient_id WHERE pt.void_reason LIKE 'Data cutoff:%' AND x.voided = 0
UNION ALL SELECT 'patient_state',     COUNT(*) FROM patient_state x JOIN patient_program pp ON pp.patient_program_id = x.patient_program_id JOIN patient pt ON pt.patient_id = pp.patient_id WHERE pt.void_reason LIKE 'Data cutoff:%' AND x.voided = 0;

-- 7) Protected persons were not voided (user 1's person, every user, every provider).         Expect: 0
SELECT COUNT(*) AS protected_persons_voided FROM person p
WHERE p.voided = 1 AND p.void_reason LIKE 'Data cutoff:%'
  AND (p.person_id = 1 OR p.person_id IN (SELECT person_id FROM users) OR p.person_id IN (SELECT person_id FROM provider));

-- 8) Informational: what the procedure logged, and what remains.
SELECT step, table_name, rows_affected FROM _cutoff_log WHERE run_id = (SELECT run_id FROM _cutoff_log ORDER BY id DESC LIMIT 1) ORDER BY id;
SELECT (SELECT COUNT(*) FROM encounter) AS encounters_left, (SELECT COUNT(*) FROM obs) AS obs_left,
       (SELECT COUNT(*) FROM orders) AS orders_left, (SELECT COUNT(*) FROM patient WHERE voided = 0) AS active_patients,
       (SELECT COUNT(*) FROM patient WHERE void_reason LIKE 'Data cutoff:%') AS voided_by_cutoff;
