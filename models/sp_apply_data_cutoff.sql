-- ============================================================================
-- Data cutoff procedures (target OpenMRS DB).  p_cutoff_date '2021-01-01' == "keep year > 2020".
--
-- KEEP RULES
--   * An encounter is KEPT if ANY of date_created, date_changed, date_voided, encounter_datetime >= cutoff,
--     OR any of its obs was created/voided on/after the cutoff (obs are never edited in place: an edit inserts a
--     NEW obs row pointing at the old one via previous_version, so a recent edit keeps the whole encounter).
--   * orders, drug_order, labtest_*, encounter_provider and the remaining obs follow their encounter.
--   * An obs WITHOUT an encounter is judged by its own dates, and is kept if a kept obs points at it
--     (obs_group_id / previous_version) or it points at a kept obs, so groups and edit chains are never split.
--   * A patient is VOIDED (never deleted) if it has no encounter left, no encounter-less obs, and its own
--     date_created/date_changed/date_voided are before the cutoff. Never touched: patient/person 1, users, providers.
--   * sp_cutoff_reconcile_patients is idempotent and is meant to run after EVERY ETL load: it voids inactive
--     patients (and children) and un-voids patients that became active again - but only rows it voided itself
--     (void_reason 'Data cutoff:%'), never rows voided in the source system.
--
-- DELETE SAFETY: FK checks stay ON. Deletes use DELETE IGNORE in repeated passes: a row still referenced by another
-- row is skipped (not orphaned) and retried; anything still blocked after the passes is reported as BLOCKED and the
-- run is NOT marked apply_complete.
--
-- Usage (DBeaver):
--   CALL sp_apply_data_cutoff('2021-01-01', 1, 10000);   -- dry run: fills _target_* tables + _cutoff_log only
--   CALL sp_apply_data_cutoff('2021-01-01', 0, 10000);   -- real run (ONE TIME): deletes / voids. IRREVERSIBLE - back up
--   SELECT * FROM _cutoff_log ORDER BY id;
--
-- Statements are separated by $$ so the file works in DBeaver ("Execute script") and in etl/cutoff.py.
-- ============================================================================

DELIMITER $$

DROP PROCEDURE IF EXISTS sp_cutoff_log$$
CREATE PROCEDURE sp_cutoff_log(IN p_run_id CHAR(36), IN p_step VARCHAR(64), IN p_table VARCHAR(64), IN p_rows BIGINT)
BEGIN
    INSERT INTO _cutoff_log (run_id, step, table_name, rows_affected) VALUES (p_run_id, p_step, p_table, p_rows);
END$$

DROP PROCEDURE IF EXISTS sp_cutoff_ensure_tables$$
CREATE PROCEDURE sp_cutoff_ensure_tables()
BEGIN
    CREATE TABLE IF NOT EXISTS _cutoff_log (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        run_id CHAR(36) NOT NULL, step VARCHAR(64) NOT NULL, table_name VARCHAR(64) NOT NULL,
        rows_affected BIGINT NOT NULL, logged_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    ) ENGINE = InnoDB;
    CREATE TABLE IF NOT EXISTS _target_encounter_id (
        seq BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, encounter_id INT NOT NULL, UNIQUE KEY uq_encounter_id (encounter_id)
    ) ENGINE = InnoDB;
    -- labtest_test / labtest_sample / labtest_attribute / drug_order are keyed by order_id, so this covers them too
    CREATE TABLE IF NOT EXISTS _target_order_id (
        seq BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, order_id INT NOT NULL, UNIQUE KEY uq_order_id (order_id)
    ) ENGINE = InnoDB;
    CREATE TABLE IF NOT EXISTS _target_obs_id (
        seq BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, obs_id INT NOT NULL, UNIQUE KEY uq_obs_id (obs_id)
    ) ENGINE = InnoDB;
    CREATE TABLE IF NOT EXISTS _target_patient_id (
        seq BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, patient_id INT NOT NULL, UNIQUE KEY uq_patient_id (patient_id)
    ) ENGINE = InnoDB;
    CREATE TABLE IF NOT EXISTS _target_unvoid_patient_id (
        seq BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, patient_id INT NOT NULL, UNIQUE KEY uq_patient_id (patient_id)
    ) ENGINE = InnoDB;
END$$

-- Deletes rows of p_table whose p_join_col matches a staged id. FK checks stay on: DELETE IGNORE skips rows that are
-- still referenced, and further passes retry them (e.g. obs group parents after their children are gone).
DROP PROCEDURE IF EXISTS sp_cutoff_delete_batched$$
CREATE PROCEDURE sp_cutoff_delete_batched(
    IN p_run_id CHAR(36), IN p_table VARCHAR(64), IN p_join_col VARCHAR(64),
    IN p_stage VARCHAR(64), IN p_stage_key VARCHAR(64), IN p_batch INT)
BEGIN
    DECLARE v_from BIGINT DEFAULT 1;
    DECLARE v_max BIGINT DEFAULT 0;
    DECLARE v_total BIGINT DEFAULT 0;
    DECLARE v_pass INT DEFAULT 0;
    DECLARE v_pass_rows BIGINT DEFAULT 0;
    DECLARE v_left BIGINT DEFAULT 0;

    SET @cutoff_sql = CONCAT('SELECT COALESCE(MAX(seq), 0) INTO @cutoff_max FROM `', p_stage, '`');
    PREPARE st FROM @cutoff_sql; EXECUTE st; DEALLOCATE PREPARE st;
    SET v_max = @cutoff_max;

    REPEAT
        SET v_pass = v_pass + 1;
        SET v_from = 1;
        SET v_pass_rows = 0;
        WHILE v_from <= v_max DO
            SET @cutoff_sql = CONCAT('DELETE IGNORE d FROM `', p_stage, '` s JOIN `', p_table, '` d ON d.`', p_join_col,
                                     '` = s.`', p_stage_key, '` WHERE s.seq BETWEEN ', v_from, ' AND ', v_from + p_batch - 1);
            PREPARE st FROM @cutoff_sql; EXECUTE st;
            SET v_pass_rows = v_pass_rows + ROW_COUNT();
            DEALLOCATE PREPARE st;
            COMMIT;
            SET v_from = v_from + p_batch;
        END WHILE;
        SET v_total = v_total + v_pass_rows;

        SET @cutoff_sql = CONCAT('SELECT COUNT(*) INTO @cutoff_left FROM `', p_stage, '` s JOIN `', p_table,
                                 '` d ON d.`', p_join_col, '` = s.`', p_stage_key, '`');
        PREPARE st FROM @cutoff_sql; EXECUTE st; DEALLOCATE PREPARE st;
        SET v_left = @cutoff_left;
    UNTIL v_left = 0 OR v_pass_rows = 0 OR v_pass >= 10 END REPEAT;

    CALL sp_cutoff_log(p_run_id, 'deleted', p_table, v_total);
    IF v_left > 0 THEN
        CALL sp_cutoff_log(p_run_id, 'BLOCKED (still referenced)', p_table, v_left);
        SET @cutoff_blocked = @cutoff_blocked + v_left;
    END IF;
END$$

-- Voids (p_unvoid = 0) or un-voids (p_unvoid = 1, only rows whose void_reason starts with 'Data cutoff:') the rows of
-- p_table that belong to the patients in p_stage. patient_state has no patient_id, so it goes through patient_program.
DROP PROCEDURE IF EXISTS sp_cutoff_set_void_batched$$
CREATE PROCEDURE sp_cutoff_set_void_batched(
    IN p_run_id CHAR(36), IN p_table VARCHAR(64), IN p_join_col VARCHAR(64), IN p_stage VARCHAR(64),
    IN p_batch INT, IN p_reason VARCHAR(255), IN p_unvoid TINYINT)
BEGIN
    DECLARE v_from BIGINT DEFAULT 1;
    DECLARE v_max BIGINT DEFAULT 0;
    DECLARE v_total BIGINT DEFAULT 0;
    DECLARE v_set VARCHAR(400);
    DECLARE v_where VARCHAR(100);
    DECLARE v_join VARCHAR(400);

    IF p_unvoid = 1 THEN
        SET v_set = 'd.voided = 0, d.voided_by = NULL, d.date_voided = NULL, d.void_reason = NULL';
        SET v_where = 'd.void_reason LIKE ''Data cutoff:%''';
    ELSE
        SET v_set = CONCAT('d.voided = 1, d.voided_by = 1, d.date_voided = NOW(), d.void_reason = ', QUOTE(p_reason));
        SET v_where = 'd.voided = 0';
    END IF;
    IF p_table = 'patient_state' THEN
        SET v_join = CONCAT('JOIN patient_program pp ON pp.patient_program_id = d.patient_program_id JOIN `', p_stage, '` s ON s.patient_id = pp.patient_id');
    ELSE
        SET v_join = CONCAT('JOIN `', p_stage, '` s ON s.patient_id = d.`', p_join_col, '`');
    END IF;

    SET @cutoff_sql = CONCAT('SELECT COALESCE(MAX(seq), 0) INTO @cutoff_max FROM `', p_stage, '`');
    PREPARE st FROM @cutoff_sql; EXECUTE st; DEALLOCATE PREPARE st;
    SET v_max = @cutoff_max;

    WHILE v_from <= v_max DO
        SET @cutoff_sql = CONCAT('UPDATE `', p_table, '` d ', v_join, ' SET ', v_set, ' WHERE ', v_where,
                                 ' AND s.seq BETWEEN ', v_from, ' AND ', v_from + p_batch - 1);
        PREPARE st FROM @cutoff_sql; EXECUTE st;
        SET v_total = v_total + ROW_COUNT();
        DEALLOCATE PREPARE st;
        COMMIT;
        SET v_from = v_from + p_batch;
    END WHILE;

    CALL sp_cutoff_log(p_run_id, IF(p_unvoid = 1, 'unvoided', 'voided'), p_table, v_total);
END$$

-- Idempotent patient reconcile. Safe to run after every ETL load (a second run changes nothing).
-- p_use_staged = 1 : simulate the purge (dry run) - encounters/obs listed in _target_encounter_id/_target_obs_id count as gone.
-- p_use_staged = 0 : look at the tables as they are now (after the purge, and in every later ETL run).
DROP PROCEDURE IF EXISTS sp_cutoff_reconcile_patients$$
CREATE PROCEDURE sp_cutoff_reconcile_patients(
    IN p_run_id CHAR(36), IN p_cutoff_date DATE, IN p_dry_run TINYINT, IN p_batch INT, IN p_use_staged TINYINT)
BEGIN
    DECLARE v_rows BIGINT DEFAULT 0;
    DECLARE v_reason VARCHAR(255);

    IF p_batch IS NULL OR p_batch <= 0 THEN
        SET p_batch = 10000;
    END IF;
    SET v_reason = CONCAT('Data cutoff: no activity on/after ', p_cutoff_date);

    CALL sp_cutoff_ensure_tables();
    TRUNCATE TABLE _target_patient_id;
    TRUNCATE TABLE _target_unvoid_patient_id;

    INSERT INTO _target_patient_id (patient_id)
    SELECT pt.patient_id FROM patient pt
    WHERE pt.voided = 0
      AND pt.patient_id <> 1
      AND (pt.date_created IS NULL OR pt.date_created < p_cutoff_date)
      AND (pt.date_changed IS NULL OR pt.date_changed < p_cutoff_date)
      AND (pt.date_voided IS NULL OR pt.date_voided < p_cutoff_date)
      AND NOT EXISTS (SELECT 1 FROM users u WHERE u.person_id = pt.patient_id)
      AND NOT EXISTS (SELECT 1 FROM provider pv WHERE pv.person_id = pt.patient_id)
      AND NOT EXISTS (SELECT 1 FROM encounter e
                      LEFT JOIN _target_encounter_id te ON te.encounter_id = e.encounter_id AND p_use_staged = 1
                      WHERE e.patient_id = pt.patient_id AND te.encounter_id IS NULL)
      AND NOT EXISTS (SELECT 1 FROM obs o
                      LEFT JOIN _target_obs_id tobs ON tobs.obs_id = o.obs_id AND p_use_staged = 1
                      WHERE o.person_id = pt.patient_id AND o.encounter_id IS NULL AND tobs.obs_id IS NULL)
    ORDER BY pt.patient_id;
    SELECT COUNT(*) INTO v_rows FROM _target_patient_id;
    CALL sp_cutoff_log(p_run_id, 'identified', 'patient (to void)', v_rows);

    -- patients we voided earlier that are active again (new encounter/obs arrived, or their own record changed).
    -- date_voided is deliberately NOT considered: our own void stamps it with NOW().
    IF p_use_staged = 0 THEN
        INSERT INTO _target_unvoid_patient_id (patient_id)
        SELECT pt.patient_id FROM patient pt
        WHERE pt.void_reason LIKE 'Data cutoff:%'
          AND (pt.patient_id = 1
               OR pt.date_created >= p_cutoff_date OR pt.date_changed >= p_cutoff_date
               OR EXISTS (SELECT 1 FROM users u WHERE u.person_id = pt.patient_id)
               OR EXISTS (SELECT 1 FROM provider pv WHERE pv.person_id = pt.patient_id)
               OR EXISTS (SELECT 1 FROM encounter e WHERE e.patient_id = pt.patient_id)
               OR EXISTS (SELECT 1 FROM obs o WHERE o.person_id = pt.patient_id AND o.encounter_id IS NULL))
        ORDER BY pt.patient_id;
        SELECT COUNT(*) INTO v_rows FROM _target_unvoid_patient_id;
        CALL sp_cutoff_log(p_run_id, 'identified', 'patient (to un-void)', v_rows);
    END IF;

    IF p_dry_run = 0 THEN
        CALL sp_cutoff_set_void_batched(p_run_id, 'patient_state', '', '_target_patient_id', p_batch, v_reason, 0);
        CALL sp_cutoff_set_void_batched(p_run_id, 'patient_program', 'patient_id', '_target_patient_id', p_batch, v_reason, 0);
        CALL sp_cutoff_set_void_batched(p_run_id, 'patient_identifier', 'patient_id', '_target_patient_id', p_batch, v_reason, 0);
        CALL sp_cutoff_set_void_batched(p_run_id, 'person_attribute', 'person_id', '_target_patient_id', p_batch, v_reason, 0);
        CALL sp_cutoff_set_void_batched(p_run_id, 'person_address', 'person_id', '_target_patient_id', p_batch, v_reason, 0);
        CALL sp_cutoff_set_void_batched(p_run_id, 'person_name', 'person_id', '_target_patient_id', p_batch, v_reason, 0);
        CALL sp_cutoff_set_void_batched(p_run_id, 'patient', 'patient_id', '_target_patient_id', p_batch, v_reason, 0);
        CALL sp_cutoff_set_void_batched(p_run_id, 'person', 'person_id', '_target_patient_id', p_batch, v_reason, 0);

        IF p_use_staged = 0 THEN
            CALL sp_cutoff_set_void_batched(p_run_id, 'patient_state', '', '_target_unvoid_patient_id', p_batch, v_reason, 1);
            CALL sp_cutoff_set_void_batched(p_run_id, 'patient_program', 'patient_id', '_target_unvoid_patient_id', p_batch, v_reason, 1);
            CALL sp_cutoff_set_void_batched(p_run_id, 'patient_identifier', 'patient_id', '_target_unvoid_patient_id', p_batch, v_reason, 1);
            CALL sp_cutoff_set_void_batched(p_run_id, 'person_attribute', 'person_id', '_target_unvoid_patient_id', p_batch, v_reason, 1);
            CALL sp_cutoff_set_void_batched(p_run_id, 'person_address', 'person_id', '_target_unvoid_patient_id', p_batch, v_reason, 1);
            CALL sp_cutoff_set_void_batched(p_run_id, 'person_name', 'person_id', '_target_unvoid_patient_id', p_batch, v_reason, 1);
            CALL sp_cutoff_set_void_batched(p_run_id, 'patient', 'patient_id', '_target_unvoid_patient_id', p_batch, v_reason, 1);
            CALL sp_cutoff_set_void_batched(p_run_id, 'person', 'person_id', '_target_unvoid_patient_id', p_batch, v_reason, 1);
        END IF;
    END IF;
END$$

-- ONE-TIME purge of encounters/obs/orders/lab rows before the cutoff, followed by the patient reconcile.
DROP PROCEDURE IF EXISTS sp_apply_data_cutoff$$
CREATE PROCEDURE sp_apply_data_cutoff(IN p_cutoff_date DATE, IN p_dry_run TINYINT, IN p_batch INT)
BEGIN
    DECLARE v_run CHAR(36) DEFAULT UUID();
    DECLARE v_rows BIGINT DEFAULT 0;
    DECLARE v_old4 BIGINT DEFAULT 0;
    DECLARE v_changed BIGINT DEFAULT 0;

    IF p_cutoff_date IS NULL OR p_cutoff_date < '2000-01-01' OR p_cutoff_date > CURDATE() THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'p_cutoff_date must be between 2000-01-01 and today';
    END IF;
    IF p_batch IS NULL OR p_batch <= 0 THEN
        SET p_batch = 10000;
    END IF;
    SET @cutoff_blocked = 0;

    CALL sp_cutoff_ensure_tables();
    TRUNCATE TABLE _target_encounter_id;
    TRUNCATE TABLE _target_order_id;
    TRUNCATE TABLE _target_obs_id;

    -- ---- 1) identify ids (read-only on OpenMRS tables) ----
    SELECT COUNT(*) INTO v_old4 FROM encounter e
    WHERE (e.date_created IS NULL OR e.date_created < p_cutoff_date) AND (e.date_changed IS NULL OR e.date_changed < p_cutoff_date)
      AND (e.date_voided IS NULL OR e.date_voided < p_cutoff_date) AND (e.encounter_datetime IS NULL OR e.encounter_datetime < p_cutoff_date);

    INSERT INTO _target_encounter_id (encounter_id)
    SELECT e.encounter_id FROM encounter e
    LEFT JOIN (SELECT DISTINCT encounter_id FROM obs
               WHERE encounter_id IS NOT NULL AND (date_created >= p_cutoff_date OR date_voided >= p_cutoff_date)) r
           ON r.encounter_id = e.encounter_id
    WHERE r.encounter_id IS NULL
      AND (e.date_created IS NULL OR e.date_created < p_cutoff_date)
      AND (e.date_changed IS NULL OR e.date_changed < p_cutoff_date)
      AND (e.date_voided IS NULL OR e.date_voided < p_cutoff_date)
      AND (e.encounter_datetime IS NULL OR e.encounter_datetime < p_cutoff_date)
    ORDER BY e.encounter_id;
    SELECT COUNT(*) INTO v_rows FROM _target_encounter_id;
    CALL sp_cutoff_log(v_run, 'identified', 'encounter', v_rows);
    CALL sp_cutoff_log(v_run, 'kept only because of recent obs', 'encounter', v_old4 - v_rows);

    INSERT INTO _target_order_id (order_id)
    SELECT o.order_id FROM orders o JOIN _target_encounter_id t ON t.encounter_id = o.encounter_id
    ORDER BY o.order_id;
    SELECT COUNT(*) INTO v_rows FROM _target_order_id;
    CALL sp_cutoff_log(v_run, 'identified', 'orders (+drug_order, labtest_*)', v_rows);

    INSERT INTO _target_obs_id (obs_id)
    SELECT o.obs_id FROM obs o JOIN _target_encounter_id t ON t.encounter_id = o.encounter_id
    ORDER BY o.obs_id;
    -- encounter-less obs: old on their own, then protect groups / edit chains that contain a kept obs
    INSERT INTO _target_obs_id (obs_id)
    SELECT o.obs_id FROM obs o
    WHERE o.encounter_id IS NULL
      AND (o.date_created IS NULL OR o.date_created < p_cutoff_date)
      AND (o.date_voided IS NULL OR o.date_voided < p_cutoff_date)
    ORDER BY o.obs_id;
    REPEAT
        SET v_changed = 0;
        -- a kept obs points at this staged obs as its group parent -> keep it
        DELETE t FROM _target_obs_id t JOIN obs c ON c.obs_group_id = t.obs_id
        LEFT JOIN _target_obs_id k ON k.obs_id = c.obs_id WHERE k.obs_id IS NULL;
        SET v_changed = v_changed + ROW_COUNT();
        -- a kept obs points at this staged obs as its previous version -> keep it
        DELETE t FROM _target_obs_id t JOIN obs c ON c.previous_version = t.obs_id
        LEFT JOIN _target_obs_id k ON k.obs_id = c.obs_id WHERE k.obs_id IS NULL;
        SET v_changed = v_changed + ROW_COUNT();
        -- this staged obs is a member of a kept group -> keep it
        DELETE t FROM _target_obs_id t JOIN obs o ON o.obs_id = t.obs_id JOIN obs p ON p.obs_id = o.obs_group_id
        LEFT JOIN _target_obs_id k ON k.obs_id = p.obs_id WHERE k.obs_id IS NULL;
        SET v_changed = v_changed + ROW_COUNT();
        -- this staged obs is an older version of nothing but points at a kept previous version -> keep it
        DELETE t FROM _target_obs_id t JOIN obs o ON o.obs_id = t.obs_id JOIN obs p ON p.obs_id = o.previous_version
        LEFT JOIN _target_obs_id k ON k.obs_id = p.obs_id WHERE k.obs_id IS NULL;
        SET v_changed = v_changed + ROW_COUNT();
    UNTIL v_changed = 0 END REPEAT;
    SELECT COUNT(*) INTO v_rows FROM _target_obs_id;
    CALL sp_cutoff_log(v_run, 'identified', 'obs', v_rows);

    -- diagnostics: kept rows that point at a row about to be deleted (they will be set to NULL, see below)
    SELECT COUNT(*) INTO v_rows FROM obs o JOIN _target_obs_id t ON t.obs_id = o.obs_group_id
    LEFT JOIN _target_obs_id k ON k.obs_id = o.obs_id WHERE k.obs_id IS NULL;
    CALL sp_cutoff_log(v_run, 'kept row -> deleted row', 'obs.obs_group_id', v_rows);
    SELECT COUNT(*) INTO v_rows FROM obs o JOIN _target_obs_id t ON t.obs_id = o.previous_version
    LEFT JOIN _target_obs_id k ON k.obs_id = o.obs_id WHERE k.obs_id IS NULL;
    CALL sp_cutoff_log(v_run, 'kept row -> deleted row', 'obs.previous_version', v_rows);
    SELECT COUNT(*) INTO v_rows FROM obs o JOIN _target_order_id t ON t.order_id = o.order_id
    LEFT JOIN _target_obs_id k ON k.obs_id = o.obs_id WHERE k.obs_id IS NULL;
    CALL sp_cutoff_log(v_run, 'kept row -> deleted row', 'obs.order_id', v_rows);
    SELECT COUNT(*) INTO v_rows FROM orders o JOIN _target_order_id t ON t.order_id = o.previous_order_id
    LEFT JOIN _target_order_id k ON k.order_id = o.order_id WHERE k.order_id IS NULL;
    CALL sp_cutoff_log(v_run, 'kept row -> deleted row', 'orders.previous_order_id', v_rows);
    SELECT COUNT(*) INTO v_rows FROM patient_state ps JOIN _target_encounter_id t ON t.encounter_id = ps.encounter_id;
    CALL sp_cutoff_log(v_run, 'kept row -> deleted row', 'patient_state.encounter_id', v_rows);

    IF p_dry_run = 1 THEN
        CALL sp_cutoff_reconcile_patients(v_run, p_cutoff_date, 1, p_batch, 1);
        CALL sp_cutoff_log(v_run, 'dry_run_complete', '-', 0);
    ELSE
        -- ---- 2) apply ----
        -- detach kept rows that point at rows about to be deleted
        UPDATE patient_state ps JOIN _target_encounter_id t ON t.encounter_id = ps.encounter_id
        SET ps.encounter_id = NULL;
        CALL sp_cutoff_log(v_run, 'detached', 'patient_state.encounter_id', ROW_COUNT());

        UPDATE orders o
        JOIN _target_order_id t ON t.order_id = o.previous_order_id
        LEFT JOIN _target_order_id k ON k.order_id = o.order_id
        SET o.previous_order_id = NULL WHERE k.order_id IS NULL;
        CALL sp_cutoff_log(v_run, 'detached', 'orders.previous_order_id', ROW_COUNT());

        UPDATE obs o
        JOIN _target_order_id t ON t.order_id = o.order_id
        LEFT JOIN _target_obs_id k ON k.obs_id = o.obs_id
        SET o.order_id = NULL WHERE k.obs_id IS NULL;
        CALL sp_cutoff_log(v_run, 'detached', 'obs.order_id', ROW_COUNT());

        UPDATE obs o
        JOIN _target_obs_id t ON t.obs_id = o.obs_group_id
        LEFT JOIN _target_obs_id k ON k.obs_id = o.obs_id
        SET o.obs_group_id = NULL WHERE k.obs_id IS NULL;
        CALL sp_cutoff_log(v_run, 'detached', 'obs.obs_group_id', ROW_COUNT());

        UPDATE obs o
        JOIN _target_obs_id t ON t.obs_id = o.previous_version
        LEFT JOIN _target_obs_id k ON k.obs_id = o.obs_id
        SET o.previous_version = NULL WHERE k.obs_id IS NULL;
        CALL sp_cutoff_log(v_run, 'detached', 'obs.previous_version', ROW_COUNT());
        COMMIT;

        -- delete, children first
        CALL sp_cutoff_delete_batched(v_run, 'labtest_attribute', 'test_order_id', '_target_order_id', 'order_id', p_batch);
        CALL sp_cutoff_delete_batched(v_run, 'labtest_sample', 'test_order_id', '_target_order_id', 'order_id', p_batch);
        CALL sp_cutoff_delete_batched(v_run, 'labtest_test', 'test_order_id', '_target_order_id', 'order_id', p_batch);
        CALL sp_cutoff_delete_batched(v_run, 'drug_order', 'order_id', '_target_order_id', 'order_id', p_batch);
        CALL sp_cutoff_delete_batched(v_run, 'obs', 'obs_id', '_target_obs_id', 'obs_id', p_batch);
        CALL sp_cutoff_delete_batched(v_run, 'orders', 'order_id', '_target_order_id', 'order_id', p_batch);
        CALL sp_cutoff_delete_batched(v_run, 'encounter_provider', 'encounter_id', '_target_encounter_id', 'encounter_id', p_batch);
        CALL sp_cutoff_delete_batched(v_run, 'encounter', 'encounter_id', '_target_encounter_id', 'encounter_id', p_batch);

        -- patients: look at the tables as they are now
        CALL sp_cutoff_reconcile_patients(v_run, p_cutoff_date, 0, p_batch, 0);

        IF @cutoff_blocked = 0 THEN
            CALL sp_cutoff_log(v_run, 'apply_complete', '-', 0);
        ELSE
            CALL sp_cutoff_log(v_run, 'apply_INCOMPLETE (see BLOCKED rows)', '-', @cutoff_blocked);
        END IF;
    END IF;

    SELECT step, table_name, rows_affected FROM _cutoff_log WHERE run_id = v_run ORDER BY id;
END$$

DELIMITER ;
