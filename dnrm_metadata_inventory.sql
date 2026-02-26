-- DNRM metadata inventory script (SQL*Plus / SQLcl / SQL Developer compatible)
set serveroutput on size unlimited
set define off

DECLARE
  v_run_ts   TIMESTAMP WITH TIME ZONE := SYSTIMESTAMP;

  PROCEDURE ensure_table(p_ddl CLOB) IS
  BEGIN
    EXECUTE IMMEDIATE p_ddl;
  EXCEPTION
    WHEN OTHERS THEN
      IF SQLCODE != -955 THEN
        RAISE;
      END IF;
  END ensure_table;

  PROCEDURE log_table_error(
    p_owner      IN VARCHAR2,
    p_table_name IN VARCHAR2,
    p_err        IN VARCHAR2
  ) IS
  BEGIN
    INSERT INTO dnrm_inv_table (
      run_ts,
      owner,
      table_name,
      notes
    ) VALUES (
      v_run_ts,
      p_owner,
      p_table_name,
      SUBSTR(p_err, 1, 4000)
    );
  EXCEPTION
    WHEN OTHERS THEN
      NULL;
  END log_table_error;
BEGIN
  ensure_table(q'[
    CREATE TABLE dnrm_inv_run (
      run_ts          TIMESTAMP WITH TIME ZONE PRIMARY KEY,
      inv_user        VARCHAR2(128),
      current_schema  VARCHAR2(128),
      db_name         VARCHAR2(128),
      con_name        VARCHAR2(128),
      started_at      TIMESTAMP WITH TIME ZONE,
      completed_at    TIMESTAMP WITH TIME ZONE,
      notes           VARCHAR2(4000)
    )
  ]');

  ensure_table(q'[
    CREATE TABLE dnrm_inv_table (
      run_ts               TIMESTAMP WITH TIME ZONE NOT NULL,
      owner                VARCHAR2(128) NOT NULL,
      table_name           VARCHAR2(128) NOT NULL,
      tablespace_name      VARCHAR2(30),
      temporary_flag       VARCHAR2(1),
      partitioned_flag     VARCHAR2(3),
      iot_type             VARCHAR2(12),
      logging_flag         VARCHAR2(3),
      compression_flag     VARCHAR2(8),
      compress_for         VARCHAR2(30),
      degree               VARCHAR2(40),
      num_rows_stats       NUMBER,
      stats_last_analyzed  DATE,
      avg_row_len          NUMBER,
      column_count         NUMBER,
      exact_row_count      NUMBER,
      notes                VARCHAR2(4000)
    )
  ]');

  ensure_table(q'[
    CREATE TABLE dnrm_inv_column (
      run_ts                TIMESTAMP WITH TIME ZONE NOT NULL,
      owner                 VARCHAR2(128) NOT NULL,
      table_name            VARCHAR2(128) NOT NULL,
      column_id             NUMBER,
      column_name           VARCHAR2(128),
      data_type             VARCHAR2(128),
      data_length           NUMBER,
      data_precision        NUMBER,
      data_scale            NUMBER,
      char_col_decl_length  NUMBER,
      char_used             VARCHAR2(1),
      nullable_flag         VARCHAR2(1),
      data_default          CLOB
    )
  ]');

  ensure_table(q'[
    CREATE TABLE dnrm_inv_constraint (
      run_ts             TIMESTAMP WITH TIME ZONE NOT NULL,
      owner              VARCHAR2(128) NOT NULL,
      table_name         VARCHAR2(128) NOT NULL,
      constraint_name    VARCHAR2(128) NOT NULL,
      constraint_type    VARCHAR2(1),
      constraint_type_desc VARCHAR2(30),
      status             VARCHAR2(8),
      validated          VARCHAR2(13),
      deferrable         VARCHAR2(14),
      deferred_state     VARCHAR2(9),
      delete_rule        VARCHAR2(9),
      r_owner            VARCHAR2(128),
      r_constraint_name  VARCHAR2(128),
      column_position    NUMBER,
      column_name        VARCHAR2(128)
    )
  ]');

  ensure_table(q'[
    CREATE TABLE dnrm_inv_index (
      run_ts             TIMESTAMP WITH TIME ZONE NOT NULL,
      table_owner        VARCHAR2(128) NOT NULL,
      table_name         VARCHAR2(128) NOT NULL,
      index_owner        VARCHAR2(128) NOT NULL,
      index_name         VARCHAR2(128) NOT NULL,
      uniqueness         VARCHAR2(9),
      status             VARCHAR2(8),
      index_type         VARCHAR2(27),
      column_position    NUMBER,
      column_name        VARCHAR2(4000),
      descend_flag       VARCHAR2(4)
    )
  ]');

  INSERT INTO dnrm_inv_run (
    run_ts,
    inv_user,
    current_schema,
    db_name,
    con_name,
    started_at,
    notes
  ) VALUES (
    v_run_ts,
    USER,
    SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA'),
    SYS_CONTEXT('USERENV', 'DB_NAME'),
    SYS_CONTEXT('USERENV', 'CON_NAME'),
    v_run_ts,
    'Inventory for accessible *_DNRM tables.'
  );

  FOR t IN (
    SELECT
      at.owner,
      at.table_name,
      at.tablespace_name,
      at.temporary,
      at.partitioned,
      at.iot_type,
      at.logging,
      at.compression,
      at.compress_for,
      at.degree,
      at.num_rows,
      at.last_analyzed,
      at.avg_row_len,
      (
        SELECT COUNT(*)
        FROM all_tab_columns c
        WHERE c.owner = at.owner
          AND c.table_name = at.table_name
      ) AS column_count
    FROM all_tables at
    WHERE at.table_name LIKE '%\_DNRM' ESCAPE '\'
    ORDER BY at.owner, at.table_name
  ) LOOP
    DECLARE
      v_exact_count NUMBER;
      v_notes       VARCHAR2(4000);
      v_sql         VARCHAR2(1000);
    BEGIN
      v_exact_count := NULL;
      v_notes := NULL;

      IF t.num_rows IS NOT NULL THEN
        v_notes := 'Used dictionary stats NUM_ROWS; exact COUNT(*) skipped.';
      ELSE
        BEGIN
          v_sql :=
            'SELECT COUNT(*) FROM "' || REPLACE(t.owner, '"', '""') || '"."' || REPLACE(t.table_name, '"', '""') || '"';
          EXECUTE IMMEDIATE v_sql INTO v_exact_count;
          v_notes := 'NUM_ROWS unavailable; exact COUNT(*) computed.';
        EXCEPTION
          WHEN OTHERS THEN
            v_notes := 'COUNT(*) failed: ' || SQLERRM;
        END;
      END IF;

      INSERT INTO dnrm_inv_table (
        run_ts,
        owner,
        table_name,
        tablespace_name,
        temporary_flag,
        partitioned_flag,
        iot_type,
        logging_flag,
        compression_flag,
        compress_for,
        degree,
        num_rows_stats,
        stats_last_analyzed,
        avg_row_len,
        column_count,
        exact_row_count,
        notes
      ) VALUES (
        v_run_ts,
        t.owner,
        t.table_name,
        t.tablespace_name,
        t.temporary,
        t.partitioned,
        t.iot_type,
        t.logging,
        t.compression,
        t.compress_for,
        t.degree,
        t.num_rows,
        t.last_analyzed,
        t.avg_row_len,
        t.column_count,
        v_exact_count,
        SUBSTR(v_notes, 1, 4000)
      );

      BEGIN
        INSERT INTO dnrm_inv_column (
          run_ts,
          owner,
          table_name,
          column_id,
          column_name,
          data_type,
          data_length,
          data_precision,
          data_scale,
          char_col_decl_length,
          char_used,
          nullable_flag,
          data_default
        )
        SELECT
          v_run_ts,
          c.owner,
          c.table_name,
          c.column_id,
          c.column_name,
          c.data_type,
          c.data_length,
          c.data_precision,
          c.data_scale,
          c.char_col_decl_length,
          c.char_used,
          c.nullable,
          TO_LOB(c.data_default)
        FROM all_tab_columns c
        WHERE c.owner = t.owner
          AND c.table_name = t.table_name
        ORDER BY c.column_id;
      EXCEPTION
        WHEN OTHERS THEN
          UPDATE dnrm_inv_table
          SET notes = SUBSTR(NVL(notes, '') || ' Column metadata failed: ' || SQLERRM, 1, 4000)
          WHERE run_ts = v_run_ts
            AND owner = t.owner
            AND table_name = t.table_name;
      END;

      BEGIN
        INSERT INTO dnrm_inv_constraint (
          run_ts,
          owner,
          table_name,
          constraint_name,
          constraint_type,
          constraint_type_desc,
          status,
          validated,
          deferrable,
          deferred_state,
          delete_rule,
          r_owner,
          r_constraint_name,
          column_position,
          column_name
        )
        SELECT
          v_run_ts,
          ac.owner,
          ac.table_name,
          ac.constraint_name,
          ac.constraint_type,
          CASE ac.constraint_type
            WHEN 'P' THEN 'PRIMARY KEY'
            WHEN 'U' THEN 'UNIQUE'
            WHEN 'R' THEN 'FOREIGN KEY'
            WHEN 'C' THEN 'CHECK'
            ELSE 'OTHER'
          END AS constraint_type_desc,
          ac.status,
          ac.validated,
          ac.deferrable,
          ac.deferred,
          ac.delete_rule,
          ac.r_owner,
          ac.r_constraint_name,
          acc.position,
          acc.column_name
        FROM all_constraints ac
        LEFT JOIN all_cons_columns acc
          ON acc.owner = ac.owner
         AND acc.constraint_name = ac.constraint_name
         AND acc.table_name = ac.table_name
        WHERE ac.owner = t.owner
          AND ac.table_name = t.table_name
          AND ac.constraint_type IN ('P', 'U', 'R', 'C')
        ORDER BY ac.constraint_name, acc.position;
      EXCEPTION
        WHEN OTHERS THEN
          UPDATE dnrm_inv_table
          SET notes = SUBSTR(NVL(notes, '') || ' Constraint metadata failed: ' || SQLERRM, 1, 4000)
          WHERE run_ts = v_run_ts
            AND owner = t.owner
            AND table_name = t.table_name;
      END;

      BEGIN
        INSERT INTO dnrm_inv_index (
          run_ts,
          table_owner,
          table_name,
          index_owner,
          index_name,
          uniqueness,
          status,
          index_type,
          column_position,
          column_name,
          descend_flag
        )
        SELECT
          v_run_ts,
          ai.table_owner,
          ai.table_name,
          ai.owner AS index_owner,
          ai.index_name,
          ai.uniqueness,
          ai.status,
          ai.index_type,
          aic.column_position,
          aic.column_name,
          aic.descend
        FROM all_indexes ai
        LEFT JOIN all_ind_columns aic
          ON aic.index_owner = ai.owner
         AND aic.index_name = ai.index_name
         AND aic.table_owner = ai.table_owner
         AND aic.table_name = ai.table_name
        WHERE ai.table_owner = t.owner
          AND ai.table_name = t.table_name
        ORDER BY ai.owner, ai.index_name, aic.column_position;
      EXCEPTION
        WHEN OTHERS THEN
          UPDATE dnrm_inv_table
          SET notes = SUBSTR(NVL(notes, '') || ' Index metadata failed: ' || SQLERRM, 1, 4000)
          WHERE run_ts = v_run_ts
            AND owner = t.owner
            AND table_name = t.table_name;
      END;

    EXCEPTION
      WHEN OTHERS THEN
        log_table_error(t.owner, t.table_name, 'Table processing failed: ' || SQLERRM);
    END;
  END LOOP;

  UPDATE dnrm_inv_run
  SET completed_at = SYSTIMESTAMP
  WHERE run_ts = v_run_ts;

  COMMIT;

  DBMS_OUTPUT.PUT_LINE('DNRM metadata inventory complete. RUN_TS=' || TO_CHAR(v_run_ts, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'));
END;
/
