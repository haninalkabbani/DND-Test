WITH
  FUNCTION exact_rowcount(p_owner IN VARCHAR2, p_table_name IN VARCHAR2)
    RETURN NUMBER
  IS
    l_cnt NUMBER;
  BEGIN
    EXECUTE IMMEDIATE
      'SELECT COUNT(*) FROM "' || REPLACE(p_owner, '"', '""') || '"."' || REPLACE(p_table_name, '"', '""') || '"'
      INTO l_cnt;
    RETURN l_cnt;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN NULL;
  END;
SELECT
  t.owner,
  t.table_name,
  t.num_rows,
  t.last_analyzed,
  exact_rowcount(t.owner, t.table_name) AS exact_rowcount,
  COUNT(*) OVER (PARTITION BY c.owner, c.table_name) AS column_count,
  c.column_id,
  c.column_name,
  c.data_type,
  c.data_length,
  c.data_precision,
  c.data_scale,
  c.nullable,
  c.data_default
FROM all_tables t
JOIN all_tab_columns c
  ON c.owner = t.owner
 AND c.table_name = t.table_name
WHERE t.table_name LIKE '%\_DNRM' ESCAPE '\\'
ORDER BY t.owner, t.table_name, c.column_id;
