package storage

const sqlInsertEntity = `
WITH new_entity AS (
    SELECT ent_id::text
         , data::float
         , value::float
    FROM (
             VALUES %s
             ) as ss_temp (ent_id, data, value)
)

INSERT INTO entity_table(ent_id, data, value)
SELECT ent_id, data, value
FROM new_entity
ON CONFLICT (ent_id) DO UPDATE
    SET data = EXCLUDED.data
      , value = EXCLUDED.value
      , date_updated = timezone('utc'::text, CURRENT_TIMESTAMP)
    WHERE 1 = 2
        OR  EXCLUDED.data != entity_table.data
        OR EXCLUDED.value != entity_table.value
RETURNING entity_table.ent_id
    , entity_table.data
    , entity_table.value
    , entity_table.date_added
    , entity_table.date_updated
;
`
