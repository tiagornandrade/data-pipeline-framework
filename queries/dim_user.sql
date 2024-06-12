UPDATE analytics.dim_user AS dim
SET
    column1 = stg.column1,
    column2 = stg.column2
FROM analytics.stg_user AS stg
WHERE dim.id = stg.id;

INSERT INTO analytics.dim_user (id, column1, column2)
SELECT id, column1, column2
FROM analytics.stg_user AS stg
WHERE NOT EXISTS (
    SELECT 1
    FROM analytics.dim_user AS dim
    WHERE dim.id = stg.id
)
ON CONFLICT (id) DO NOTHING;
