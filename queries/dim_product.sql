UPDATE analytics.dim_product AS dim
SET
    column1 = stg.column1,
    column2 = stg.column2
FROM analytics.stg_product AS stg
WHERE dim.id = stg.id;

INSERT INTO analytics.dim_product (id, column1, column2)
SELECT id, column1, column2
FROM analytics.stg_product AS stg
WHERE NOT EXISTS (
    SELECT 1
    FROM analytics.dim_product AS dim
    WHERE dim.id = stg.id
)
ON CONFLICT (id) DO NOTHING;
