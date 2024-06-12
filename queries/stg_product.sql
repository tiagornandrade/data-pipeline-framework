INSERT INTO analytics.stg_product (id, column1, column2)
SELECT id, column1, column2 FROM public."product";
