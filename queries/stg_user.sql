INSERT INTO analytics.stg_user (id, column1, column2)
SELECT id, column1, column2 FROM public."user";
