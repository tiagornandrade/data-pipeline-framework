INSERT INTO analytics.stg_location (id, column1, column2)
SELECT id, column1, column2 FROM public."location";
