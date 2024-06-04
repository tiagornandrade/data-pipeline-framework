CREATE SCHEMA analytics;

CREATE TABLE analytics.dim_user (id INT, column1 INT, column2 INT);
CREATE TABLE public.stg_user (id INT, column1 INT, column2 INT);


INSERT INTO public.stg_user VALUES (1, 1, 1);
INSERT INTO public.stg_user VALUES (2, 1, 2);

SELECT * FROM public.stg_user;
SELECT * FROM analytics.dim_user;