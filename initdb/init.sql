CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS public.user (id INT, column1 INT, column2 INT);
CREATE TABLE IF NOT EXISTS analytics.stg_user (id INT, column1 INT, column2 INT);
CREATE TABLE IF NOT EXISTS analytics.dim_user (id INT, column1 INT, column2 INT);

CREATE TABLE IF NOT EXISTS public.product (id INT, column1 INT, column2 INT);
CREATE TABLE IF NOT EXISTS analytics.stg_product (id INT, column1 INT, column2 INT);
CREATE TABLE IF NOT EXISTS analytics.dim_product (id INT, column1 INT, column2 INT);

CREATE TABLE IF NOT EXISTS public.location (id INT, column1 INT, column2 INT);
CREATE TABLE IF NOT EXISTS analytics.stg_location (id INT, column1 INT, column2 INT);
CREATE TABLE IF NOT EXISTS analytics.dim_location (id INT, column1 INT, column2 INT);

ALTER TABLE IF EXISTS analytics.dim_user
ADD CONSTRAINT unique_id_user UNIQUE (id);

ALTER TABLE IF EXISTS analytics.dim_product
ADD CONSTRAINT unique_id_product UNIQUE (id);

ALTER TABLE IF EXISTS analytics.dim_location
ADD CONSTRAINT unique_id_location UNIQUE (id);

INSERT INTO public.user VALUES (1, 1, 1);
INSERT INTO public.user VALUES (2, 1, 2);

INSERT INTO public.product VALUES (1, 1, 1);
INSERT INTO public.product VALUES (2, 1, 2);

INSERT INTO public.location VALUES (1, 1, 1);
INSERT INTO public.location VALUES (2, 1, 2);