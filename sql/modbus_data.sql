-- Table: public.modbus_data

-- DROP TABLE IF EXISTS public.modbus_data;

CREATE TABLE IF NOT EXISTS public.modbus_data
(
    id integer NOT NULL DEFAULT nextval('modbus_data_id_seq'::regclass),
    device_id integer NOT NULL,
    "timestamp" timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    register_name text COLLATE pg_catalog."default" NOT NULL,
    value double precision,
    CONSTRAINT modbus_data_pkey PRIMARY KEY (device_id, "timestamp", register_name)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.modbus_data
    OWNER to sigma;
-- Index: idx_modbus_data_register_timestamp_desc

-- DROP INDEX IF EXISTS public.idx_modbus_data_register_timestamp_desc;

CREATE INDEX IF NOT EXISTS idx_modbus_data_register_timestamp_desc
    ON public.modbus_data USING btree
    (register_name COLLATE pg_catalog."default" ASC NULLS LAST, "timestamp" DESC NULLS FIRST)
    WITH (fillfactor=100, deduplicate_items=True)
    TABLESPACE pg_default;
-- Index: idx_modbus_data_timestamp_register

-- DROP INDEX IF EXISTS public.idx_modbus_data_timestamp_register;

CREATE INDEX IF NOT EXISTS idx_modbus_data_timestamp_register
    ON public.modbus_data USING btree
    ("timestamp" ASC NULLS LAST, register_name COLLATE pg_catalog."default" ASC NULLS LAST)
    WITH (fillfactor=100, deduplicate_items=True)
    TABLESPACE pg_default;
-- Index: idx_modbus_device_timestamp

-- DROP INDEX IF EXISTS public.idx_modbus_device_timestamp;

CREATE INDEX IF NOT EXISTS idx_modbus_device_timestamp
    ON public.modbus_data USING btree
    (device_id ASC NULLS LAST, "timestamp" DESC NULLS FIRST)
    WITH (fillfactor=90, deduplicate_items=True)
    TABLESPACE pg_default;
-- Index: idx_modbus_timestamp_register_value

-- DROP INDEX IF EXISTS public.idx_modbus_timestamp_register_value;

CREATE INDEX IF NOT EXISTS idx_modbus_timestamp_register_value
    ON public.modbus_data USING btree
    ("timestamp" DESC NULLS FIRST, register_name COLLATE pg_catalog."default" ASC NULLS LAST, value ASC NULLS LAST)
    WITH (fillfactor=90, deduplicate_items=True)
    TABLESPACE pg_default;