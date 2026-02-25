# Создание витрины

```SQL
-- Витрина
CREATE TABLE cdm.courier_payouts (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    courier_id VARCHAR(50) NOT NULL,
    courier_name VARCHAR(200) NOT NULL,
    settlement_year INTEGER NOT NULL,
    settlement_month INTEGER NOT NULL,
    orders_count INTEGER DEFAULT 0,
    orders_total_sum DECIMAL(12,2) DEFAULT 0,
    rate_avg DECIMAL(3,2) DEFAULT 0,
    order_processing_fee DECIMAL(12,2) DEFAULT 0,
    courier_order_sum DECIMAL(12,2) DEFAULT 0,
    courier_tips_sum DECIMAL(12,2) DEFAULT 0,
    courier_reward_sum DECIMAL(12,2) DEFAULT 0,
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(courier_id, settlement_year, settlement_month)
);

--Staging
-- Создание схемы stg, если она не существует
CREATE SCHEMA IF NOT EXISTS stg;



-- Таблица для курьеров (stg.api_couriers)
CREATE TABLE IF NOT EXISTS stg.api_couriers (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    object_id VARCHAR NOT NULL,
    object_value JSON NOT NULL,
    update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_object_id_couriers UNIQUE(object_id)
);

-- Таблица для доставок (stg.api_deliveries)
CREATE TABLE IF NOT EXISTS stg.api_deliveries (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    object_id VARCHAR NOT NULL,
    object_value JSON NOT NULL,
    update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_object_id_deliveries UNIQUE(object_id)
);

-- Таблица для ресторанов (stg.api_restaurants)
CREATE TABLE IF NOT EXISTS stg.api_restaurants (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    object_id VARCHAR NOT NULL,
    object_value JSON NOT NULL,
    update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_object_id_restaurants UNIQUE(object_id)
);

-- DDS
-- Курьеры
CREATE TABLE IF NOT EXISTS dds.dm_couriers (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    courier_id VARCHAR NOT NULL,
    courier_name VARCHAR NOT NULL,
    CONSTRAINT unique_courier_id UNIQUE(courier_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Доставки
CREATE TABLE dds.dm_deliveries (
	id int4 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE) NOT NULL,
	order_key varchar NOT NULL,
	delivery_key varchar NOT NULL,
	courier_id int4 NOT NULL,
	order_ts timestamp NOT NULL,
	delivery_ts timestamp NOT NULL,
	address varchar NOT NULL,
	rate int4 NOT NULL,
	sum numeric(10, 2) NOT NULL,
	tip_sum numeric(10, 2) NOT NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT dm_deliveries_pkey PRIMARY KEY (id),
	CONSTRAINT dm_deliveries_rate_check CHECK (((rate >= 1) AND (rate <= 5))),
	CONSTRAINT dm_deliveries_sum_check CHECK ((sum >= (0)::numeric)),
	CONSTRAINT dm_deliveries_tip_sum_check CHECK ((tip_sum >= (0)::numeric)),
	CONSTRAINT unique_delivery_key UNIQUE (delivery_key),
	CONSTRAINT fk_delivery_courier FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id),
	CONSTRAINT fk_delivery_order FOREIGN KEY (order_key) REFERENCES dds.dm_orders(order_key)
);



CREATE TABLE dds.fct_couriers_deliveries (
	id int4 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE) NOT NULL,
	delivery_key varchar NOT NULL,
	order_key int4 NOT NULL,
	order_ts timestamp NOT NULL,
	delivery_ts timestamp NOT NULL,
	delivery_price numeric(10, 2) NOT NULL,
	courier_id int4 NOT NULL,
	courier_name varchar NOT NULL,
	rate int4 NOT NULL,
	tip_sum numeric(10, 2) NOT NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT fct_couriers_deliveries_pkey PRIMARY KEY (id),
	CONSTRAINT fct_couriers_deliveries_unique UNIQUE (delivery_key, courier_id, order_key)
);

```
