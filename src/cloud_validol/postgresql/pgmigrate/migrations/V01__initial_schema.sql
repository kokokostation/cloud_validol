-- creating users

CREATE USER validol_internal PASSWORD 'validol_internal';
CREATE USER validol_reader PASSWORD 'validol_reader';


--- internal schema

CREATE SCHEMA validol_internal;
GRANT USAGE ON SCHEMA validol_internal TO validol_internal;
ALTER DEFAULT PRIVILEGES IN SCHEMA validol_internal GRANT ALL PRIVILEGES ON SEQUENCES TO validol_internal;
ALTER DEFAULT PRIVILEGES IN SCHEMA validol_internal GRANT ALL PRIVILEGES ON TABLES TO validol_internal;

CREATE TABLE validol_internal.investing_prices_info
(
    id             BIGSERIAL PRIMARY KEY,
    currency_cross VARCHAR NOT NULL,

    UNIQUE (currency_cross)
);

CREATE TABLE validol_internal.investing_prices_data
(
    id          BIGSERIAL PRIMARY KEY,
    series_id   BIGINT      NOT NULL REFERENCES validol_internal.investing_prices_info (id),
    event_dttm  TIMESTAMPTZ NOT NULL,
    open_price  DECIMAL     NOT NULL,
    high_price  DECIMAL     NOT NULL,
    low_price   DECIMAL     NOT NULL,
    close_price DECIMAL     NOT NULL,

    UNIQUE (series_id, event_dttm)
);

CREATE TABLE validol_internal.fredgraph_info
(
    id BIGSERIAL PRIMARY KEY
);

INSERT INTO validol_internal.fredgraph_info (id)
VALUES (1);

CREATE TABLE validol_internal.fredgraph_data
(
    id         BIGSERIAL PRIMARY KEY,
    series_id  BIGINT      NOT NULL DEFAULT 1 REFERENCES validol_internal.fredgraph_info (id),
    event_dttm TIMESTAMPTZ NOT NULL,
    mbase      DECIMAL,
    tdebt      DECIMAL,

    UNIQUE (series_id, event_dttm)
);

CREATE TABLE validol_internal.moex_derivatives_info
(
    id   BIGSERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,

    UNIQUE (name)
);

CREATE TABLE validol_internal.moex_derivatives_data
(
    id         BIGSERIAL PRIMARY KEY,
    series_id  BIGINT      NOT NULL REFERENCES validol_internal.moex_derivatives_info (id),
    event_dttm TIMESTAMPTZ NOT NULL,
    fl         DECIMAL,
    fs         DECIMAL,
    ul         DECIMAL,
    us         DECIMAL,
    flq        DECIMAL,
    fsq        DECIMAL,
    ulq        DECIMAL,
    usq        DECIMAL,

    UNIQUE (series_id, event_dttm)
);

CREATE TABLE validol_internal.cot_derivatives_platform
(
    id     BIGSERIAL PRIMARY KEY,
    source VARCHAR NOT NULL,
    code   VARCHAR NOT NULL,
    name   VARCHAR NOT NULL,

    UNIQUE (source, code)
);

CREATE TYPE report_type AS ENUM ('futures_only', 'combined');

CREATE TABLE validol_internal.cot_derivatives_info
(
    id                          BIGSERIAL PRIMARY KEY,
    cot_derivatives_platform_id BIGINT      NOT NULL REFERENCES validol_internal.cot_derivatives_platform (id),
    name                        VARCHAR     NOT NULL,
    report_type                 report_type NOT NULL,

    UNIQUE (cot_derivatives_platform_id, name, report_type)
);

CREATE TABLE validol_internal.cot_futures_only_data
(
    id           BIGSERIAL PRIMARY KEY,
    series_id    BIGINT      NOT NULL REFERENCES validol_internal.cot_derivatives_info (id),
    event_dttm   TIMESTAMPTZ NOT NULL,
    oi           DECIMAL,
    ncl          DECIMAL,
    ncs          DECIMAL,
    cl           DECIMAL,
    cs           DECIMAL,
    nrl          DECIMAL,
    nrs          DECIMAL,
    x_4l_percent DECIMAL,
    x_4s_percent DECIMAL,
    x_8l_percent DECIMAL,
    x_8s_percent DECIMAL,

    UNIQUE (series_id, event_dttm)
);

CREATE TABLE validol_internal.cot_disaggregated_data
(
    id            BIGSERIAL PRIMARY KEY,
    series_id     BIGINT      NOT NULL REFERENCES validol_internal.cot_derivatives_info (id),
    event_dttm    TIMESTAMPTZ NOT NULL,
    oi            DECIMAL,
    nrl           DECIMAL,
    nrs           DECIMAL,
    pmpl          DECIMAL,
    pmps          DECIMAL,
    sdpl          DECIMAL,
    sdps          DECIMAL,
    mmpl          DECIMAL,
    mmps          DECIMAL,
    orpl          DECIMAL,
    orps          DECIMAL,
    x_4gl_percent DECIMAL,
    x_4gs_percent DECIMAL,
    x_8gl_percent DECIMAL,
    x_8gs_percent DECIMAL,
    x_4l_percent  DECIMAL,
    x_4s_percent  DECIMAL,
    x_8l_percent  DECIMAL,
    x_8s_percent  DECIMAL,
    sdp_spr       DECIMAL,
    mmp_spr       DECIMAL,
    orp_spr       DECIMAL,

    UNIQUE (series_id, event_dttm)
);

CREATE TABLE validol_internal.cot_financial_futures_data
(
    id         BIGSERIAL PRIMARY KEY,
    series_id  BIGINT      NOT NULL REFERENCES validol_internal.cot_derivatives_info (id),
    event_dttm TIMESTAMPTZ NOT NULL,
    oi         DECIMAL,
    dipl       DECIMAL,
    dips       DECIMAL,
    dip_spr    DECIMAL,
    ampl       DECIMAL,
    amps       DECIMAL,
    amp_spr    DECIMAL,
    lmpl       DECIMAL,
    lmps       DECIMAL,
    lmp_spr    DECIMAL,
    orpl       DECIMAL,
    orps       DECIMAL,
    orp_spr    DECIMAL,
    nrl        DECIMAL,
    nrs        DECIMAL,

    UNIQUE (series_id, event_dttm)
);


-- interface

CREATE SCHEMA validol_interface;
GRANT USAGE ON SCHEMA validol_interface TO validol_internal;
ALTER DEFAULT PRIVILEGES IN SCHEMA validol_interface GRANT SELECT ON TABLES TO validol_internal;
GRANT SELECT ON ALL TABLES IN SCHEMA validol_interface TO validol_internal;

CREATE VIEW validol_interface.investing_prices_index AS
SELECT id AS series_id,
       currency_cross
FROM validol_internal.investing_prices_info;

CREATE VIEW validol_interface.investing_prices_data AS
SELECT *
FROM validol_internal.investing_prices_data;

CREATE VIEW validol_interface.fredgraph_index AS
SELECT id AS series_id
FROM validol_internal.fredgraph_info;

CREATE VIEW validol_interface.fredgraph_data AS
SELECT *
FROM validol_internal.fredgraph_data;

CREATE VIEW validol_interface.moex_derivatives_index AS
SELECT id   AS series_id,
       name AS derivative_name
FROM validol_internal.moex_derivatives_info;

CREATE VIEW validol_interface.moex_derivatives_data AS
SELECT *
FROM validol_internal.moex_derivatives_data;

CREATE VIEW validol_interface.cot_derivatives_index AS
SELECT info.id         AS series_id,
       platform.source AS platform_source,
       platform.code   AS platform_code,
       platform.name   AS platform_name,
       info.name       AS derivative_name,
       info.report_type
FROM validol_internal.cot_derivatives_info AS info
         INNER JOIN validol_internal.cot_derivatives_platform AS platform
                    ON info.cot_derivatives_platform_id = platform.id;

CREATE VIEW validol_interface.cot_futures_only_index AS
SELECT DISTINCT ON (cot_derivatives_index.series_id) cot_derivatives_index.*
FROM validol_interface.cot_derivatives_index AS cot_derivatives_index
         INNER JOIN validol_internal.cot_futures_only_data AS data
                    ON data.series_id = cot_derivatives_index.series_id;

CREATE VIEW validol_interface.cot_futures_only_data AS
SELECT *
FROM validol_internal.cot_futures_only_data;

CREATE VIEW validol_interface.cot_disaggregated_index AS
SELECT DISTINCT ON (cot_derivatives_index.series_id) cot_derivatives_index.*
FROM validol_interface.cot_derivatives_index AS cot_derivatives_index
         INNER JOIN validol_internal.cot_disaggregated_data AS data
                    ON data.series_id = cot_derivatives_index.series_id;

CREATE VIEW validol_interface.cot_disaggregated_data AS
SELECT *,
       pmpl + sdpl AS cl,
       pmps + sdps AS cs,
       mmpl + orpl AS ncl,
       mmps + orps AS ncs
FROM validol_internal.cot_disaggregated_data;

CREATE VIEW validol_interface.cot_financial_futures_index AS
SELECT DISTINCT ON (cot_derivatives_index.series_id) cot_derivatives_index.*
FROM validol_interface.cot_derivatives_index AS cot_derivatives_index
         INNER JOIN validol_internal.cot_financial_futures_data AS data
                    ON data.series_id = cot_derivatives_index.series_id;

CREATE VIEW validol_interface.cot_financial_futures_data AS
SELECT *
FROM validol_internal.cot_financial_futures_data;


-- views for superset usage

CREATE SCHEMA validol;
GRANT USAGE ON SCHEMA validol TO validol_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA validol GRANT SELECT ON TABLES TO validol_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA validol TO validol_reader;
GRANT USAGE ON SCHEMA validol TO validol_internal;

-- the following code is generated, don't edit it by hand!

CREATE MATERIALIZED VIEW validol.fredgraph AS
SELECT data.event_dttm,
       FIRST_VALUE(data.mbase)
       OVER (PARTITION BY data.series_id, data.mbase_partition_value ORDER BY data.event_dttm) AS mbase,
       FIRST_VALUE(data.tdebt)
       OVER (PARTITION BY data.series_id, data.tdebt_partition_value ORDER BY data.event_dttm) AS tdebt
FROM (
         SELECT generated_dates.event_dttm,
                generated_dates.series_id,
                SUM(CASE WHEN data.mbase IS NULL THEN 0 ELSE 1 END) OVER w AS mbase_partition_value,
                SUM(CASE WHEN data.tdebt IS NULL THEN 0 ELSE 1 END) OVER w AS tdebt_partition_value,
                data.mbase,
                data.tdebt
         FROM validol_interface.fredgraph_data AS data
                  RIGHT JOIN (
             SELECT *
             FROM (SELECT generate_series('2010-01-01T00:00:00+00:00', NOW(), interval '1 day') AS event_dttm) AS q1
                      CROSS JOIN (SELECT series_id FROM validol_interface.fredgraph_index) AS q2
         ) AS generated_dates
                             ON (
                                     data.series_id = generated_dates.series_id AND
                                     data.event_dttm = generated_dates.event_dttm
                                 )
             WINDOW w AS (PARTITION BY generated_dates.series_id ORDER BY generated_dates.event_dttm)
     ) AS data
         INNER JOIN validol_interface.fredgraph_index AS index
                    ON index.series_id = data.series_id;


ALTER TABLE validol.fredgraph
    OWNER TO validol_internal;

CREATE MATERIALIZED VIEW validol.investing_prices AS
SELECT data.event_dttm,
       index.currency_cross,
       FIRST_VALUE(data.open_price)
       OVER (PARTITION BY data.series_id, data.open_price_partition_value ORDER BY data.event_dttm)  AS open_price,
       FIRST_VALUE(data.high_price)
       OVER (PARTITION BY data.series_id, data.high_price_partition_value ORDER BY data.event_dttm)  AS high_price,
       FIRST_VALUE(data.low_price)
       OVER (PARTITION BY data.series_id, data.low_price_partition_value ORDER BY data.event_dttm)   AS low_price,
       FIRST_VALUE(data.close_price)
       OVER (PARTITION BY data.series_id, data.close_price_partition_value ORDER BY data.event_dttm) AS close_price
FROM (
         SELECT generated_dates.event_dttm,
                generated_dates.series_id,
                SUM(CASE WHEN data.open_price IS NULL THEN 0 ELSE 1 END) OVER w  AS open_price_partition_value,
                SUM(CASE WHEN data.high_price IS NULL THEN 0 ELSE 1 END) OVER w  AS high_price_partition_value,
                SUM(CASE WHEN data.low_price IS NULL THEN 0 ELSE 1 END) OVER w   AS low_price_partition_value,
                SUM(CASE WHEN data.close_price IS NULL THEN 0 ELSE 1 END) OVER w AS close_price_partition_value,
                data.open_price,
                data.high_price,
                data.low_price,
                data.close_price
         FROM validol_interface.investing_prices_data AS data
                  RIGHT JOIN (
             SELECT *
             FROM (SELECT generate_series('2010-01-01T00:00:00+00:00', NOW(), interval '1 day') AS event_dttm) AS q1
                      CROSS JOIN (SELECT series_id FROM validol_interface.investing_prices_index) AS q2
         ) AS generated_dates
                             ON (
                                     data.series_id = generated_dates.series_id AND
                                     data.event_dttm = generated_dates.event_dttm
                                 )
             WINDOW w AS (PARTITION BY generated_dates.series_id ORDER BY generated_dates.event_dttm)
     ) AS data
         INNER JOIN validol_interface.investing_prices_index AS index
                    ON index.series_id = data.series_id;

CREATE INDEX investing_prices_catalogue_index
    ON validol.investing_prices (currency_cross);

ALTER TABLE validol.investing_prices
    OWNER TO validol_internal;

CREATE MATERIALIZED VIEW validol.moex_derivatives AS
SELECT data.event_dttm,
       index.derivative_name,
       FIRST_VALUE(data.fl) OVER (PARTITION BY data.series_id, data.fl_partition_value ORDER BY data.event_dttm)   AS fl,
       FIRST_VALUE(data.fs)
       OVER (PARTITION BY data.series_id, data.fs_partition_value ORDER BY data.event_dttm)                        AS fs,
       FIRST_VALUE(data.ul)
       OVER (PARTITION BY data.series_id, data.ul_partition_value ORDER BY data.event_dttm)                        AS ul,
       FIRST_VALUE(data.us)
       OVER (PARTITION BY data.series_id, data.us_partition_value ORDER BY data.event_dttm)                        AS us,
       FIRST_VALUE(data.flq)
       OVER (PARTITION BY data.series_id, data.flq_partition_value ORDER BY data.event_dttm)                       AS flq,
       FIRST_VALUE(data.fsq)
       OVER (PARTITION BY data.series_id, data.fsq_partition_value ORDER BY data.event_dttm)                       AS fsq,
       FIRST_VALUE(data.ulq)
       OVER (PARTITION BY data.series_id, data.ulq_partition_value ORDER BY data.event_dttm)                       AS ulq,
       FIRST_VALUE(data.usq)
       OVER (PARTITION BY data.series_id, data.usq_partition_value ORDER BY data.event_dttm)                       AS usq
FROM (
         SELECT generated_dates.event_dttm,
                generated_dates.series_id,
                SUM(CASE WHEN data.fl IS NULL THEN 0 ELSE 1 END) OVER w  AS fl_partition_value,
                SUM(CASE WHEN data.fs IS NULL THEN 0 ELSE 1 END) OVER w  AS fs_partition_value,
                SUM(CASE WHEN data.ul IS NULL THEN 0 ELSE 1 END) OVER w  AS ul_partition_value,
                SUM(CASE WHEN data.us IS NULL THEN 0 ELSE 1 END) OVER w  AS us_partition_value,
                SUM(CASE WHEN data.flq IS NULL THEN 0 ELSE 1 END) OVER w AS flq_partition_value,
                SUM(CASE WHEN data.fsq IS NULL THEN 0 ELSE 1 END) OVER w AS fsq_partition_value,
                SUM(CASE WHEN data.ulq IS NULL THEN 0 ELSE 1 END) OVER w AS ulq_partition_value,
                SUM(CASE WHEN data.usq IS NULL THEN 0 ELSE 1 END) OVER w AS usq_partition_value,
                data.fl,
                data.fs,
                data.ul,
                data.us,
                data.flq,
                data.fsq,
                data.ulq,
                data.usq
         FROM validol_interface.moex_derivatives_data AS data
                  RIGHT JOIN (
             SELECT *
             FROM (SELECT generate_series('2010-01-01T00:00:00+00:00', NOW(), interval '1 day') AS event_dttm) AS q1
                      CROSS JOIN (SELECT series_id FROM validol_interface.moex_derivatives_index) AS q2
         ) AS generated_dates
                             ON (
                                     data.series_id = generated_dates.series_id AND
                                     data.event_dttm = generated_dates.event_dttm
                                 )
             WINDOW w AS (PARTITION BY generated_dates.series_id ORDER BY generated_dates.event_dttm)
     ) AS data
         INNER JOIN validol_interface.moex_derivatives_index AS index
                    ON index.series_id = data.series_id;

CREATE INDEX moex_derivatives_catalogue_index
    ON validol.moex_derivatives (derivative_name);

ALTER TABLE validol.moex_derivatives
    OWNER TO validol_internal;

CREATE MATERIALIZED VIEW validol.cot_futures_only AS
SELECT data.event_dttm,
       index.platform_source,
       index.platform_code,
       index.platform_name,
       index.derivative_name,
       index.report_type,
       FIRST_VALUE(data.oi) OVER (PARTITION BY data.series_id, data.oi_partition_value ORDER BY data.event_dttm)   AS oi,
       FIRST_VALUE(data.ncl)
       OVER (PARTITION BY data.series_id, data.ncl_partition_value ORDER BY data.event_dttm)                       AS ncl,
       FIRST_VALUE(data.ncs)
       OVER (PARTITION BY data.series_id, data.ncs_partition_value ORDER BY data.event_dttm)                       AS ncs,
       FIRST_VALUE(data.cl)
       OVER (PARTITION BY data.series_id, data.cl_partition_value ORDER BY data.event_dttm)                        AS cl,
       FIRST_VALUE(data.cs)
       OVER (PARTITION BY data.series_id, data.cs_partition_value ORDER BY data.event_dttm)                        AS cs,
       FIRST_VALUE(data.nrl)
       OVER (PARTITION BY data.series_id, data.nrl_partition_value ORDER BY data.event_dttm)                       AS nrl,
       FIRST_VALUE(data.nrs)
       OVER (PARTITION BY data.series_id, data.nrs_partition_value ORDER BY data.event_dttm)                       AS nrs,
       FIRST_VALUE(data.x_4l_percent)
       OVER (PARTITION BY data.series_id, data.x_4l_percent_partition_value ORDER BY data.event_dttm)              AS x_4l_percent,
       FIRST_VALUE(data.x_4s_percent)
       OVER (PARTITION BY data.series_id, data.x_4s_percent_partition_value ORDER BY data.event_dttm)              AS x_4s_percent,
       FIRST_VALUE(data.x_8l_percent)
       OVER (PARTITION BY data.series_id, data.x_8l_percent_partition_value ORDER BY data.event_dttm)              AS x_8l_percent,
       FIRST_VALUE(data.x_8s_percent)
       OVER (PARTITION BY data.series_id, data.x_8s_percent_partition_value ORDER BY data.event_dttm)              AS x_8s_percent
FROM (
         SELECT generated_dates.event_dttm,
                generated_dates.series_id,
                SUM(CASE WHEN data.oi IS NULL THEN 0 ELSE 1 END) OVER w           AS oi_partition_value,
                SUM(CASE WHEN data.ncl IS NULL THEN 0 ELSE 1 END) OVER w          AS ncl_partition_value,
                SUM(CASE WHEN data.ncs IS NULL THEN 0 ELSE 1 END) OVER w          AS ncs_partition_value,
                SUM(CASE WHEN data.cl IS NULL THEN 0 ELSE 1 END) OVER w           AS cl_partition_value,
                SUM(CASE WHEN data.cs IS NULL THEN 0 ELSE 1 END) OVER w           AS cs_partition_value,
                SUM(CASE WHEN data.nrl IS NULL THEN 0 ELSE 1 END) OVER w          AS nrl_partition_value,
                SUM(CASE WHEN data.nrs IS NULL THEN 0 ELSE 1 END) OVER w          AS nrs_partition_value,
                SUM(CASE WHEN data.x_4l_percent IS NULL THEN 0 ELSE 1 END) OVER w AS x_4l_percent_partition_value,
                SUM(CASE WHEN data.x_4s_percent IS NULL THEN 0 ELSE 1 END) OVER w AS x_4s_percent_partition_value,
                SUM(CASE WHEN data.x_8l_percent IS NULL THEN 0 ELSE 1 END) OVER w AS x_8l_percent_partition_value,
                SUM(CASE WHEN data.x_8s_percent IS NULL THEN 0 ELSE 1 END) OVER w AS x_8s_percent_partition_value,
                data.oi,
                data.ncl,
                data.ncs,
                data.cl,
                data.cs,
                data.nrl,
                data.nrs,
                data.x_4l_percent,
                data.x_4s_percent,
                data.x_8l_percent,
                data.x_8s_percent
         FROM validol_interface.cot_futures_only_data AS data
                  RIGHT JOIN (
             SELECT *
             FROM (SELECT generate_series('2010-01-01T00:00:00+00:00', NOW(), interval '1 day') AS event_dttm) AS q1
                      CROSS JOIN (SELECT series_id FROM validol_interface.cot_futures_only_index) AS q2
         ) AS generated_dates
                             ON (
                                     data.series_id = generated_dates.series_id AND
                                     data.event_dttm = generated_dates.event_dttm
                                 )
             WINDOW w AS (PARTITION BY generated_dates.series_id ORDER BY generated_dates.event_dttm)
     ) AS data
         INNER JOIN validol_interface.cot_futures_only_index AS index
                    ON index.series_id = data.series_id;

CREATE INDEX cot_futures_only_catalogue_index
    ON validol.cot_futures_only (platform_source, platform_code, derivative_name, report_type);

ALTER TABLE validol.cot_futures_only
    OWNER TO validol_internal;

CREATE MATERIALIZED VIEW validol.cot_disaggregated AS
SELECT data.event_dttm,
       index.platform_source,
       index.platform_code,
       index.platform_name,
       index.derivative_name,
       index.report_type,
       FIRST_VALUE(data.oi) OVER (PARTITION BY data.series_id, data.oi_partition_value ORDER BY data.event_dttm)     AS oi,
       FIRST_VALUE(data.nrl)
       OVER (PARTITION BY data.series_id, data.nrl_partition_value ORDER BY data.event_dttm)                         AS nrl,
       FIRST_VALUE(data.nrs)
       OVER (PARTITION BY data.series_id, data.nrs_partition_value ORDER BY data.event_dttm)                         AS nrs,
       FIRST_VALUE(data.pmpl)
       OVER (PARTITION BY data.series_id, data.pmpl_partition_value ORDER BY data.event_dttm)                        AS pmpl,
       FIRST_VALUE(data.pmps)
       OVER (PARTITION BY data.series_id, data.pmps_partition_value ORDER BY data.event_dttm)                        AS pmps,
       FIRST_VALUE(data.sdpl)
       OVER (PARTITION BY data.series_id, data.sdpl_partition_value ORDER BY data.event_dttm)                        AS sdpl,
       FIRST_VALUE(data.sdps)
       OVER (PARTITION BY data.series_id, data.sdps_partition_value ORDER BY data.event_dttm)                        AS sdps,
       FIRST_VALUE(data.mmpl)
       OVER (PARTITION BY data.series_id, data.mmpl_partition_value ORDER BY data.event_dttm)                        AS mmpl,
       FIRST_VALUE(data.mmps)
       OVER (PARTITION BY data.series_id, data.mmps_partition_value ORDER BY data.event_dttm)                        AS mmps,
       FIRST_VALUE(data.orpl)
       OVER (PARTITION BY data.series_id, data.orpl_partition_value ORDER BY data.event_dttm)                        AS orpl,
       FIRST_VALUE(data.orps)
       OVER (PARTITION BY data.series_id, data.orps_partition_value ORDER BY data.event_dttm)                        AS orps,
       FIRST_VALUE(data.x_4gl_percent)
       OVER (PARTITION BY data.series_id, data.x_4gl_percent_partition_value ORDER BY data.event_dttm)               AS x_4gl_percent,
       FIRST_VALUE(data.x_4gs_percent)
       OVER (PARTITION BY data.series_id, data.x_4gs_percent_partition_value ORDER BY data.event_dttm)               AS x_4gs_percent,
       FIRST_VALUE(data.x_8gl_percent)
       OVER (PARTITION BY data.series_id, data.x_8gl_percent_partition_value ORDER BY data.event_dttm)               AS x_8gl_percent,
       FIRST_VALUE(data.x_8gs_percent)
       OVER (PARTITION BY data.series_id, data.x_8gs_percent_partition_value ORDER BY data.event_dttm)               AS x_8gs_percent,
       FIRST_VALUE(data.x_4l_percent)
       OVER (PARTITION BY data.series_id, data.x_4l_percent_partition_value ORDER BY data.event_dttm)                AS x_4l_percent,
       FIRST_VALUE(data.x_4s_percent)
       OVER (PARTITION BY data.series_id, data.x_4s_percent_partition_value ORDER BY data.event_dttm)                AS x_4s_percent,
       FIRST_VALUE(data.x_8l_percent)
       OVER (PARTITION BY data.series_id, data.x_8l_percent_partition_value ORDER BY data.event_dttm)                AS x_8l_percent,
       FIRST_VALUE(data.x_8s_percent)
       OVER (PARTITION BY data.series_id, data.x_8s_percent_partition_value ORDER BY data.event_dttm)                AS x_8s_percent,
       FIRST_VALUE(data.sdp_spr)
       OVER (PARTITION BY data.series_id, data.sdp_spr_partition_value ORDER BY data.event_dttm)                     AS sdp_spr,
       FIRST_VALUE(data.mmp_spr)
       OVER (PARTITION BY data.series_id, data.mmp_spr_partition_value ORDER BY data.event_dttm)                     AS mmp_spr,
       FIRST_VALUE(data.orp_spr)
       OVER (PARTITION BY data.series_id, data.orp_spr_partition_value ORDER BY data.event_dttm)                     AS orp_spr,
       FIRST_VALUE(data.cl)
       OVER (PARTITION BY data.series_id, data.cl_partition_value ORDER BY data.event_dttm)                          AS cl,
       FIRST_VALUE(data.cs)
       OVER (PARTITION BY data.series_id, data.cs_partition_value ORDER BY data.event_dttm)                          AS cs,
       FIRST_VALUE(data.ncl)
       OVER (PARTITION BY data.series_id, data.ncl_partition_value ORDER BY data.event_dttm)                         AS ncl,
       FIRST_VALUE(data.ncs)
       OVER (PARTITION BY data.series_id, data.ncs_partition_value ORDER BY data.event_dttm)                         AS ncs
FROM (
         SELECT generated_dates.event_dttm,
                generated_dates.series_id,
                SUM(CASE WHEN data.oi IS NULL THEN 0 ELSE 1 END) OVER w            AS oi_partition_value,
                SUM(CASE WHEN data.nrl IS NULL THEN 0 ELSE 1 END) OVER w           AS nrl_partition_value,
                SUM(CASE WHEN data.nrs IS NULL THEN 0 ELSE 1 END) OVER w           AS nrs_partition_value,
                SUM(CASE WHEN data.pmpl IS NULL THEN 0 ELSE 1 END) OVER w          AS pmpl_partition_value,
                SUM(CASE WHEN data.pmps IS NULL THEN 0 ELSE 1 END) OVER w          AS pmps_partition_value,
                SUM(CASE WHEN data.sdpl IS NULL THEN 0 ELSE 1 END) OVER w          AS sdpl_partition_value,
                SUM(CASE WHEN data.sdps IS NULL THEN 0 ELSE 1 END) OVER w          AS sdps_partition_value,
                SUM(CASE WHEN data.mmpl IS NULL THEN 0 ELSE 1 END) OVER w          AS mmpl_partition_value,
                SUM(CASE WHEN data.mmps IS NULL THEN 0 ELSE 1 END) OVER w          AS mmps_partition_value,
                SUM(CASE WHEN data.orpl IS NULL THEN 0 ELSE 1 END) OVER w          AS orpl_partition_value,
                SUM(CASE WHEN data.orps IS NULL THEN 0 ELSE 1 END) OVER w          AS orps_partition_value,
                SUM(CASE WHEN data.x_4gl_percent IS NULL THEN 0 ELSE 1 END) OVER w AS x_4gl_percent_partition_value,
                SUM(CASE WHEN data.x_4gs_percent IS NULL THEN 0 ELSE 1 END) OVER w AS x_4gs_percent_partition_value,
                SUM(CASE WHEN data.x_8gl_percent IS NULL THEN 0 ELSE 1 END) OVER w AS x_8gl_percent_partition_value,
                SUM(CASE WHEN data.x_8gs_percent IS NULL THEN 0 ELSE 1 END) OVER w AS x_8gs_percent_partition_value,
                SUM(CASE WHEN data.x_4l_percent IS NULL THEN 0 ELSE 1 END) OVER w  AS x_4l_percent_partition_value,
                SUM(CASE WHEN data.x_4s_percent IS NULL THEN 0 ELSE 1 END) OVER w  AS x_4s_percent_partition_value,
                SUM(CASE WHEN data.x_8l_percent IS NULL THEN 0 ELSE 1 END) OVER w  AS x_8l_percent_partition_value,
                SUM(CASE WHEN data.x_8s_percent IS NULL THEN 0 ELSE 1 END) OVER w  AS x_8s_percent_partition_value,
                SUM(CASE WHEN data.sdp_spr IS NULL THEN 0 ELSE 1 END) OVER w       AS sdp_spr_partition_value,
                SUM(CASE WHEN data.mmp_spr IS NULL THEN 0 ELSE 1 END) OVER w       AS mmp_spr_partition_value,
                SUM(CASE WHEN data.orp_spr IS NULL THEN 0 ELSE 1 END) OVER w       AS orp_spr_partition_value,
                SUM(CASE WHEN data.cl IS NULL THEN 0 ELSE 1 END) OVER w            AS cl_partition_value,
                SUM(CASE WHEN data.cs IS NULL THEN 0 ELSE 1 END) OVER w            AS cs_partition_value,
                SUM(CASE WHEN data.ncl IS NULL THEN 0 ELSE 1 END) OVER w           AS ncl_partition_value,
                SUM(CASE WHEN data.ncs IS NULL THEN 0 ELSE 1 END) OVER w           AS ncs_partition_value,
                data.oi,
                data.nrl,
                data.nrs,
                data.pmpl,
                data.pmps,
                data.sdpl,
                data.sdps,
                data.mmpl,
                data.mmps,
                data.orpl,
                data.orps,
                data.x_4gl_percent,
                data.x_4gs_percent,
                data.x_8gl_percent,
                data.x_8gs_percent,
                data.x_4l_percent,
                data.x_4s_percent,
                data.x_8l_percent,
                data.x_8s_percent,
                data.sdp_spr,
                data.mmp_spr,
                data.orp_spr,
                data.cl,
                data.cs,
                data.ncl,
                data.ncs
         FROM validol_interface.cot_disaggregated_data AS data
                  RIGHT JOIN (
             SELECT *
             FROM (SELECT generate_series('2010-01-01T00:00:00+00:00', NOW(), interval '1 day') AS event_dttm) AS q1
                      CROSS JOIN (SELECT series_id FROM validol_interface.cot_disaggregated_index) AS q2
         ) AS generated_dates
                             ON (
                                     data.series_id = generated_dates.series_id AND
                                     data.event_dttm = generated_dates.event_dttm
                                 )
             WINDOW w AS (PARTITION BY generated_dates.series_id ORDER BY generated_dates.event_dttm)
     ) AS data
         INNER JOIN validol_interface.cot_disaggregated_index AS index
                    ON index.series_id = data.series_id;

CREATE INDEX cot_disaggregated_catalogue_index
    ON validol.cot_disaggregated (platform_source, platform_code, derivative_name, report_type);

ALTER TABLE validol.cot_disaggregated
    OWNER TO validol_internal;

CREATE MATERIALIZED VIEW validol.cot_financial_futures AS
SELECT data.event_dttm,
       index.platform_source,
       index.platform_code,
       index.platform_name,
       index.derivative_name,
       index.report_type,
       FIRST_VALUE(data.oi) OVER (PARTITION BY data.series_id, data.oi_partition_value ORDER BY data.event_dttm)     AS oi,
       FIRST_VALUE(data.dipl)
       OVER (PARTITION BY data.series_id, data.dipl_partition_value ORDER BY data.event_dttm)                        AS dipl,
       FIRST_VALUE(data.dips)
       OVER (PARTITION BY data.series_id, data.dips_partition_value ORDER BY data.event_dttm)                        AS dips,
       FIRST_VALUE(data.dip_spr)
       OVER (PARTITION BY data.series_id, data.dip_spr_partition_value ORDER BY data.event_dttm)                     AS dip_spr,
       FIRST_VALUE(data.ampl)
       OVER (PARTITION BY data.series_id, data.ampl_partition_value ORDER BY data.event_dttm)                        AS ampl,
       FIRST_VALUE(data.amps)
       OVER (PARTITION BY data.series_id, data.amps_partition_value ORDER BY data.event_dttm)                        AS amps,
       FIRST_VALUE(data.amp_spr)
       OVER (PARTITION BY data.series_id, data.amp_spr_partition_value ORDER BY data.event_dttm)                     AS amp_spr,
       FIRST_VALUE(data.lmpl)
       OVER (PARTITION BY data.series_id, data.lmpl_partition_value ORDER BY data.event_dttm)                        AS lmpl,
       FIRST_VALUE(data.lmps)
       OVER (PARTITION BY data.series_id, data.lmps_partition_value ORDER BY data.event_dttm)                        AS lmps,
       FIRST_VALUE(data.lmp_spr)
       OVER (PARTITION BY data.series_id, data.lmp_spr_partition_value ORDER BY data.event_dttm)                     AS lmp_spr,
       FIRST_VALUE(data.orpl)
       OVER (PARTITION BY data.series_id, data.orpl_partition_value ORDER BY data.event_dttm)                        AS orpl,
       FIRST_VALUE(data.orps)
       OVER (PARTITION BY data.series_id, data.orps_partition_value ORDER BY data.event_dttm)                        AS orps,
       FIRST_VALUE(data.orp_spr)
       OVER (PARTITION BY data.series_id, data.orp_spr_partition_value ORDER BY data.event_dttm)                     AS orp_spr,
       FIRST_VALUE(data.nrl)
       OVER (PARTITION BY data.series_id, data.nrl_partition_value ORDER BY data.event_dttm)                         AS nrl,
       FIRST_VALUE(data.nrs)
       OVER (PARTITION BY data.series_id, data.nrs_partition_value ORDER BY data.event_dttm)                         AS nrs
FROM (
         SELECT generated_dates.event_dttm,
                generated_dates.series_id,
                SUM(CASE WHEN data.oi IS NULL THEN 0 ELSE 1 END) OVER w      AS oi_partition_value,
                SUM(CASE WHEN data.dipl IS NULL THEN 0 ELSE 1 END) OVER w    AS dipl_partition_value,
                SUM(CASE WHEN data.dips IS NULL THEN 0 ELSE 1 END) OVER w    AS dips_partition_value,
                SUM(CASE WHEN data.dip_spr IS NULL THEN 0 ELSE 1 END) OVER w AS dip_spr_partition_value,
                SUM(CASE WHEN data.ampl IS NULL THEN 0 ELSE 1 END) OVER w    AS ampl_partition_value,
                SUM(CASE WHEN data.amps IS NULL THEN 0 ELSE 1 END) OVER w    AS amps_partition_value,
                SUM(CASE WHEN data.amp_spr IS NULL THEN 0 ELSE 1 END) OVER w AS amp_spr_partition_value,
                SUM(CASE WHEN data.lmpl IS NULL THEN 0 ELSE 1 END) OVER w    AS lmpl_partition_value,
                SUM(CASE WHEN data.lmps IS NULL THEN 0 ELSE 1 END) OVER w    AS lmps_partition_value,
                SUM(CASE WHEN data.lmp_spr IS NULL THEN 0 ELSE 1 END) OVER w AS lmp_spr_partition_value,
                SUM(CASE WHEN data.orpl IS NULL THEN 0 ELSE 1 END) OVER w    AS orpl_partition_value,
                SUM(CASE WHEN data.orps IS NULL THEN 0 ELSE 1 END) OVER w    AS orps_partition_value,
                SUM(CASE WHEN data.orp_spr IS NULL THEN 0 ELSE 1 END) OVER w AS orp_spr_partition_value,
                SUM(CASE WHEN data.nrl IS NULL THEN 0 ELSE 1 END) OVER w     AS nrl_partition_value,
                SUM(CASE WHEN data.nrs IS NULL THEN 0 ELSE 1 END) OVER w     AS nrs_partition_value,
                data.oi,
                data.dipl,
                data.dips,
                data.dip_spr,
                data.ampl,
                data.amps,
                data.amp_spr,
                data.lmpl,
                data.lmps,
                data.lmp_spr,
                data.orpl,
                data.orps,
                data.orp_spr,
                data.nrl,
                data.nrs
         FROM validol_interface.cot_financial_futures_data AS data
                  RIGHT JOIN (
             SELECT *
             FROM (SELECT generate_series('2010-01-01T00:00:00+00:00', NOW(), interval '1 day') AS event_dttm) AS q1
                      CROSS JOIN (SELECT series_id FROM validol_interface.cot_financial_futures_index) AS q2
         ) AS generated_dates
                             ON (
                                     data.series_id = generated_dates.series_id AND
                                     data.event_dttm = generated_dates.event_dttm
                                 )
             WINDOW w AS (PARTITION BY generated_dates.series_id ORDER BY generated_dates.event_dttm)
     ) AS data
         INNER JOIN validol_interface.cot_financial_futures_index AS index
                    ON index.series_id = data.series_id;

CREATE INDEX cot_financial_futures_catalogue_index
    ON validol.cot_financial_futures (platform_source, platform_code, derivative_name, report_type);

ALTER TABLE validol.cot_financial_futures
    OWNER TO validol_internal;