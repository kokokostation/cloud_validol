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
    visible        BOOLEAN NOT NULL DEFAULT TRUE,

    UNIQUE (currency_cross)
);

CREATE TABLE validol_internal.investing_prices_data
(
    id          BIGSERIAL PRIMARY KEY,
    series_id   BIGINT      NOT NULL REFERENCES validol_internal.investing_prices_info (id) ON DELETE CASCADE,
    event_dttm  TIMESTAMPTZ NOT NULL,
    open_price  DECIMAL     NOT NULL,
    high_price  DECIMAL     NOT NULL,
    low_price   DECIMAL     NOT NULL,
    close_price DECIMAL     NOT NULL,

    UNIQUE (series_id, event_dttm)
);

CREATE TABLE validol_internal.fredgraph_info
(
    id      BIGSERIAL PRIMARY KEY,
    visible BOOLEAN NOT NULL DEFAULT TRUE
);

INSERT INTO validol_internal.fredgraph_info (id)
VALUES (1);

CREATE TABLE validol_internal.fredgraph_data
(
    id         BIGSERIAL PRIMARY KEY,
    series_id  BIGINT      NOT NULL DEFAULT 1 REFERENCES validol_internal.fredgraph_info (id) ON DELETE CASCADE,
    event_dttm TIMESTAMPTZ NOT NULL,
    mbase      DECIMAL,
    tdebt      DECIMAL,

    UNIQUE (series_id, event_dttm)
);

CREATE TABLE validol_internal.moex_derivatives_info
(
    id      BIGSERIAL PRIMARY KEY,
    name    VARCHAR NOT NULL,
    visible BOOLEAN NOT NULL DEFAULT FALSE,

    UNIQUE (name)
);

CREATE TABLE validol_internal.moex_derivatives_data
(
    id         BIGSERIAL PRIMARY KEY,
    series_id  BIGINT      NOT NULL REFERENCES validol_internal.moex_derivatives_info (id) ON DELETE CASCADE,
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
    cot_derivatives_platform_id BIGINT      NOT NULL REFERENCES validol_internal.cot_derivatives_platform (id) ON DELETE CASCADE,
    name                        VARCHAR     NOT NULL,
    report_type                 report_type NOT NULL,
    visible                     BOOLEAN     NOT NULL DEFAULT FALSE,

    UNIQUE (cot_derivatives_platform_id, name, report_type)
);

CREATE TABLE validol_internal.cot_futures_only_data
(
    id           BIGSERIAL PRIMARY KEY,
    series_id    BIGINT      NOT NULL REFERENCES validol_internal.cot_derivatives_info (id) ON DELETE CASCADE,
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
    series_id     BIGINT      NOT NULL REFERENCES validol_internal.cot_derivatives_info (id) ON DELETE CASCADE,
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
    series_id  BIGINT      NOT NULL REFERENCES validol_internal.cot_derivatives_info (id) ON DELETE CASCADE,
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
       currency_cross,
       visible
FROM validol_internal.investing_prices_info;

CREATE VIEW validol_interface.investing_prices_data AS
SELECT *
FROM validol_internal.investing_prices_data;

CREATE VIEW validol_interface.fredgraph_index AS
SELECT id AS series_id,
       visible
FROM validol_internal.fredgraph_info;

CREATE VIEW validol_interface.fredgraph_data AS
SELECT *
FROM validol_internal.fredgraph_data;

CREATE VIEW validol_interface.moex_derivatives_index AS
SELECT id   AS series_id,
       name AS derivative_name,
       visible
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
       info.report_type,
       info.visible
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
SELECT
    data.event_dttm,
    data.mbase,
    data.tdebt
FROM validol_interface.fredgraph_data AS data
INNER JOIN validol_interface.fredgraph_index AS index
    ON index.series_id = data.series_id
WHERE index.visible;


ALTER TABLE validol.fredgraph OWNER TO validol_internal;

CREATE MATERIALIZED VIEW validol.investing_prices AS
SELECT
    data.event_dttm,
    index.currency_cross,
    data.open_price,
    data.high_price,
    data.low_price,
    data.close_price
FROM validol_interface.investing_prices_data AS data
INNER JOIN validol_interface.investing_prices_index AS index
    ON index.series_id = data.series_id
WHERE index.visible;

CREATE INDEX investing_prices_catalogue_index
    ON validol.investing_prices (currency_cross);

ALTER TABLE validol.investing_prices OWNER TO validol_internal;

CREATE MATERIALIZED VIEW validol.moex_derivatives AS
SELECT
    data.event_dttm,
    index.derivative_name,
    data.fl,
    data.fs,
    data.ul,
    data.us,
    data.flq,
    data.fsq,
    data.ulq,
    data.usq
FROM validol_interface.moex_derivatives_data AS data
INNER JOIN validol_interface.moex_derivatives_index AS index
    ON index.series_id = data.series_id
WHERE index.visible;

CREATE INDEX moex_derivatives_catalogue_index
    ON validol.moex_derivatives (derivative_name);

ALTER TABLE validol.moex_derivatives OWNER TO validol_internal;

CREATE MATERIALIZED VIEW validol.cot_futures_only AS
SELECT
    data.event_dttm,
    index.platform_source,
    index.platform_code,
    index.platform_name,
    index.derivative_name,
    index.report_type,
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
INNER JOIN validol_interface.cot_futures_only_index AS index
    ON index.series_id = data.series_id
WHERE index.visible;

CREATE INDEX cot_futures_only_catalogue_index
    ON validol.cot_futures_only (platform_source, platform_code, derivative_name, report_type);

ALTER TABLE validol.cot_futures_only OWNER TO validol_internal;

CREATE MATERIALIZED VIEW validol.cot_disaggregated AS
SELECT
    data.event_dttm,
    index.platform_source,
    index.platform_code,
    index.platform_name,
    index.derivative_name,
    index.report_type,
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
INNER JOIN validol_interface.cot_disaggregated_index AS index
    ON index.series_id = data.series_id
WHERE index.visible;

CREATE INDEX cot_disaggregated_catalogue_index
    ON validol.cot_disaggregated (platform_source, platform_code, derivative_name, report_type);

ALTER TABLE validol.cot_disaggregated OWNER TO validol_internal;

CREATE MATERIALIZED VIEW validol.cot_financial_futures AS
SELECT
    data.event_dttm,
    index.platform_source,
    index.platform_code,
    index.platform_name,
    index.derivative_name,
    index.report_type,
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
INNER JOIN validol_interface.cot_financial_futures_index AS index
    ON index.series_id = data.series_id
WHERE index.visible;

CREATE INDEX cot_financial_futures_catalogue_index
    ON validol.cot_financial_futures (platform_source, platform_code, derivative_name, report_type);

ALTER TABLE validol.cot_financial_futures OWNER TO validol_internal;
