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
    id                       BIGSERIAL PRIMARY KEY,
    investing_prices_info_id BIGINT      NOT NULL REFERENCES validol_internal.investing_prices_info (id),
    event_dttm               TIMESTAMPTZ NOT NULL,
    open_price               DECIMAL     NOT NULL,
    high_price               DECIMAL     NOT NULL,
    low_price                DECIMAL     NOT NULL,
    close_price              DECIMAL     NOT NULL,

    UNIQUE (investing_prices_info_id, event_dttm)
);

CREATE TABLE validol_internal.fredgraph
(
    id         BIGSERIAL PRIMARY KEY,
    event_dttm TIMESTAMPTZ NOT NULL,
    sensor     VARCHAR     NOT NULL,
    value      DECIMAL     NOT NULL,

    UNIQUE (event_dttm, sensor)
);

CREATE TABLE validol_internal.moex_derivatives_info
(
    id   BIGSERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,

    UNIQUE (name)
);

CREATE TABLE validol_internal.moex_derivatives_data
(
    id                       BIGSERIAL PRIMARY KEY,
    moex_derivatives_info_id BIGINT      NOT NULL REFERENCES validol_internal.moex_derivatives_info (id),
    event_dttm               TIMESTAMPTZ NOT NULL,
    fl                       DECIMAL,
    fs                       DECIMAL,
    ul                       DECIMAL,
    us                       DECIMAL,
    flq                      DECIMAL,
    fsq                      DECIMAL,
    ulq                      DECIMAL,
    usq                      DECIMAL,

    UNIQUE (moex_derivatives_info_id, event_dttm)
);

CREATE TABLE validol_internal.cot_derivatives_platform
(
    id     BIGSERIAL PRIMARY KEY,
    source VARCHAR NOT NULL,
    code   VARCHAR NOT NULL,
    name   VARCHAR NOT NULL,

    UNIQUE (source, code)
);

CREATE TABLE validol_internal.cot_derivatives_info
(
    id                          BIGSERIAL PRIMARY KEY,
    cot_derivatives_platform_id BIGINT  NOT NULL REFERENCES validol_internal.cot_derivatives_platform (id),
    name                        VARCHAR NOT NULL,

    UNIQUE (cot_derivatives_platform_id, name)
);

CREATE TYPE report_type AS ENUM ('futures_only', 'combined');

CREATE TABLE validol_internal.cot_futures_only_data
(
    id                      BIGSERIAL PRIMARY KEY,
    cot_derivatives_info_id BIGINT      NOT NULL REFERENCES validol_internal.cot_derivatives_info (id),
    report_type             report_type NOT NULL,
    event_dttm              TIMESTAMPTZ NOT NULL,
    oi                      DECIMAL,
    ncl                     DECIMAL,
    ncs                     DECIMAL,
    cl                      DECIMAL,
    cs                      DECIMAL,
    nrl                     DECIMAL,
    nrs                     DECIMAL,
    "4l%"                   DECIMAL,
    "4s%"                   DECIMAL,
    "8l%"                   DECIMAL,
    "8s%"                   DECIMAL,

    UNIQUE (cot_derivatives_info_id, report_type, event_dttm)
);

CREATE TABLE validol_internal.cot_disaggregated_data
(
    id                      BIGSERIAL PRIMARY KEY,
    cot_derivatives_info_id BIGINT      NOT NULL REFERENCES validol_internal.cot_derivatives_info (id),
    report_type             report_type NOT NULL,
    event_dttm              TIMESTAMPTZ NOT NULL,
    oi                      DECIMAL,
    nrl                     DECIMAL,
    nrs                     DECIMAL,
    pmpl                    DECIMAL,
    pmps                    DECIMAL,
    sdpl                    DECIMAL,
    sdps                    DECIMAL,
    mmpl                    DECIMAL,
    mmps                    DECIMAL,
    orpl                    DECIMAL,
    orps                    DECIMAL,
    "4gl%"                  DECIMAL,
    "4gs%"                  DECIMAL,
    "8gl%"                  DECIMAL,
    "8gs%"                  DECIMAL,
    "4l%"                   DECIMAL,
    "4s%"                   DECIMAL,
    "8l%"                   DECIMAL,
    "8s%"                   DECIMAL,
    sdp_spr                 DECIMAL,
    mmp_spr                 DECIMAL,
    orp_spr                 DECIMAL,

    UNIQUE (cot_derivatives_info_id, report_type, event_dttm)
);

CREATE TABLE validol_internal.cot_financial_futures_data
(
    id                      BIGSERIAL PRIMARY KEY,
    cot_derivatives_info_id BIGINT      NOT NULL REFERENCES validol_internal.cot_derivatives_info (id),
    report_type             report_type NOT NULL,
    event_dttm              TIMESTAMPTZ NOT NULL,
    oi                      DECIMAL,
    dipl                    DECIMAL,
    dips                    DECIMAL,
    dip_spr                 DECIMAL,
    ampl                    DECIMAL,
    amps                    DECIMAL,
    amp_spr                 DECIMAL,
    lmpl                    DECIMAL,
    lmps                    DECIMAL,
    lmp_spr                 DECIMAL,
    orpl                    DECIMAL,
    orps                    DECIMAL,
    orp_spr                 DECIMAL,
    nrl                     DECIMAL,
    nrs                     DECIMAL,

    UNIQUE (cot_derivatives_info_id, report_type, event_dttm)
);


-- views for superset usage

CREATE SCHEMA validol;
GRANT USAGE ON SCHEMA validol TO validol_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA validol GRANT ALL PRIVILEGES ON TABLES TO validol_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA validol TO validol_reader;

CREATE VIEW validol.investing_prices AS
SELECT data.event_dttm,
       data.open_price,
       data.high_price,
       data.low_price,
       data.close_price,
       info.currency_cross
FROM validol_internal.investing_prices_data AS data
         INNER JOIN validol_internal.investing_prices_info AS info
                    ON data.investing_prices_info_id = info.id;

CREATE VIEW validol.fredgraph AS
SELECT event_dttm,
       sensor,
       value
FROM validol_internal.fredgraph;

CREATE VIEW validol.moex_derivatives AS
SELECT data.event_dttm,
       data.fl,
       data.fs,
       data.ul,
       data.us,
       data.flq,
       data.fsq,
       data.ulq,
       data.usq,
       info.name
FROM validol_internal.moex_derivatives_data AS data
         INNER JOIN validol_internal.moex_derivatives_info AS info
                    ON data.moex_derivatives_info_id = info.id;

CREATE VIEW validol.cot_futures_only AS
SELECT platform.source AS platform_source,
       platform.code   AS platform_code,
       platform.name   AS platform_name,
       info.name       AS derivative_name,
       data.report_type,
       data.event_dttm,
       data.oi,
       data.ncl,
       data.ncs,
       data.cl,
       data.cs,
       data.nrl,
       data.nrs,
       data."4l%",
       data."4s%",
       data."8l%",
       data."8s%"
FROM validol_internal.cot_futures_only_data AS data
         INNER JOIN validol_internal.cot_derivatives_info AS info
                    ON data.cot_derivatives_info_id = info.id
         INNER JOIN validol_internal.cot_derivatives_platform AS platform
                    ON info.cot_derivatives_platform_id = platform.id;

CREATE VIEW validol.cot_disaggregated AS
SELECT platform.source       AS platform_source,
       platform.code         AS platform_code,
       platform.name         AS platform_name,
       info.name             AS derivative_name,
       data.report_type,
       data.event_dttm,
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
       data."4gl%",
       data."4gs%",
       data."8gl%",
       data."8gs%",
       data."4l%",
       data."4s%",
       data."8l%",
       data."8s%",
       data.sdp_spr,
       data.mmp_spr,
       data.orp_spr,
       data.pmpl + data.sdpl AS cl,
       data.pmps + data.sdps AS cs,
       data.mmpl + data.orpl AS ncl,
       data.mmps + data.orps AS ncs
FROM validol_internal.cot_disaggregated_data AS data
         INNER JOIN validol_internal.cot_derivatives_info AS info
                    ON data.cot_derivatives_info_id = info.id
         INNER JOIN validol_internal.cot_derivatives_platform AS platform
                    ON info.cot_derivatives_platform_id = platform.id;

CREATE VIEW validol.cot_financial_futures AS
SELECT platform.source AS platform_source,
       platform.code   AS platform_code,
       platform.name   AS platform_name,
       info.name       AS derivative_name,
       data.report_type,
       data.event_dttm,
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
FROM validol_internal.cot_financial_futures_data AS data
         INNER JOIN validol_internal.cot_derivatives_info AS info
                    ON data.cot_derivatives_info_id = info.id
         INNER JOIN validol_internal.cot_derivatives_platform AS platform
                    ON info.cot_derivatives_platform_id = platform.id;
