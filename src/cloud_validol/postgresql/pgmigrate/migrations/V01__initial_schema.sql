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
    currency_cross VARCHAR NOT NULL
);

CREATE TABLE validol_internal.investing_prices_data
(
    id                       BIGSERIAL PRIMARY KEY,
    investing_prices_info_id BIGINT      NOT NULL REFERENCES validol_internal.investing_prices_info (id),
    event_dttm               TIMESTAMPTZ NOT NULL,
    open_price               DECIMAL     NOT NULL,
    high_price               DECIMAL     NOT NULL,
    low_price                DECIMAL     NOT NULL,
    close_price              DECIMAL     NOT NULL
);

CREATE TABLE validol_internal.fredgraph
(
    id         BIGSERIAL PRIMARY KEY,
    event_dttm TIMESTAMPTZ NOT NULL,
    sensor     VARCHAR     NOT NULL,
    value      DECIMAL     NOT NULL
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
    usq                      DECIMAL
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
