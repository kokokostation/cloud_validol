CREATE SCHEMA validol;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA validol TO validol;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA validol TO validol;

CREATE TABLE validol.investing_prices_info
(
    id             BIGSERIAL PRIMARY KEY,
    currency_cross VARCHAR NOT NULL
);

CREATE TABLE validol.investing_prices_data
(
    id                       BIGSERIAL PRIMARY KEY,
    investing_prices_info_id BIGINT      NOT NULL REFERENCES validol.investing_prices_info (id),
    event_dttm               TIMESTAMPTZ NOT NULL,
    open_price               DECIMAL     NOT NULL,
    high_price               DECIMAL     NOT NULL,
    low_price                DECIMAL     NOT NULL,
    close_price              DECIMAL     NOT NULL
);
