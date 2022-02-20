CREATE TABLE validol_internal.atom
(
    id         BIGSERIAL PRIMARY KEY,
    name       VARCHAR NOT NULL,
    expression VARCHAR NOT NULL,

    UNIQUE (name)
);
