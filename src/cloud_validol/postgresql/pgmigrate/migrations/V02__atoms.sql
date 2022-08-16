BEGIN;

CREATE TABLE validol_internal.atom
(
    id         BIGSERIAL PRIMARY KEY,
    name       VARCHAR NOT NULL,
    expression VARCHAR NOT NULL,
    deleted_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX atom_name
ON validol_internal.atom (name)
WHERE (deleted_at IS NULL);

COMMIT;
