CREATE ROLE palimpsest_runtime
    LOGIN
    PASSWORD 'palimpsest-runtime-local-only'
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    NOINHERIT
    NOBYPASSRLS;

CREATE DATABASE palimpsest OWNER palimpsest_runtime;

REVOKE ALL ON DATABASE palimpsest FROM PUBLIC;
GRANT CONNECT, TEMPORARY ON DATABASE palimpsest TO palimpsest_runtime;

\connect palimpsest
CREATE EXTENSION vector WITH SCHEMA public;
