#!/usr/bin/env bash
# Creates the application roles (D-006). The postgres entrypoint runs this once, on an
# empty data dir, as POSTGRES_USER; Testcontainers copies the same file in. It may be
# executed or sourced, so no `set -u` and no `exit`: psql's status is the result.
# Passwords travel as psql variables and are quoted by psql (:'var'), never by the shell.
# Grants on tables live in the Flyway migration that creates each table.
psql -v ON_ERROR_STOP=1 --no-psqlrc \
	--username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
	-v owner_password="$LEDGER_API_OWNER_PASSWORD" \
	-v app_password="$LEDGER_API_APP_PASSWORD" \
	-v worker_password="$SETTLEMENT_WORKER_PASSWORD" \
	-v dbname="$POSTGRES_DB" <<'EOSQL'
-- Owns the schema; used only by Flyway.
CREATE ROLE ledger_api_owner LOGIN PASSWORD :'owner_password'
	NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;

-- ledger-api runtime. Not the owner, so it cannot disable triggers or grant itself TRUNCATE.
CREATE ROLE ledger_api_app LOGIN PASSWORD :'app_password'
	NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;

-- settlement-worker runtime; never migrates.
CREATE ROLE settlement_worker LOGIN PASSWORD :'worker_password'
	NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;

-- Postgres 15+ gives PUBLIC no CREATE on schema public.
GRANT CREATE ON SCHEMA public TO ledger_api_owner;

-- No temp tables for non-superusers: pg_temp is searched first for unqualified names,
-- so a temp table could shadow a real one (D-040). The default grant comes from the
-- database owner, so only it or a superuser can revoke it: not Flyway's owner role.
REVOKE TEMPORARY ON DATABASE :"dbname" FROM PUBLIC;
EOSQL
