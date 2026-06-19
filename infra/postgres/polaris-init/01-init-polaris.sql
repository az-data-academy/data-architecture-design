-- Pré-création du schéma Polaris (EclipseLink JPA attend polaris_schema)
CREATE SCHEMA IF NOT EXISTS polaris_schema;
GRANT ALL PRIVILEGES ON SCHEMA polaris_schema TO polarisadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA polaris_schema
    GRANT ALL PRIVILEGES ON TABLES TO polarisadmin;
ALTER ROLE polarisadmin SET search_path TO polaris_schema, public;
