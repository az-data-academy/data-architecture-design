-- =============================================================================
-- init-polaris.sql — Pré-création du schéma Polaris
-- Exécuté automatiquement par PostgreSQL au premier démarrage du container
-- (docker-entrypoint-initdb.d/)
--
-- Polaris 1.4 utilise EclipseLink JPA avec le schéma "polaris_schema".
-- Sans ce fichier, si Polaris démarre avant la migration DDL complète,
-- il obtient : relation "polaris_schema.entities" does not exist
-- =============================================================================

-- Créer le schéma attendu par Polaris
CREATE SCHEMA IF NOT EXISTS polaris_schema;

-- Donner tous les droits au user Polaris sur ce schéma
GRANT ALL PRIVILEGES ON SCHEMA polaris_schema TO polarisadmin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA polaris_schema TO polarisadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA polaris_schema
    GRANT ALL PRIVILEGES ON TABLES TO polarisadmin;

-- S'assurer que le search_path inclut polaris_schema
ALTER ROLE polarisadmin SET search_path TO polaris_schema, public;
