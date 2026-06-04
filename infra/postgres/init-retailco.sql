-- ============================================================
-- RetailCo — Base de données opérationnelle (source CDC)
-- Utilisée pour la démo Debezium live
-- ============================================================

-- Activer la réplication logique au niveau table
ALTER SYSTEM SET wal_level = logical;

-- Schéma RetailCo
CREATE SCHEMA IF NOT EXISTS retailco;

-- Table des transactions (source CDC)
CREATE TABLE IF NOT EXISTS retailco.transactions (
    transaction_id  VARCHAR(50)  PRIMARY KEY,
    store_id        VARCHAR(20)  NOT NULL,
    store_name      VARCHAR(100) NOT NULL,
    store_region    VARCHAR(50)  NOT NULL,
    customer_id     VARCHAR(50),
    product_id      VARCHAR(20)  NOT NULL,
    product_name    VARCHAR(200) NOT NULL,
    category        VARCHAR(50)  NOT NULL,
    unit_price      DECIMAL(10,2) NOT NULL,
    quantity        INTEGER      NOT NULL,
    discount_pct    DECIMAL(5,3) DEFAULT 0.0,
    total_amount    DECIMAL(12,2) NOT NULL,
    payment_method  VARCHAR(30),
    channel         VARCHAR(30)  NOT NULL,
    return_flag     BOOLEAN      DEFAULT FALSE,
    transaction_ts  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Publication pour Debezium
CREATE PUBLICATION debezium_pub FOR TABLE retailco.transactions;

-- Données initiales pour la démo
INSERT INTO retailco.transactions VALUES
  ('txn-demo-001','STR-007','RetailCo Paris Opéra','Île-de-France','cust-001','PRD-001','TV 55p 4K','Électronique',599.00,1,0.05,682.86,'CB','Web',false,NOW()),
  ('txn-demo-002','STR-042','RetailCo Lyon Bellecour','Auvergne-Rhône-Alpes','cust-002','PRD-002','Jean slim H38','Vêtements',49.99,2,0.10,107.98,'PayPal','App mobile',false,NOW()),
  ('txn-demo-003','STR-115','RetailCo Marseille Centre','PACA',NULL,'PRD-003','Casserole inox','Maison',35.90,1,0.00,43.08,'Espèces','Magasin',false,NOW());

-- Slot de réplication pour Debezium
SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput');
