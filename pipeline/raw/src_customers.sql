-- =============================================================
-- SOURCE FILE 2: Raw Customers Data  (PostgreSQL)
-- Simulates a CSV ingestion from a CRM system.
--
-- Intentional data quality issues present
-- =============================================================

CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.src_customers;

CREATE TABLE raw.src_customers (
    customer_id        TEXT,
    first_name         TEXT,
    last_name          TEXT,
    email              TEXT,
    phone              TEXT,
    country            TEXT,        
    city               TEXT,
    signup_date        TEXT,        
    customer_segment   TEXT,        
    loyalty_points     TEXT,        
    ingest_ts          TIMESTAMP DEFAULT NOW()
);

-- ── Insert raw source rows ────────────────────────────────────
INSERT INTO raw.src_customers
    (customer_id, first_name, last_name, email, phone, country, city, signup_date, customer_segment, loyalty_points)
VALUES
('CUST-101', 'Alice',   'Morgan',   'alice.morgan@email.com',   '+1-212-555-0101', 'US',            'New York',      '2023-01-15', 'Premium',  '1250'),
('CUST-102', 'Bob',     'Chen',     'bob.chen@email.com',       '+1-310-555-0102', 'US',            'Los Angeles',   '2023-02-20', 'Standard', '340'),
('CUST-103', 'Carol',   'Davis',    'carol.davis@email.com',    '+1-312-555-0103', 'US',            'Chicago',       '2023-03-05', 'Premium',  '980'),
('CUST-104', 'David',   'Kim',      'david.kim@email.com',      '+1-713-555-0104', 'US',            'Houston',       '2023-03-18', 'Standard', '120'),
('CUST-105', 'Eva',     'Russo',    'eva.russo@email.com',      '+1-602-555-0105', 'US',            'Phoenix',       '2023-04-01', 'Trial',    '50'),
('CUST-106', 'Frank',   'Lee',      'frank.lee@email.com',      '215-555-0106',    'usa',           'Philadelphia',  '2023-04-10', 'Standard', '670'),
('CUST-107', 'Grace',   'Patel',    'grace.patel@email.com',    '2105550107',      'United States', 'San Antonio',   '2023-05-03', 'Premium',  '1540'),
('CUST-108', 'Henry',   'Torres',   'henry.torres@email.com',   '(619)555-0108',   'U.S.',          'San Diego',     '2023-05-22', 'Standard', '290'),
('CUST-109', 'Iris',    'Nakamura', 'iris.nakamura@',           '+1-214-555-0109', 'US',            'Dallas',        '2023-06-08', 'Trial',    '10'),
('CUST-110', 'James',   'White',    'NOT_AN_EMAIL',             '+1-408-555-0110', 'US',            'San Jose',      '2023-06-19', 'Standard', '450'),
('CUST-111', 'Karen',   'Brown',    'karen.brown@email.com',    '+1-512-555-0111', 'US',            'Austin',        'Jan 15 2023','Premium',  '820'),
('CUST-112', 'Leo',     'Garcia',   'leo.garcia@email.com',     '+1-904-555-0112', 'US',            'Jacksonville',  '15/02/2023', 'Standard', '190'),
('CUST-113', 'Mia',     'Wilson',   'mia.wilson@email.com',     '+1-817-555-0113', 'US',            'Fort Worth',    '2023-07-04',  NULL,      '0'),
('CUST-114', 'Noah',    'Martinez', 'noah.martinez@email.com',  '+1-210-555-0114', 'US',            'San Antonio',   '2023-07-20', 'Trial',     NULL),
('CUST-101', 'Alice',   'Morgan',   'alice.morgan@email.com',   '+1-212-555-0101', 'US',            'New York',      '2023-01-15', 'Premium',  '1250'),
('CUST-115', 'Olivia',  'Anderson', 'olivia.a@email.com',       '+1-650-555-0115', 'US',            'San Francisco', '2023-08-11', 'Premium',  '2100'),
('CUST-116', 'Paul',    'Jackson',  'paul.j@email.com',         '+1-206-555-0116', 'US',            'Seattle',       '2023-09-01', 'Standard', '510'),
('CUST-117', 'Quinn',   'Harris',   'quinn.h@email.com',        '+1-720-555-0117', 'US',            'Denver',        '2023-09-15', 'Trial',    '75'),
('CUST-118', 'Rachel',  'Clark',    'rachel.c@email.com',       '+1-617-555-0118', 'US',            'Boston',        '2023-10-02', 'Premium',  '1875'),
('CUST-119', 'Sam',     'Lewis',    'sam.l@email.com',          '+1-503-555-0119', 'US',            'Portland',      '2023-10-20', 'Standard', '330');
