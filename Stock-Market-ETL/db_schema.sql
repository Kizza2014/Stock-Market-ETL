-- RAW TABLES FOR STOCK MARKET ETL
-- This file defines the schema for raw data tables in the Stock Market ETL process.
CREATE TABLE company_raw(
    name VARCHAR(255) NOT NULL,
    ticker VARCHAR(255) NOT NULL,
    cik VARCHAR(255) NULL,
    cusip VARCHAR(255) NULL,
    exchange VARCHAR(255) NOT NULL,
    isDelisted BOOLEAN NOT NULL,
    category VARCHAR(255) NULL,
    sector VARCHAR(255) NULL,
    industry VARCHAR(255) NULL,
    sic VARCHAR(255) NULL,
    sicSector VARCHAR(255) NULL,
    sicIndustry VARCHAR(255) NULL,
    famaSector VARCHAR(255) NULL,
    famaIndustry VARCHAR(255) NULL,
    currency VARCHAR(255) NOT NULL,
    location VARCHAR(255) NULL,
    id VARCHAR(255) PRIMARY KEY
);

CREATE TABLE market_raw(
    market_type VARCHAR(255) NOT NULL,
    region VARCHAR(255) NOT NULL,
    primary_exchanges VARCHAR(255) NOT NULL,
    local_open TIME NOT NULL,
    local_close TIME NOT NULL,
    current_status VARCHAR(255) NOT NULL,
    notes TEXT
);

-- NORMALIZED TABLES FOR STOCK MARKET ETL
-- This file defines the schema for normalized data tables in the Stock Market ETL process.
CREATE TABLE company(
    company_id VARCHAR(255) PRIMARY KEY,
    company_exchange_id VARCHAR(255),
    company_industry_id VARCHAR(255),
    company_sic_id VARCHAR(255),
    company_update_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    company_name VARCHAR(255) NOT NULL,
    company_ticker VARCHAR(255) NOT NULL,
    company_is_delisted BOOLEAN NOT NULL,
    company_category VARCHAR(255) NULL,
    company_currency VARCHAR(255) NOT NULL,
    company_location VARCHAR(255) NULL
);

CREATE TABLE exchange(
    exchange_id VARCHAR(255) PRIMARY KEY,
    exchange_region_id VARCHAR(255),
    exchange_name VARCHAR(255) NOT NULL,
    update_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE industry(
    industry_id VARCHAR(255) PRIMARY KEY,
    industry_name VARCHAR(255) NOT NULL,
    industry_sector VARCHAR(255),
    update_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE region(
    region_id VARCHAR(255) PRIMARY KEY,
    region_name VARCHAR(255) NOT NULL,
    region_local_open TIME NOT NULL,
    region_local_close TIME NOT NULL,
    update_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sicIndustry(
    sic_id VARCHAR(255) PRIMARY KEY,
    sic_industry VARCHAR(255) NOT NULL,
    sic_sector VARCHAR(255),
    update_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- INSERT DATA FROM RAW TABLES TO NORMALIZED TABLES
-- This section contains the SQL statements to insert data from raw tables into normalized tables.
INSERT INTO company(
    company_id, company_exchange_id, company_industry_id, company_sic_id, 
    company_update_at, company_name, company_ticker, company_is_delisted, 
    company_category, company_currency, company_location
) 
SELECT
    id AS company_id,
    exchange AS company_exchange_id,
    industry AS company_industry_id,
    sic AS company_sic_id,
    CURRENT_TIMESTAMP AS company_update_at,
    name AS company_name,
    ticker AS company_ticker,
    isDelisted AS company_is_delisted,
    category AS company_category,
    currency AS company_currency,
    location AS company_location
FROM company_raw;

INSERT INTO exchange(
    exchange_id VARCHAR(255) PRIMARY KEY,
    exchange_region_id VARCHAR(255),
    exchange_name VARCHAR(255) NOT NULL,
    update_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
SELECT 