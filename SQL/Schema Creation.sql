-- SQL Server Schema Creation Script for Global Wage Equity, Inequality, Labor Power Data Warehouse

-- Create the database (only if needed)
CREATE DATABASE DW_Inequality;
GO

-- Use the database
USE DW_Inequality;
GO

-- Create Schemas
    CREATE SCHEMA dim;
    GO

    CREATE SCHEMA fact;
    GO

-- Create Dimension Tables

-- Dim_Geography
CREATE TABLE dim.Dim_Geography (
    geography_key INT PRIMARY KEY,
    iso3 VARCHAR(3),
    country_name VARCHAR(255),
    subregion_un VARCHAR(255),
    region_un VARCHAR(255),
    continent VARCHAR(255),
    income_group VARCHAR(255),
    population_latest FLOAT,
    gdp_per_capita FLOAT,
    is_oecd BIT,
    is_eu BIT,
    is_g20 BIT
);

-- Dim_Time
CREATE TABLE dim.Dim_Time (
    year INT,
    time_key INT PRIMARY KEY,
    decade INT,
    five_year_period INT,
    is_crisis_year BIT,
    is_pre_covid BIT,
    is_post_covid BIT
);

-- Dim_Indicator
CREATE TABLE dim.Dim_Indicator (
    indicator_code VARCHAR(255),
    indicator_name VARCHAR(255),
    domain VARCHAR(255),
    theme VARCHAR(255),
    category VARCHAR(255),
    unit VARCHAR(255),
    source VARCHAR(255),
    indicator_key INT PRIMARY KEY
);

-- Dim_Age
CREATE TABLE dim.Dim_Age (
    age_group VARCHAR(255),
    age_category VARCHAR(255),
    age_key INT PRIMARY KEY
);

-- Dim_Sex
CREATE TABLE dim.Dim_Gender (
    gender_code CHAR(1),
    gender_label VARCHAR(255),
    gender_key INT PRIMARY KEY
);

-- Dim_Source
CREATE TABLE dim.Dim_Source (
    source_code VARCHAR(255),
    full_name VARCHAR(255),
    organization VARCHAR(255),
    data_quality_rating VARCHAR(255),
    update_frequency VARCHAR(255),
    coverage_start_year INT,
    coverage_end_year INT,
    source_key INT PRIMARY KEY
);

-- Dim_Economic_Classification
CREATE TABLE dim.Dim_Economic_Classification (
    income_group VARCHAR(255),
    development_status VARCHAR(255),
    economic_classification_key INT PRIMARY KEY,
    valid_from_year INT,
    valid_to_year INT
);

-- Create Fact Tables

-- Fact_Economy
CREATE TABLE fact.Fact_Economy (
    geography_key INT,
    time_key INT,
    sex_key INT,
    age_key INT,
    economic_classification_key INT,
    indicator_key INT,
    value FLOAT,
    FOREIGN KEY (geography_key) REFERENCES dim.Dim_Geography(geography_key),
    FOREIGN KEY (time_key) REFERENCES dim.Dim_Time(time_key),
    FOREIGN KEY (sex_key) REFERENCES dim.Dim_Sex(sex_key),
    FOREIGN KEY (age_key) REFERENCES dim.Dim_Age(age_key),
    FOREIGN KEY (economic_classification_key) REFERENCES dim.Dim_Economic_Classification(economic_classification_key),
    FOREIGN KEY (indicator_key) REFERENCES dim.Dim_Indicator(indicator_key)
);

-- Fact_Inequality
CREATE TABLE fact.Fact_Inequality (
    geography_key INT,
    time_key INT,
    economic_classification_key INT,
    source_key INT,
    indicator_key INT,
    value FLOAT,
    FOREIGN KEY (geography_key) REFERENCES dim.Dim_Geography(geography_key),
    FOREIGN KEY (time_key) REFERENCES dim.Dim_Time(time_key),
    FOREIGN KEY (economic_classification_key) REFERENCES dim.Dim_Economic_Classification(economic_classification_key),
    FOREIGN KEY (source_key) REFERENCES dim.Dim_Source(source_key),
    FOREIGN KEY (indicator_key) REFERENCES dim.Dim_Indicator(indicator_key)
);

-- Fact_SocialDevelopment
CREATE TABLE fact.Fact_SocialDevelopment (
    geography_key INT,
    time_key INT,
    economic_classification_key INT,
    source_key INT,
    indicator_key INT,
    value FLOAT,
    FOREIGN KEY (geography_key) REFERENCES dim.Dim_Geography(geography_key),
    FOREIGN KEY (time_key) REFERENCES dim.Dim_Time(time_key),
    FOREIGN KEY (economic_classification_key) REFERENCES dim.Dim_Economic_Classification(economic_classification_key),
    FOREIGN KEY (source_key) REFERENCES dim.Dim_Source(source_key),
    FOREIGN KEY (indicator_key) REFERENCES dim.Dim_Indicator(indicator_key)
);

-- Create indexes for better performance
CREATE INDEX IX_Fact_Economy_Geography_Time ON fact.Fact_Economy(geography_key, time_key);
CREATE INDEX IX_Fact_Inequality_Geography_Time ON fact.Fact_Inequality(geography_key, time_key);
CREATE INDEX IX_Fact_SocialDevelopment_Geography_Time ON fact.Fact_SocialDevelopment(geography_key, time_key);

PRINT 'Schema created successfully!';