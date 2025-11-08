-- === DIMENSIONS ===
CREATE TABLE Dim_Pays (
    iso3 CHAR(3) PRIMARY KEY,
    country_name VARCHAR(100),
    region_wb VARCHAR(100),
    region_un VARCHAR(100),
    region_un_sub VARCHAR(100),
    income_group VARCHAR(50),
    population_latest BIGINT NULL,
    gdp_latest DECIMAL(18,2) NULL
);

CREATE TABLE Dim_Temps (
    year INT PRIMARY KEY,
    quarter TINYINT NULL,
    month TINYINT NULL,
    decade INT NULL
);

CREATE TABLE Dim_Sex (
    sex_key INT IDENTITY(1,1) PRIMARY KEY,
    sex VARCHAR(20)
);

CREATE TABLE Dim_Age (
    age_key INT IDENTITY(1,1) PRIMARY KEY,
    age_group VARCHAR(50)
);

CREATE TABLE Dim_Sector (
    sector_key INT IDENTITY(1,1) PRIMARY KEY,
    sector VARCHAR(100)
);

CREATE TABLE Dim_Indicateur (
    indicator_key INT IDENTITY(1,1) PRIMARY KEY,
    indicator_name VARCHAR(120),
    unit VARCHAR(40),
    source VARCHAR(80)
);

-- === FACTS ===
CREATE TABLE Fact_Inequality (
    iso3 CHAR(3) NOT NULL REFERENCES Dim_Pays(iso3),
    year INT NOT NULL REFERENCES Dim_Temps(year),
    gini_std DECIMAL(10,4) NULL,
    gini DECIMAL(10,4) NULL,
    palma DECIMAL(10,4) NULL,
    s80s20 DECIMAL(10,4) NULL,
    CONSTRAINT PK_Fact_Inequality PRIMARY KEY (iso3, year)
);

CREATE TABLE Fact_LabourEquity (
    iso3 CHAR(3) NOT NULL REFERENCES Dim_Pays(iso3),
    year INT NOT NULL REFERENCES Dim_Temps(year),
    sex_key INT NULL REFERENCES Dim_Sex(sex_key),
    age_key INT NULL REFERENCES Dim_Age(age_key),
    sector_key INT NULL REFERENCES Dim_Sector(sector_key),
    indicator_key INT NOT NULL REFERENCES Dim_Indicateur(indicator_key),
    value DECIMAL(18,6) NULL,
    CONSTRAINT PK_Fact_LabourEquity PRIMARY KEY (iso3, year, sex_key, age_key, sector_key, indicator_key)
);
