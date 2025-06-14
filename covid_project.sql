CREATE OR REPLACE DATABASE covid_project;
CREATE OR REPLACE SCHEMA raw_data;
CREATE OR REPLACE WAREHOUSE covid_wh;


-- creating a table in snowflake 
CREATE OR REPLACE TABLE covid_data_raw (
    iso_code STRING,
    continent STRING,
    location STRING,
    date DATE,
    total_cases FLOAT,
    new_cases FLOAT,
    new_cases_smoothed FLOAT,
    total_deaths FLOAT,
    new_deaths FLOAT,
    new_deaths_smoothed FLOAT,
    total_cases_per_million FLOAT,
    new_cases_per_million FLOAT,
    new_cases_smoothed_per_million FLOAT,
    total_deaths_per_million FLOAT,
    new_deaths_per_million FLOAT,
    new_deaths_smoothed_per_million FLOAT,
    reproduction_rate FLOAT,
    icu_patients FLOAT,
    icu_patients_per_million FLOAT,
    hosp_patients FLOAT,
    hosp_patients_per_million FLOAT,
    weekly_icu_admissions FLOAT,
    weekly_icu_admissions_per_million FLOAT,
    weekly_hosp_admissions FLOAT,
    weekly_hosp_admissions_per_million FLOAT,
    total_tests FLOAT,
    new_tests FLOAT,
    total_tests_per_thousand FLOAT,
    new_tests_per_thousand FLOAT,
    new_tests_smoothed FLOAT,
    new_tests_smoothed_per_thousand FLOAT,
    positive_rate FLOAT,
    tests_per_case FLOAT,
    tests_units STRING,
    total_vaccinations FLOAT,
    people_vaccinated FLOAT,
    people_fully_vaccinated FLOAT,
    total_boosters FLOAT,
    new_vaccinations FLOAT,
    new_vaccinations_smoothed FLOAT,
    total_vaccinations_per_hundred FLOAT,
    people_vaccinated_per_hundred FLOAT,
    people_fully_vaccinated_per_hundred FLOAT,
    total_boosters_per_hundred FLOAT,
    new_vaccinations_smoothed_per_million FLOAT,
    new_people_vaccinated_smoothed FLOAT,
    new_people_vaccinated_smoothed_per_hundred FLOAT,
    stringency_index FLOAT,
    population FLOAT,
    population_density FLOAT,
    median_age FLOAT,
    aged_65_older FLOAT,
    aged_70_older FLOAT,
    gdp_per_capita FLOAT,
    extreme_poverty FLOAT,
    cardiovasc_death_rate FLOAT,
    diabetes_prevalence FLOAT,
    female_smokers FLOAT,
    male_smokers FLOAT,
    handwashing_facilities FLOAT,
    hospital_beds_per_thousand FLOAT,
    life_expectancy FLOAT,
    human_development_index FLOAT,
    excess_mortality_cumulative_absolute FLOAT,
    excess_mortality_cumulative FLOAT,
    excess_mortality FLOAT,
    excess_mortality_cumulative_per_million FLOAT
);


-- temporary usage 
CREATE OR REPLACE STAGE covid_stage;


--copying of the data 
COPY INTO covid_data_raw
FROM @covid_stage/owid-covid-data.csv
FILE_FORMAT = (
  TYPE = 'CSV'
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
);

-- creating a new table 
CREATE OR REPLACE TABLE covid_data_clean AS
SELECT
    iso_code,
    continent,
    location,
    date,
    population,
    total_cases,
    new_cases,
    total_deaths,
    new_deaths,
    people_vaccinated,
    people_fully_vaccinated,
    total_boosters,
    
    -- Metrics
    (new_cases / NULLIF(population, 0)) * 100000 AS cases_per_100k,
    (new_deaths / NULLIF(population, 0)) * 100000 AS deaths_per_100k,
    (people_vaccinated / NULLIF(population, 0)) * 100 AS vaccinated_pct,
    (people_fully_vaccinated / NULLIF(population, 0)) * 100 AS fully_vaccinated_pct,
    (total_boosters / NULLIF(population, 0)) * 100 AS boosters_pct

FROM covid_data_raw
WHERE
    continent IS NOT NULL
    AND population IS NOT NULL
    AND date IS NOT NULL;



show tables 


-- a view for the latest data per country 
CREATE OR REPLACE VIEW latest_country_data AS
SELECT *
FROM covid_data_clean c
QUALIFY ROW_NUMBER() OVER (PARTITION BY location ORDER BY date DESC) = 1;


-- A UDF
CREATE OR REPLACE FUNCTION get_risk_level(cases_per_100k FLOAT)
RETURNS STRING
AS
$$
    CASE 
        WHEN cases_per_100k IS NULL THEN 'Unknown'
        WHEN cases_per_100k < 1 THEN 'Low'
        WHEN cases_per_100k < 10 THEN 'Moderate'
        WHEN cases_per_100k < 25 THEN 'High'
        ELSE 'Critical'
    END
$$;


-- VIEW + UDF IN QUERY 
SELECT
    location,
    date,
    population,
    total_cases,
    new_cases,
    cases_per_100k,
    vaccinated_pct,
    fully_vaccinated_pct,
    get_risk_level(cases_per_100k) AS risk_level
FROM latest_country_data
ORDER BY risk_level DESC;


-- creating an aggreagted Global Summary View 

CREATE OR REPLACE VIEW global_summary AS
SELECT
    CURRENT_DATE AS report_date,
    COUNT(DISTINCT location) AS total_countries,
    SUM(total_cases) AS global_cases,
    SUM(total_deaths) AS global_deaths,
    SUM(people_vaccinated) AS total_vaccinated,
    SUM(people_fully_vaccinated) AS total_fully_vaccinated,
    SUM(total_boosters) AS total_boosters,
    AVG(vaccinated_pct) AS avg_vaccinated_pct
FROM latest_country_data;


--Continent-Level Aggregation
CREATE OR REPLACE VIEW continent_summary AS
SELECT
    continent,
    COUNT(DISTINCT location) AS countries,
    SUM(total_cases) AS total_cases,
    SUM(total_deaths) AS total_deaths,
    AVG(cases_per_100k) AS avg_case_rate,
    AVG(deaths_per_100k) AS avg_death_rate,
    AVG(vaccinated_pct) AS avg_vaccinated_pct
FROM latest_country_data
GROUP BY continent;


-- Final Table for Dashboard
CREATE OR REPLACE TABLE covid_dashboard AS
SELECT
    l.location,
    l.date,
    l.population,
    l.total_cases,
    l.total_deaths,
    l.people_vaccinated,
    l.vaccinated_pct,
    l.cases_per_100k,
    get_risk_level(l.cases_per_100k) AS risk_level,
    c.continent
FROM latest_country_data l
JOIN covid_data_clean c
  ON l.location = c.location AND l.date = c.date
WHERE c.continent IS NOT NULL;




-- new table with updated columns 
CREATE OR REPLACE TABLE covid_dashboard AS
SELECT
    l.location,
    l.date,
    l.population,
    l.total_cases,
    l.new_cases,
    l.total_deaths,
    l.people_vaccinated,
    l.people_fully_vaccinated,  -- ✅ Needed for the new column
    ROUND(100.0 * l.people_vaccinated / NULLIF(l.population, 0), 2) AS vaccinated_pct,
    ROUND(100.0 * l.people_fully_vaccinated / NULLIF(l.population, 0), 2) AS fully_vaccinated_pct, -- ✅ NEW COLUMN
    ROUND(100000.0 * l.total_cases / NULLIF(l.population, 0), 2) AS cases_per_100k,
    get_risk_level(ROUND(100000.0 * l.total_cases / NULLIF(l.population, 0), 2)) AS risk_level,
    c.continent
FROM latest_country_data l
JOIN covid_data_clean c
  ON l.location = c.location AND l.date = c.date
WHERE c.continent IS NOT NULL;




-- MAKING OF THE TILES 


-- Top 10 Most Affected Countries (by new cases per 100k)
SELECT location, cases_per_100k, new_cases
FROM covid_dashboard
WHERE cases_per_100k IS NOT NULL
ORDER BY cases_per_100k DESC
LIMIT 10;

--Top 10 Safest Countries (Lowest case rate)
SELECT location, cases_per_100k, new_cases
FROM covid_dashboard
WHERE cases_per_100k IS NOT NULL AND new_cases IS NOT NULL
ORDER BY cases_per_100k ASC
LIMIT 10;


-- Vaccination Gap Table
SELECT location, vaccinated_pct, fully_vaccinated_pct, (vaccinated_pct - fully_vaccinated_pct) AS gap_pct
FROM covid_dashboard
WHERE vaccinated_pct IS NOT NULL AND fully_vaccinated_pct IS NOT NULL
ORDER BY gap_pct DESC;


-- Daily Trend: Global New Cases
CREATE OR REPLACE VIEW daily_trend AS
SELECT
    date,
    SUM(new_cases) AS global_new_cases,
    SUM(new_deaths) AS global_new_deaths
FROM covid_data_clean
GROUP BY date
ORDER BY date;


-- Average Risk by Continent 
SELECT
    continent,
    COUNT(*) AS country_count,
    ROUND(AVG(cases_per_100k), 2) AS avg_case_rate,
    ROUND(AVG(vaccinated_pct), 2) AS avg_vaccination,
    ROUND(AVG(fully_vaccinated_pct), 2) AS avg_fully_vaccinated
FROM covid_dashboard
GROUP BY continent
ORDER BY avg_case_rate DESC;



-- add 7 days moving averages 
CREATE OR REPLACE VIEW moving_avg_trend AS
SELECT
    location,
    date,
    AVG(new_cases) OVER (PARTITION BY location ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS new_cases_7day_avg,
    AVG(new_deaths) OVER (PARTITION BY location ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS new_deaths_7day_avg
FROM covid_data_clean;
