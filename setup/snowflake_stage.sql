-- Step 1: Create a stage for COVID data
CREATE OR REPLACE STAGE covid_data_stage
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = 'Stage for OWID COVID dataset';

-- Step 2: Upload your CSV file via:
-- Snowsight UI → Data → Stages → "covid_data_stage" → Upload
-- OR using SnowSQL CLI:
-- PUT file:///path/to/owid-covid-data.csv @covid_data_stage;

-- Step 3: Verify upload
LIST @covid_data_stage;

-- Step 4: Load data (already in covid_project.sql)
-- COPY INTO covid_data_raw FROM @covid_data_stage/owid-covid-data.csv
-- FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);