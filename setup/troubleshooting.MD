# Troubleshooting Guide

## ❌ "Stage does not exist" error
**Fix**:  
1. Verify stage name matches in both:
   ```sql
   SHOW STAGES;  -- Check existing stages

2. Update the COPY INTO command in covid_project.sql:
    COPY INTO covid_data_raw FROM @your_correct_stage_name/owid-covid-data.csv


3. RUN these as ACCOUNTADMIN 
    GRANTE USAGE ON DATABASE covid_project TO ROLE your_role;
    GRANT ALL ON SCHEMA raw_data TO ROLE your_role;


