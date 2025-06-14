# 🦠 COVID-19 Global Data Analytics Dashboard using Snowflake

## 📌 Project Overview
This project analyzes global COVID-19 data using the Snowflake data cloud. It enables powerful insights into:
- 🦠 Case and death trends
- 💉 Vaccination progress
- ⚠️ Country-level risk classification using UDFs
- 📊 Dashboard-ready views for reporting

---

## 🛠️ Technologies Used
- **Snowflake** Data Warehouse
- **SQL** for data ingestion, transformation, and analytics
- **User-Defined Functions (UDFs)** for classification and formatting
- **Snowsight** Dashboard (Snowflake UI)

---

## ⚙️ Setup Instructions
1. Run the SQL scripts in order from the `sql_scripts/` folder.
2. Load your COVID-19 dataset (`owid-covid-data.csv`) into the Snowflake internal stage.
3. Execute the `covid_project.sql` script to create required tables, views, and UDFs.

---

## 🖼️ Dashboard Snapshots

### 🌍 Global Summary Dashboard
![Global Summary Dashboard](Screenshot%202025-06-14%20185954.png)

### 💉 Vaccination Analysis Dashboard
![Vaccination Dashboard](Screenshot%202025-06-14%20190003.png)


💡 Future Enhancements
🔄 Automate daily data refresh using Snowflake Tasks and Streams

📈 Integrate with Streamlit or Power BI for richer visualizations

🧠 Add predictive modeling using Python (e.g., forecast cases, risk spikes)

🧪 Extend UDFs to calculate infection rate, growth rate, or regional heatmaps

🧠 Learning Outcomes
Built an end-to-end pipeline inside Snowflake

Used advanced SQL concepts and UDFs

Connected business relevance with technical implementation

Created dashboards to support data-driven decisions in public health
