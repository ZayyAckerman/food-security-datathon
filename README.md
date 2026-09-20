# Food Insecurity & the Double Burden of Malnutrition — Data Pipeline

A small ETL pipeline supporting our Women in Data datathon submission. It extracts, cleans, and loads two research-derived datasets into PostgreSQL, orchestrated with an Airflow DAG — applying the ETL/DAG/Postgres workflow from DataCamp's *Introduction to Data Engineering* course to this project's own data.

## Why this exists

Growing up, I often noticed a pattern that I didn't have a name for: an overweight or obese mother alongside a child who appeared visibly undernourished.

I didn't initially think of this as a data or food-systems question. It was simply something I had seen repeatedly. Later, a friend studying pediatrics introduced me to the term **double burden of malnutrition (DBM)** — a phenomenon in which different forms of malnutrition, such as maternal overweight or obesity and child undernutrition, can coexist within the same household. I also learned that DBM can be studied at different levels, which made me want to understand the phenomenon beyond the individual level.

While researching food systems for the Women in Data datathon, I kept encountering two issues separately: **adult obesity** and **child malnutrition**. That earlier conversation about DBM came back to me.

I started wondering: **what if we don't investigate these as two separate problems, or even only as a single mother-child pairing, but as a household-level phenomenon?**

That became the question behind this project:

> **How can the food system produce conditions in which undernutrition and overweight/obesity coexist within the same household?**

This shifted the project from simply asking whether food insecurity exists to asking what it can look like **inside a household**. Rising food prices can push households toward cheaper, energy-dense staples, while nutrient-dense foods can remain relatively expensive and vulnerable to losses across the food system. These pressures may affect household members differently, potentially contributing to the coexistence of undernutrition and overweight/obesity.

Our core finding connects three parts of that story: the **food-price substitution pathway**, **post-harvest loss of nutrient-dense foods**, and the **double burden of malnutrition**. The pipeline puts the two DBM-relevant datasets behind that investigation into a queryable form instead of leaving them as static spreadsheets, so downstream analysis, charts, or dashboards can work from one source of truth.

## Data sources

| Table                            | Source                                                      | Notes                                                            |
| -------------------------------- | ----------------------------------------------------------- | ---------------------------------------------------------------- |
| `dbm_by_country`                 | Bawuah et al. (2026), *Maternal & Child Nutrition*, Table 1 | 22 sub-Saharan African countries, 103,497 DHS mother-child pairs |
| `sa_food_insecurity_by_province` | Statistics South Africa, GHS Report 03-10-28 (2025)         | South Africa, 2019/2022/2023                                     |

Both are transcribed from published, cited tables rather than pulled from a live API — the underlying microdata (DHS/MICS) is access-gated and required a formal registration process we didn't have time to complete before the submission deadline. See the project's main report PDF for the full source list and that limitation in context.

## Pipeline structure

```text
src/
  extract.py       # reads the two raw CSVs, validates expected columns
  transform.py     # cleans country-name inconsistencies, derives dbm_risk_tier,
                   # computes the female-headed-household food-insecurity gap
  load.py           # writes analysis-ready tables to Postgres via pandas.to_sql
  etl.py            # ties extract -> transform -> load into one callable

dags/
  food_insecurity_dag.py  # Airflow TaskFlow DAG that runs etl()

data/raw/            # versioned source CSVs (see table above)

tests/               # smoke tests for extract/transform (no DB required)
```

## Running it

```bash
pip install -r requirements.txt
cp .env.example .env   # edit if your Postgres isn't on localhost:5433
createdb food_datathon

python src/etl.py
```

Verify it landed:

```sql
SELECT country, dbm_pct, dbm_risk_tier
FROM dbm_by_country
ORDER BY dbm_pct DESC
LIMIT 5;
```

## Running the DAG

```bash
export AIRFLOW_HOME=~/airflow_home

airflow standalone   # first run: initializes the metadata DB, prints a login

# copy dags/food_insecurity_dag.py into $AIRFLOW_HOME/dags/
```

The DAG is scheduled `@monthly` rather than daily. Our sources are periodically revised published research snapshots, not a live operational feed — there's nothing new to extract at midnight every day. If this pipeline is later pointed at a live-updating source (e.g. a FAOSTAT API endpoint), the schedule can be changed back to a cron daily job.

## Known limitations

* **Sample size for the SA table is small** (11 rows) since it's a province-level summary, not row-level survey data — this pipeline demonstrates the ETL/orchestration pattern at the scale our available, ungated data actually supports, rather than simulating a bigger dataset.
* **`if_exists="replace"`** is used on load rather than `"append"`, since each run represents the latest known snapshot of these sources, not an incrementing log.
* No CI workflow is wired up yet (`tests/test_pipeline.py` runs locally with `python tests/test_pipeline.py`); adding a GitHub Actions job that runs it on every push would be the natural next step.



# Food Security & Nutritional Resilience Data Pipeline

## Overview
This project was built for the **Women in Data Datathon** to analyze food security and agricultural trade dependency across target countries. 

It takes raw data on malnutrition and international food trade, processes it to compute a **Nutritional Resilience Index (NRI)**, and stores the results in a PostgreSQL database for reporting and data visualization.

---

## What This Project Does

* **Ingests Raw Data:** Collects regional data on Double Burden of Malnutrition (DBM) and national agricultural trade metrics.
* **Calculates NRI (Nutritional Resilience Index):** Measures how resilient a region's food system is based on its farming diversity, trade reliance, and health indicators.
* **Stores Analytical Data:** Prepares structured PostgreSQL tables and creates a unified view (`view_eat_trade_empowerment_matrix`) for easy querying and dashboard reporting.
* **Automates & Visualizes:** Uses Apache Airflow to run the pipeline automatically and outputs visual chart assets (`data/nri_vs_dbm_chart.png`).

---

## Technical Architecture

The codebase follows a modular ETL (Extract, Transform, Load) structure:

```text
├── dags/
│   └── food_insecurity_dag.py    # Airflow DAG for pipeline orchestration
├── data/
│   └── nri_vs_dbm_chart.png      # Output scatter plot visualization
├── src/
│   ├── extract.py                # Ingestion script for raw datasets
│   ├── transform.py              # Data cleaning & NRI metric calculation logic
│   ├── load.py                   # Loads processed data into PostgreSQL
│   └── schema.sql                # SQL staging tables & view definitions
├── .gitignore                    # Prevents virtual environments & secrets from uploading
└── README.md                     # System documentation

Component Details
1. Data Processing (src/)
* extract.py: Fetches raw data from local files or APIs containing malnutrition rates and import/export balances.
* transform.py: Cleans missing values, normalizes cross-country metrics, and calculates the NRI formula: $$\text{NRI} = f(\text{Agricultural Diversity}, \text{Import Vulnerability}, \text{Nutritional Staging})$$ 
* load.py: Handles database connections and inserts processed records into target SQL tables.
2. Database & SQL Analytics (src/schema.sql)
* Staging Tables: Stores cleaned raw data for malnutrition and trade metrics.
* Analytical View (view_eat_trade_empowerment_matrix): Joins malnutrition percentages (dbm_pct), trade dependency scores, and regional decision metrics into a single table for fast dashboard queries.
3. Orchestration & Charts (dags/ & data/)
* Airflow DAG: Scheduled workflow that automatically runs the Extract, Transform, and Load steps in sequence.
* NRI Plot: Generates a visual plot comparing a country's Nutritional Resilience Index against its Malnutrition Risk Tier.
```

How to Run
1. Set Up Database
Run the SQL script to create the tables and analytical views:
psql -U your_username -d your_database -f src/schema.sql

2. Run the Pipeline Manually
You can run the ETL steps individually using Python:
python3 src/extract.py
python3 src/transform.py
python3 src/load.py

3. Run via Airflow
Move dags/food_insecurity_dag.py to your Airflow dags/ directory and trigger the DAG from the Airflow UI.
Submitted by Buhlebethu Biyela for the Women in Data Datathon.

PROJECT VERIFICATION CODE:WTC-S5PYXJW7







