# End-to-End Data Platform for NYC Taxi Analytics

![Tech Stack](https://img.shields.io/badge/Databricks-Spark%20%7C%20Delta%20Lake-FF3621)
![Tech Stack](https://img.shields.io/badge/dbt%20Cloud-Analytics%20Engineering-2272FF)
![Tech Stack](https://img.shields.io/badge/GCP-Cloud%20Storage-4285F4)
![Tech Stack](https://img.shields.io/badge/BI-Power%20BI-F2C811)
![CI/CD](https://img.shields.io/badge/CI/CD-dbt%20Cloud-2272FF)

This project demonstrates the design and implementation of a modern, end-to-end data platform on the cloud. It ingests raw NYC Taxi trip data, transforms it using a robust analytics engineering workflow, models it according to the Medallion Architecture, and orchestrates the entire pipeline for production, making it ready for business intelligence and analysis.

## Project Architecture

The platform follows a modern ELT (Extract, Load, Transform) paradigm, leveraging the strengths of cloud-native services for scalability and performance.

**Data Flow:**
`Raw Data (Parquet) -> GCS (Data Lake) -> Databricks (Compute & Lakehouse) -> dbt Cloud (Transformation & Orchestration) -> Gold Layer (BI-Ready Tables)`

> **Architecure Diagram**
>![Architecure Diagram](/assets/images/architecture-full-dark.png)

---

## Key Features & Highlights

This project goes beyond a standard tutorial by incorporating real-world challenges and modern data engineering best practices.

*   **Handling Evolving Data Schemas:** The project uses the latest NYC Taxi dataset (2025 data), which includes new columns like `cbd_congestion_fee` and `airport_fee`. The pipeline is designed to be resilient to these changes using schema evolution techniques.

*   **Adapting to New Business Rules:** The source data introduced a new `payment_type` code (`0` for "Flex Fare trip"). The dbt models and macros were updated to correctly interpret and document this new business logic, demonstrating adaptability.

*   **Professional Project Structure (Medallion Architecture):** The Databricks environment is organized using separate `prod` and `dev` catalogs. The production data flows through a structured Medallion Architecture:
    *   **Bronze (`prod.bronze`):** Raw, untouched data loaded from the data lake.
    *   **Silver (`prod.silver`):** Cleaned, conformed, and integrated data (dimensions and facts).
    *   **Gold (`prod.gold`):** Business-ready, aggregated data marts for analytics.

*   **CI/CD Automation:** The project is configured for production using dbt Cloud. A production job runs on a schedule to refresh the data, and a Continuous Integration (CI) job automatically tests any new code changes in a pull request before they are merged.

*   **Advanced dbt Customization:** The project utilizes custom macros to manage environment-aware schema generation, ensuring a clean and isolated development workflow while maintaining a structured production environment.

---

## Data Source

This project utilizes the publicly available **NYC Taxi and Limousine Commission (TLC) Trip Record Data**.

*   **Dataset:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
*   **Data Used:**
    *   **Green Taxi Trip Records:** Parquet files for Jan-Mar 2025.
    *   **Yellow Taxi Trip Records:** Parquet files for Jan-Mar 2025.
    *   **Taxi Zone Lookup Table:** A CSV file mapping `LocationID` to boroughs and zone names.

The dataset is rich, containing millions of records with details on trip durations, distances, fares, locations, and payment types. For this project, the raw Parquet and CSV files were initially staged in a Google Cloud Storage (GCS) bucket, which served as the data lake.

---

## Core Concepts & Problem Solving

During development, several real-world challenges were encountered and solved, showcasing key data engineering skills.

#### Challenge 1: Case-Sensitivity and Schema Drift
*   **Problem:** Initial queries failed because of case-sensitivity issues (`"VendorID"` vs. `vendorid`) and headers being read as data.
*   **Solution:** The problem was fixed at the root by standardizing all column names to `snake_case` in the `CREATE TABLE` DDL. The `COPY INTO` command was updated with the `'header' = 'true'` option to explicitly ignore header rows, ensuring a clean and predictable schema for the Bronze layer.

#### Challenge 2: Ensuring Global Uniqueness in a Fact Table
*   **Problem:** The `unique` test on the `fact_trips` primary key (`tripid`) was failing. The surrogate key `(vendorid, pickup_datetime)` was not unique when combining Green and Yellow taxi data.
*   **Solution:** The business key was enriched to be globally unique by adding `service_type`. The new key, `(vendorid, pickup_datetime, service_type)`, guarantees that a trip from a Green taxi and a Yellow taxi at the same time will have a unique `tripid`.

#### Challenge 3: Environment-Aware Schema Management
*   **Problem:** By default, dbt would create `gold` tables in the development environment (`dev.dbt_bistp_gold`), cluttering the dev space.
*   **Solution:** A custom `generate_schema_name` macro was implemented. This macro intelligently forces all development models into a single dev schema (`dev.dbt_bistp`) while allowing production models to be built in their correct Medallion layers (`prod.silver`, `prod.gold`).

**Snippet: `macros/generate_schema_name.sql`**
```sql
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if target.database == 'dev' -%}
        {# In dev, ALWAYS use the default schema, ignoring any custom schema #}
        {{ default_schema }}
    {%- else -%}
        {# In prod, respect the custom schema config (e.g., 'gold') #}
        {%- if custom_schema_name is none -%}
            {{ default_schema }}
        {%- else -%}
            {{ custom_schema_name }}
        {%- endif -%}
    {%- endif -%}

{%- endmacro %}
```

---

## dbt Lineage Graph (DAG)

The Directed Acyclic Graph (DAG) shows the flow of data and dependencies from the raw sources to the final data mart.

> **Lineage Graph**
>![Lineage Graph](/assets/images/dbt-dag.png)

> **Success production run - full refresh**
>![Lineage Graph](/assets/images/dbt-dag-prod.png)

---

## dbt Project Structure

The project follows a standard, scalable dbt project structure.

```
├── dbt_project.yml
├── packages.yml
├── macros
│   ├── generate_schema_name.sql
│   ├── get_payment_type_description.sql
│   └── macros_properties.yml
├── models
│   ├── core
│   │   ├── dim_zones.sql
│   │   ├── dm_monthly_zone_revenue.sql
│   │   ├── fact_trips.sql
│   │   └── schema.yml
│   └── staging
│       ├── schema.yml
│       ├── stg_green_tripdata.sql
│       └── stg_yellow_tripdata.sql
└── seeds
    ├── seeds_properties.yml
    └── taxi_zone_lookup.csv
```

---

## Production Deployment with dbt Cloud

The project is fully automated using dbt Cloud jobs.

*   **Production Job:**
    *   **Trigger:** Runs on a daily schedule.
    *   **Command:** `dbt build --vars '{is_test_run: false}'`
    *   **Action:** Performs a full refresh of the data warehouse, running all seeds, models, and tests to ensure data quality and freshness. It also generates the project documentation.

*   **Continuous Integration (CI) Job:**
    *   **Trigger:** Runs automatically on every pull request to the `main` branch.
    *   **Command:** `dbt build --select state:modified+ --defer --state target/`
    *   **Action:** Intelligently builds and tests only the modified models and their downstream dependencies, providing rapid feedback and preventing broken code from being merged into production.

---

## Setup & Installation

To replicate this project, follow these steps:

1.  **Prerequisites:**
    *   A GCP account (for GCS and IAM credentials).
    *   A Databricks account (Community Edition is sufficient).
    *   A dbt Cloud account (Developer plan is sufficient).
    *   A GitHub account.

2.  **Clone the Repository:**
    ```bash
    git clone https://github.com/bISTP/nyc-taxi-databricks-dbt
    ```

3.  **Cloud Setup:**
    *   Create a GCS/S3 bucket and upload the raw NYC Taxi parquet files and the `taxi_zone_lookup.csv`.
    *   In Databricks, create the `prod` and `dev` catalogs.
    *   Run the initial SQL scripts to create the Bronze layer tables in `prod.bronze` and load the data using the `COPY INTO` command.

4.  **dbt Cloud Configuration:**
    *   Create a new dbt Cloud project and connect it to your GitHub repository.
    *   Configure the connection to your Databricks workspace.
    *   Set up a "Development" environment pointing to your `dev` catalog.
    *   Set up a "Production" environment pointing to your `prod` catalog and `silver` schema.

5.  **Run dbt:**
    ```bash
    # Install dependencies
    dbt deps

    # Load seed data
    dbt seed

    # Run and test all models
    dbt build
    ```

---

## Next Steps & Future Work

The data is now modeled, tested, and ready for consumption. The next logical steps are:

1.  **Initial Exploration in Databricks:** Use Databricks Notebooks to perform initial data analysis, query the Gold layer tables, and generate preliminary visualizations to uncover trends.

2.  **Business Intelligence with Power BI:** Connect Power BI directly to the Databricks SQL Warehouse. The `prod.gold.dm_monthly_zone_revenue` table is optimized for this purpose and can be used to build a comprehensive, interactive dashboard for business users to explore revenue trends, service type performance, and zone-based metrics.

---
