# dbt-snowflake-project
An end-to-end data engineering pipeline that transforms unstructured PDF documents (SEC filings/financial reports) into a structured Lakehouse architecture for advanced analytics.
🚀 Overview
This project demonstrates a robust ELT (Extract, Load, Transform) pattern designed to handle unstructured data. We ingest raw PDFs, extract key financial entities using Python, and leverage the power of Snowflake and Databricks for scalable processing and modeling.

🛠️ Tech Stack
Language: Python 3.10+ (Data extraction & Orchestration)

Data Lakehouse: Snowflake (Storage & Compute)

Processing: Databricks (Large-scale PDF parsing & Spark jobs)

Transformation: dbt (Core) for modular SQL modeling

Environment: Virtualenv & python-dotenv for secrets management

🏗️ Architecture (Medallion Pattern)
The data flows through a multi-layer architecture within the Snowflake Lakehouse:

Bronze (Raw): Original PDF text and metadata extracted via Python.

Silver (Staging): Cleaned, filtered, and normalized tabular data using dbt.

Gold (Mart): Business-ready aggregated views for BI and reporting.

## 📁 Project Structure

```text
├── data/
│   ├── raw/             # Source PDFs
│   └── processed/       # Extracted CSV/JSON outputs
├── dbt_project/         # dbt models, tests, and snapshots
├── scripts/
│   ├── extract_pdf.py   # Python logic (pdfplumber/PyMuPDF)
│   └── load_to_sf.py    # Snowflake ingestion script
├── .env                 # Environment variables (ignored)
├── requirements.txt     # Python dependencies
└── README.md
🔧 Getting Started
Clone the repo: git clone https://github.com/maheshreddysama/pdf-de-project.git

Setup Venv: python -m venv venv and .\venv\Scripts\activate

Install Dependencies: pip install -r requirements.txt

Configure Snowflake: Update your .env file with your credentials.

Run Extraction: python scripts/extract_pdf.py

dbt Transformation: cd dbt_project && dbt run
