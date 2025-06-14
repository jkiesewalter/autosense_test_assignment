# EV Charging Data Engineering Assignment

## Overview

This project is a test assignment for a Data Engineer role. It involves working with four datasets from an EV charging mobile application and performing the following tasks:

- Exploratory Data Analysis (EDA): Analyze the datasets for anomalies, inconsistencies, and user behavior insights.
- Data Transformation and API Development: Transform the datasets and load them into a columnar database, then create a REST API to query the data efficiently.

The scripts as well as the REST API are written in Python. Amazon Redshift was chosen as the columnar database. An S3 bucket is used to copy the data into the database.

## Project structure

```
├── analysis/
│   ├── analyze_json_files.ipynb   # Jupyter Notebook for EDA
├── python-rest-api/
│   ├── src/
│   │   ├── app.py                 # REST API implementation
│   │   ├── controllers/
│   │   │   ├── user_controller.py # User-related API logic
│   │   │   ├── charger_controller.py # Charger-related API logic
│   │   │   ├── transaction_controller.py # Transaction-related API logic
│   ├── load_to_redshift_script/
│   │   ├── load_json_to_redshift.py # Script for data transformation and loading
├── README.md                      # Project documentation
```

## Setup Instructions

1. Prerequisites

- Amazon Redshift database
- AWS S3 bucket

2. Install Dependencies

Install the required Python packages with the requirements.txt
`pip install -r requirements.txt`

3. Run Exploratory Data Analysis

Open the Jupyter Notebook and run the cells.
`jupyter notebook analysis/analyze_json_files.ipynb`

4. Load Data into Redshift

Run the data transformation and loading script.
`python python-rest-api/write_to_db/load_json_to_redshift.py`

5. Start the REST API

Run the Flask application. The API will be available at http://127.0.0.1:5000.
`python python-rest-api/src/app.py`

## API Endpoints

1. `GET /users`

Returns all users or filters by user_id, first_name, last_name, and email.
Example: `curl -X GET "http://127.0.0.1:5000/users?first_name=Mathew"`

2. `GET /chargers`

Returns all chargers or filters by charger_id and city.
Example: `curl -X GET "http://127.0.0.1:5000/chargers?city=Zurich"`

3. `GET /chargers/{charger_id}/usage-analytics`

Returns usage analytics for a specific charger.
Example: `curl -X GET "http://127.0.0.1:5000/chargers/charger_123/usage-analytics?start_datetime=2025-01-01&end_datetime=2025-05-31"`

4. `GET /transactions-extended`

Returns all transactions in extended format with optional filters.
Example: `curl -X GET "http://127.0.0.1:5000/transactions-extended?min_kwh=10&max_amount_charged=50"`

## Limitations:

This project was developed as part of a time-constrained assignment and, as such, has the following limitations:

- Security: No authentication or authorization mechanisms are implemented for the REST API. This means the API would be publicly accessible and would not restrict access based on user roles or permissions.
Sensitive information, such as database credentials and AWS keys, is stored in environment variables but could benefit from additional security measures like secret management tools (e.g., AWS Secrets Manager). Furthermore, if the datasets were not faked, the Redshift database would contain highly user sensitive data and should be encrypted.

- Error Handling: Limited error handling is implemented in the REST API and data processing scripts. Unexpected inputs or database errors may not return user-friendly error messages.

- Scalability: The current implementation is designed for a small dataset and may not scale efficiently for large datasets or high API traffic without further optimization (e.g., caching, pagination).

- Testing: Unit tests and integration tests are not included. The code has been manually tested but lacks automated test coverage to ensure robustness.

- Data Quality: While basic data cleaning and anomaly detection are implemented, more advanced techniques (e.g., machine learning-based anomaly detection) could improve data quality analysis.

- Visualization: The exploratory data analysis (EDA) includes basic visualizations, but more advanced visualizations (e.g., interactive dashboards) could provide deeper insights for stakeholders.

- Documentation: The documentation provides an overview of the project and setup instructions but lacks detailed explanations of the codebase and design decisions.

- Deployment: The REST API is designed to run locally and does not include deployment configurations for production environments (e.g., Docker, Kubernetes, or cloud deployment).

- Clean Code: While the code is functional, it does not fully adhere to clean code principles such as using consistent naming conventions and modularity, which could improve readability, maintainability, and scalability.