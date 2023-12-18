# Big Data Project - NYC Taxi Data Analysis

![](docs/yellow-cab.png)

## Table of Contents
- [1. Project Overview](#1-project-overview)
   - [1.1. Technologies Used](#11-technologies-used)
   - [1.2. Dataset](#12-dataset)
   - [1.3. Google Cloud Platform (GCP)](#13-google-cloud-platform-gcp)
   - [1.4. Terraform](#14-terraform)
- [2. Workflow Orchestration](#2-workflow-orchestration)
   - [2.1. Prefect](#21-prefect)
   - [2.2. From Web to Google Cloud Storage (GCS)](#22-from-web-to-google-cloud-storage-gcs)
   - [2.3. From Google Cloud Storage to Big Query](#23-from-google-cloud-storage-to-big-query)
   - [2.4. Schedules & Docker Storage with Infrastructure](#24-schedules--docker-storage-with-infrastructure)
   - [2.5. Serverless Prefect Flows with Google Cloud Run Jobs](#25-serverless-prefect-flows-with-google-cloud-run-jobs)
- [3. Data Warehouse](#3-data-warehouse)
   - [3.1. Data Warehouse](#31-data-warehouse)
   - [3.2. ML in Big Query](#32-ml-in-big-query)
- [4. Analytics Engineering](#4-analytics-engineering)
- [5. Batch Processing](#5-batch-processing)


## 1. Project Overview

### 1.1. Technologies Used

- Python
- Docker
- Terraform
- Prefect
- Google Cloud Storage
- Google Cloud Run
- Google Big Query
- dbt
- Looker
- Spark


### 1.2. Dataset

Dataset:
- https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

We will use the dataset from the following [link](https://github.com/DataTalksClub/nyc-tlc-data) for our project.


### 1.3. Google Cloud Platform (GCP)

__GCP Initial Setup__

- 1.Create an account with your Google email ID
- 2.Setup your first project if you haven't already
- 3.Setup service account & authentication for this project
    - Grant Viewer role to begin with.
    - Download service-account-keys (.json) for authentication and put it in the `data` folder.
- 4.Download SDK for local setup
- 5.Set environment variable to point to your downloaded GCP keys


__Setup for Access__
 
- 1.[IAM Roles](https://cloud.google.com/storage/docs/access-control/iam-roles) for Service account:
   * Go to the *IAM* section of *IAM & Admin* https://console.cloud.google.com/iam-admin/iam
   * Click the *Edit principal* icon for your service account.
   * Add these roles in addition to *Viewer* : **Storage Admin** + **Storage Object Admin** + **BigQuery Admin**  + **Service Account User**  + **Cloud Run Admin** + **Artifact Registry Administrator** + **Artifact Registry Repository Administrator**
   
- 2.Enable these APIs for your project:
   * https://console.cloud.google.com/apis/library/iam.googleapis.com
   * https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com
   
- 3.Please ensure `GOOGLE_APPLICATION_CREDENTIALS` env-var is set.
   ```shell
   export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"
   ```


__Google Cloud SDK Authentication__

Set `GOOGLE_APPLICATION_CREDENTIALS` to point to the file
```bash
export GOOGLE_APPLICATION_CREDENTIALS={your_path}/{your_file}.json
```

Now authenticate your SDK with your GCP account
```bash
gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS
```


### 1.4. Terraform

Go to the `terraform` folder and run the following commands:
```bash
# Initialize state file (.tfstate)
terraform init

# Check changes to new infra plan
terraform plan -var="project=<your-gcp-project-id>"

# Or just
terraform plan

# Apply changes to new infra
terraform apply

# Delete infra after your work, to avoid costs on any running services
terraform destroy
```


## 2. Workflow Orchestration

### 2.1. Prefect

__Prefect Local Setup__

Install necessary packages:
```bash
pip install -r requirements.txt
```

Run prefect, then go to `http://127.0.0.1:4200` to see the dashboard.
```bash
prefect orion start

# Configure Prefect to communicate with the server
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
```


__Prefect Cloud Setup__

Run prefect then go to the app to see the dashboard.
```bash
# Login to Prefect Cloud
prefect cloud login
```


### 2.2. From Web to Google Cloud Storage (GCS)

If you choose to run the flow locally, run the following command to start the Prefect server:
```bash
prefect orion start
```

Add a GCP Bucket Block and GCP Credentials Block (Optional) in the Prefect UI.

Run flow locally:
```bash
python flows/etl_web_to_gcs.py
```

Deployment:
```bash
# Build deployment
prefect deployment build flows/etl_web_to_gcs.py:etl_parent_web_to_gcs -n "ETL Web to GCS"

# Apply deployment
prefect deployment apply etl_parent_web_to_gcs-deployment.yaml
```

Or run the following command for short:
```bash
prefect deployment build flows/etl_web_to_gcs.py:etl_parent_web_to_gcs -n "ETL Web to GCS" -a
```

Go to `Deployments` in the Prefect UI to see the deployment. Then, run the deployment directly from the UI. Or you can run the deployment from the command line:
```bash
prefect deployment run 'etl-parent-web-to-gcs/ETL Web to GCS' -p 'months=[1, 2]' -p 'year=2019'
```

Then run the `Work Queues` as below:
```bash
prefect agent start --work-queue "default"
```

__NOTE__: Run terraform to initialize the GCP infrastructure before running the ETL.


### 2.3. From Google Cloud Storage to Big Query

Run flow locally:
```bash
python flows/etl_gcs_to_bq.py
```

Deployment:
```bash
# Build deployment
prefect deployment build flows/etl_gcs_to_bq.py:el_parent_gcs_to_bq -n "ETL GCS to BQ"

# Apply deployment
prefect deployment apply el_parent_gcs_to_bq-deployment.yaml
```

Or run the following command for short:
```bash
prefect deployment build flows/etl_gcs_to_bq.py:el_parent_gcs_to_bq -n "ETL GCS to BQ" -a
```

Go to `Deployments` in the Prefect UI to see the deployment. Then, run the deployment directly from the UI. Or you can run the deployment from the command line:
```bash
prefect deployment run 'el-parent-gcs-to-bq/ETL GCS to BQ' -p 'months=[1, 2]' -p 'year=2019'
```

Then run the `Work Queues` as below:
```bash
prefect agent start --work-queue "default"
```


### 2.4. Schedules & Docker Storage with Infrastructure

Build Docker image:
```bash
docker image build -t itgaming/bigdata-project:latest .
```

Push Docker image to Docker Hub:
```bash
docker image push itgaming/bigdata-project:latest
```

Create a Docker Container Block in the Prefect UI. Remember to config the Volumes as `C:/Users/Admin/.prefect/storage:/root/.prefect/storage`.

Build and apply deployment with Docker Container:
```bash
python flows/docker_etl_web_to_gcs.py
```

Run the Prefect Agent:
```bash
# Start agent
prefect agent start -q default

# Run deployment
prefect deployment run etl-parent-web-to-gcs/docker-etl-web-to-gcs -p "months=[1, 2]"
```

Run EL data from GCS to BQ and then go to the Prefect UI to run the deployment.
```bash
prefect deployment build flows/etl_gcs_to_bq.py:el_parent_gcs_to_bq -n etl_gcs_to_bq -a
```

__NOTE:__ 
- You should config the Prefect API URL before running the flow. If you don't, please run the following command:
```bash
# Configure Prefect to communicate with local server
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api

# Exmaple for debugging
docker run -e PREFECT_API_URL=YOUR_PREFECT_API_URL -e PREFECT_API_KEY=YOUR_API_KEY prefect-docker-guide-image
```


### 2.5. Serverless Prefect Flows with Google Cloud Run Jobs

Build and push Docker image to Google Artifact Registry:
```bash
# Build Docker image
docker build -t asia-east2-docker.pkg.dev/bigdata-405714/bigdata-repo/prefect-agents-etl:latest .

# Login Google Artifact Registry
gcloud auth configure-docker asia-east2-docker.pkg.dev

# Push Docker image to Google Artifact Registry
docker push asia-east2-docker.pkg.dev/bigdata-405714/bigdata-repo/prefect-agents-etl:latest
```

Add GCP Cloud Run Block in the Prefect UI. Then build and apply deployment:
```bash
# Build deployment
prefect deployment build -n "Cloud Run ETL Web to GCS" -ib "cloud-run-job/cloud-run" flows/etl_web_to_gcs.py:etl_parent_web_to_gcs -q default -a

# Run deployment
prefect deployment run "etl-parent-web-to-gcs/Cloud Run ETL Web to GCS" -p "months=[1, 2]" -p "year=2019"
```

For EL data from GCS to BQ, run the following command:
```bash
# Build deployment
prefect deployment build -n "Cloud Run ETL GCS to BQ" -ib "cloud-run-job/cloud-run" flows/etl_gcs_to_bq.py:el_parent_gcs_to_bq -q default -a

# Run deployment
prefect deployment run "el-parent-gcs-to-bq/Cloud Run ETL GCS to BQ" -p "months=[3, 4]" -p "year=2019"
```


## 3. Data Warehouse

### 3.1. Data Warehouse

- See the [SQL](./big_query/big_query.sql) file for the SQL queries.


### 3.2. ML in Big Query

- See the [ML](./big_query/big_query_ml.sql) file for the ML queries.

- To deploy the ML model, run the following command:
```bash
# Log in
gcloud auth login

# Make directory
mkdir tmp/model

# Export the model to GCS
bq --project_id bigdata-405714 extract -m trips_data_all.tip_model gs://bigdata_ml_model/tip_model

# Copy the model to local machine
gsutil cp -r gs://bigdata_ml_model/tip_model tmp/model/

# Make a serving directory
mkdir -p serving_dir/tip_model/1
cp -r tmp/model/tip_model/* serving_dir/tip_model/1

# Pull the docker image
docker pull tensorflow/serving

# Run the docker image
docker run -p 8501:8501 --mount type=bind,source=g/School/Bigdata/Project/serving_dir/tip_model,target=/models/tip_model -e MODEL_NAME=tip_model -t tensorflow/serving

# Send a request to the model
curl -d '{"instances": [{"passenger_count":1, "trip_distance":12.2, "PULocationID":"193", "DOLocationID":"264", "payment_type":"2","fare_amount":20.4,"tolls_amount":0.0}]}' -X POST http://localhost:8501/v1/models/tip_model:predict
```

Access the model at `http://localhost:8501/v1/models/tip_model`. 


## 4. Analytics Engineering

- Create a dbt cloud account and connect to your BiqQuery datawarehouse.
- Load data into BigQuery with `yellow_tripdata` and `green_tripdata` tables.
- Create datasets `dbt_mle` and `production` in BigQuery.

Go to dbt cloud. Run the following commands:
```bash
# Download all the dependencies
dbt deps

# Seed data into BigQuery
dbt seed --full-refresh

# Run the models
dbt run --vars 'is_test_run: false'

# Or run all with one command
dbt build --vars 'is_test_run: false'
```

- Go to [this link](https://lookerstudio.google.com/s/rW6QzYH8BKo) to see the dashboard.


## 5. Batch Processing

- Go to `localhost:4040` to see the Spark UI.
- To prepare the data, run the following command:
```bash
# Run the script
./download_data.sh yellow 2020
./download_data.sh green 2020
./download_data.sh yellow 2021
./download_data.sh green 2021

# Prepare the data
python spark/taxi_schema.py
```

- Go to [this link](https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage) to download the connector and put it in the `spark/jars` folder.

- Upload the data to GCS:
```bash
# Upload the data to GCS
gsutil -m cp -r data/pq/ gs://dtc_data_lake_bigdata-405714/pq
```

- Create a Dataproc cluster in GCP and a bucket `code` in GCS.
- Upload the code to GCS:
```bash
# Upload the code to GCS
gsutil cp spark/spark_sql.py gs://bigdata-code/spark_sql.py
gsutil cp spark/spark_sql_big_query.py gs://bigdata-code/spark_sql_big_query.py

# Submit the job to write data to GCS
gcloud dataproc jobs submit pyspark --cluster=bigdata-cluster --region=asia-east2 gs://bigdata-code/spark_sql.py -- -- --input_green=gs://dtc_data_lake_bigdata-405714/pq/green/2020/*/ --input_yellow=gs://dtc_data_lake_bigdata-405714/pq/yellow/2020/*/ --output=gs://dtc_data_lake_bigdata-405714/report-2020

gcloud dataproc jobs submit pyspark --cluster=bigdata-cluster --region=asia-east2 gs://bigdata-code/spark_sql.py -- -- --input_green=gs://dtc_data_lake_bigdata-405714/pq/green/2021/*/ --input_yellow=gs://dtc_data_lake_bigdata-405714/pq/yellow/2021/*/ --output=gs://dtc_data_lake_bigdata-405714/report-2021

# Submit the job to write data to BigQuery
gcloud dataproc jobs submit pyspark  --cluster=bigdata-cluster --region=asia-east2 --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar gs://bigdata-code/spark_sql_big_query.py -- -- --input_green=gs://dtc_data_lake_bigdata-405714/pq/green/2020/*/ --input_yellow=gs://dtc_data_lake_bigdata-405714/pq/yellow/2020/*/ --output=trips_data_all.reports-2020
```