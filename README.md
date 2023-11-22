# Big Data Project - NYC Taxi Data Analysis


## Table of Contents
- [0. Pre-requisites](#0-pre-requisites)
- [1. Introduction](#1-introduction)
- [2. Workflow Orchestration](#2-workflow-orchestration)


## 0. Pre-requisites

We assume that you have the following installed on your machine:
- Docker
- Python
- Google Cloud SDK
- Terraform


## 1. Introduction

### 1.1. Technologies Used

- Python
- Docker
- Postgres
- Google Cloud Platform
- Terraform
- Prefect


### 1.2. Ingesting NY Taxi Data to Postgres

Dataset:
- https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

You can download the dataset from the following [link](https://github.com/DataTalksClub/nyc-tlc-data) for our project.

Create a network for Postgres and pgAdmin:
```bash
docker network create pg-network
```

Run Postgres:
```bash
docker run -it -e POSTGRES_USER="root" -e POSTGRES_PASSWORD="root" -e POSTGRES_DB="ny_taxi" -v G:/School/Bigdata/Project/ny_taxi_postgres_data:/var/lib/postgresql/data -p 5432:5432 --network=pg-network --name pg-database postgres:13
```

Run pgAdmin:
```bash
docker run -it -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" -e PGADMIN_DEFAULT_PASSWORD="root" -p 8080:80 --network=pg-network --name pgadmin-2 dpage/pgadmin4
```

Go to `localhost:8080` and login with the credentials you provided.


### 1.3. Data Ingestion

Run locally:
```bash
python ingest_data.py --user=root --password=root --host=localhost --port=5432 --db=ny_taxi --table_name=yellow_taxi_trips
```

Build Docker image:
```bash
docker build -t taxi_ingest:v002 .
```

Run Docker image:
```bash
docker run -it --rm --network=pg-network taxi_ingest:v002 --user=root --password=root --host=pg-database --port=5432 --db=ny_taxi --table_name=yellow_taxi_trips
```


### 1.4. Docker Compose

Run it:
```bash
docker-compose up -d
```

To stop it:
```bash
docker-compose down
```

__Note__: to make pgAdmin configuration persistent, create a folder `data_pgadmin`. Change its permission via
```bash
sudo chown 5050:5050 data_pgadmin
```

To ingest data into Postgres, run the following command:
```bash
# Insert green taxi data
docker run -it --rm --network=project_default taxi_ingest:v002 --user=root --password=root --host=project-pgdatabase-1 --port=5432 --db=ny_taxi --table_name=green_taxi_trips --url=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz

# Insert zone lookup data
docker run -it --rm --network=project_default taxi_ingest:v002 --user=root --password=root --host=project-pgdatabase-1 --port=5432 --db=ny_taxi --table_name=zone --url=https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
```


### 1.5. Google Cloud Platform (GCP) and Terraform

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
   * Add these roles in addition to *Viewer* : **Storage Admin** + **Storage Object Admin** + **BigQuery Admin**
   
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


__Terraform__

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

Install necessary packages:
```bash
pip install -r requirements.txt
```

Run prefect:
```bash
prefect orion start

# Configure Prefect to communicate with the server
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
```

Then, go to `http://127.0.0.1:4200` to see the dashboard. Configure the SQLAlchemy Block with name `postgres-connector`.

Run ingestion workflow:
```bash
python ingest_data_flow.py
```


### 2.2. ETL with GCP and Prefect

Run prefect:
```bash
prefect orion start
```

Register a block with GCP
```bash
prefect block register -m prefect_gcp
```

Add a GCP Bucket Block and GCP Credentials Block (Optional) in the Prefect UI.

Run ETL:
```bash
python etl_web_to_gcs.py
```

__NOTE__: Run terraform to initialize the GCP infrastructure before running the ETL.


### 2.3. From Google Cloud Storage to Big Query

Run ETL:
```bash
python etl_gcs_to_bq.py
```
