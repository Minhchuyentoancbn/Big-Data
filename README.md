# Big Data Project - NYC Taxi Data Analysis

## Table of Contents
- [Pre-requisites](#pre-requisites)
- [1. Project Overview](#1-project-overview)
   - [1.1. Technologies Used](#11-technologies-used)
   - [1.2. Dataset](#12-dataset)
   - [1.3. Google Cloud Platform (GCP)](#13-google-cloud-platform-gcp)
   - [1.4. Terraform](#14-terraform)
- [2. Workflow Orchestration](#2-workflow-orchestration)
   - [2.1. Prefect](#21-prefect)
   - [2.2. From Web to Google Cloud Storage (GCS)](#22-from-web-to-google-cloud-storage-gcs)
   - [2.3. From Google Cloud Storage to Big Query](#23-from-google-cloud-storage-to-big-query)
   - [2.5. Schedules & Docker Storage with Infrastructure](#25-schedules--docker-storage-with-infrastructure)


## Pre-requisites

We assume that you have the following installed on your machine:
- Docker and Docker Compose
- Python
- Google Cloud SDK
- Terraform


## 1. Project Overview

### 1.1. Technologies Used

- Python
- Docker
- Google Cloud Platform
- Terraform
- Prefect
- GCP Cloud Run


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
prefect deployment build -n "Cloud Run ETL Web to GCS" -ib "cloud-run-job/cloud-run" flows/etl_web_to_gcs.py:etl_parent_web_to_gcs -q default -p gcp-cloud-run-pool -a

# Run deployment
prefect deployment run "etl-parent-web-to-gcs/Cloud Run ETL Web to GCS" -p "months=[3, 4]" -p "year=2019"
```