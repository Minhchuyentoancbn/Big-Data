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
- Google Cloud Platform
- Terraform
- Prefect


### 1.2. Dataset

Dataset:
- https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

We will use the dataset from the following [link](https://github.com/DataTalksClub/nyc-tlc-data) for our project.


### 1.3. Google Cloud Platform (GCP) and Terraform

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

Then, go to `http://127.0.0.1:4200` to see the dashboard.


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
python flows/etl_web_to_gcs.py
```

__NOTE__: Run terraform to initialize the GCP infrastructure before running the ETL.


### 2.3. From Google Cloud Storage to Big Query

Run ETL:
```bash
python flows/etl_gcs_to_bq.py
```


### 2.4. Parametrizing Flow & Deployments

Run flow:
```bash
python flows/parameterized_flow.py
```

Deployment:
```bash
# Build deployment
prefect deployment build flows/parameterized_flow.py:etl_parent_flow -n "Parameterized ETL"

# Apply deployment
prefect deployment apply etl_parent_flow-deployment.yaml
```

Go to `Deployments` in the Prefect UI to see the deployment. Then, run the deployment.

Run the `Work Queues`:
```bash
prefect agent start --work-queue "default"
```


### 2.5. Schedules & Docker Storage with Infrastructure


