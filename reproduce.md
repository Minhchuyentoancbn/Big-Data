# How to reproduce the project

## Table of Contents
- [1. Google Cloud Platform and Terraform](#1-google-cloud-platform-and-terraform)
   - [1.1. Google Cloud Platform (GCP)](#11-google-cloud-platform-gcp)
   - [1.2. Terraform](#12-terraform)
- [2. Prefect, Docker and Cloud Run](#2-prefect-docker-and-cloud-run)
- [3]


## 1. Google Cloud Platform and Terraform

### 1.1. Google Cloud Platform (GCP)

__GCP Initial Setup__

- 1.Create an account with your Google email ID
- 2.Setup your first project if you haven't already
- 3.Setup service account & authentication for this project
    - Grant Viewer role to begin with.
    - Download service-account-keys (.json) for authentication.
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


### 1.2. Terraform

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


## 2. Prefect, Docker and Cloud Run

### 2.1. Prefect

__Prefect Cloud Setup__

Run prefect then go to the app to see the dashboard.
```bash
# Login to Prefect Cloud
prefect cloud login
```


### 2.2. From Web to Google Cloud Storage (GCS)

Add a GCP Bucket Block and GCP Credentials Block (Optional) in the Prefect UI.

Build and apply deployment:
```bash
prefect deployment build -sb "github/bigdata-github" flows/etl_web_to_gcs.py:etl_parent_web_to_gcs -n "ETL Web to GCS" -a
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

Build and apply deployment:
```bash
prefect deployment build -sb "github/bigdata-github" flows/etl_gcs_to_bq.py:el_parent_gcs_to_bq -n "ETL GCS to BQ" -a
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

Run Docker image:
```bash
# Exmaple for debugging
docker run --rm -it --entrypoint bash -e PREFECT_API_URL="https://api.prefect.cloud/api/accounts/84dd3f9a-1c11-42e7-bad7-e318173cc5a4/workspaces/3ee41cd4-af20-496d-a9f1-56bd7779db5d" -e PREFECT_API_KEY=pnu_V19kAXsXkdG4aZTGw8cbUlxWQ84VEh2gIsCw itgaming/bigdata-project:latest
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
prefect deployment build -n "Cloud Run ETL Web to GCS 2" -ib "cloud-run-job/cloud-run" flows/etl_web_to_gcs.py:etl_parent_web_to_gcs -q default -a

# Run deployment
prefect deployment run "etl-parent-web-to-gcs/Cloud Run ETL Web to GCS" -p "months=[1, 2]" -p "year=2019"
prefect deployment run "etl-parent-web-to-gcs/Cloud Run ETL Web to GCS" -p "months=[1, 2]" -p "year=2021" -p "color=green"
```

For EL data from GCS to BQ, run the following command:
```bash
# Build deployment
prefect deployment build -n "Cloud Run ETL GCS to BQ" -ib "cloud-run-job/cloud-run" flows/etl_gcs_to_bq.py:el_parent_gcs_to_bq -q default -a

# Run deployment
prefect deployment run "el-parent-gcs-to-bq/Cloud Run ETL GCS to BQ" -p "months=[3, 4]" -p "year=2019"
```


## 3. Data Warehouse

- See the [SQL](./big_query/big_query.sql) file for the SQL queries.


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


gcloud dataproc jobs submit pyspark  --cluster=bigdata-cluster --region=asia-east2 --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar clean_data.py -- -- --input_table=all

gcloud dataproc jobs submit pyspark  --cluster=bigdata-cluster --region=asia-east2 --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar preprocess.py -- -- --dataset=production
```

## 6. Stream Processing

Run the following command to setup Kafka and Zookeeper:
```bash
# Connect to VM
gcloud compute ssh kafka-instance

# Clone the repo
git clone https://github.com/Minhchuyentoancbn/Big-Data.git
cd Big-Data
bash ./setup/vm_setup.sh
exec newgrp docker

# Set the external IP of the VM
export KAFKA_ADDRESS=35.220.200.137

# Start kafka
cd kafka
pip install -r requirements.txt
docker-compose build
docker-compose up -d

# Turn off kafka
docker-compose down

# Connect to spark instance
gcloud compute ssh bigdata-cluster-m

# Set the external IP of the VM
export KAFKA_ADDRESS=35.220.200.137
export GCP_GCS_BUCKET=dtc_data_lake_bigdata-405714

# Clone the repo
git clone https://github.com/Minhchuyentoancbn/Big-Data.git

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3 streaming.py
```