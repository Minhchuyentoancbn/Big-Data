# How to reproduce the project

## Table of Contents
- [1. Google Cloud Platform and Terraform](#1-google-cloud-platform-and-terraform)
   - [1.1. Google Cloud Platform (GCP)](#11-google-cloud-platform-gcp)
   - [1.2. Terraform](#12-terraform)
- [2. Prefect, Docker and Cloud Run](#2-prefect-docker-and-cloud-run)
- [3. Analytics Engineering](#3-analytics-engineering)
- [4. Spark ETL](#4-spark-etl)
- [5. Kafka](#5-kafka)


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

   _NOTE: You might need to add more roles depending on your use case._

   
- 2.Enable these APIs for your project:
   * https://console.cloud.google.com/apis/library/iam.googleapis.com
   * https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com

   _NOTE: You might need to enable more APIs depending on your use case._
   
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


## 3. Analytics Engineering

- See the [SQL](./big_query/big_query.sql) file for the SQL queries.
- Go to [this link](https://lookerstudio.google.com/s/rW6QzYH8BKo) to see the dashboard.


## 4. Spark ETL

- Go to [this link](https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage) to download the connector and put it in the `spark/jars` folder.
- Create a Dataproc cluster in GCP and a bucket `code` in GCS.
- Upload the code to GCS:
```bash
# Upload the code to GCS
gsutil cp spark/spark_sql.py gs://bigdata-code/spark_sql.py
gsutil cp spark/spark_sql_big_query.py gs://bigdata-code/spark_sql_big_query.py

# Submit the job to Dataproc
gcloud dataproc jobs submit pyspark  --cluster=bigdata-cluster --region=asia-east2 --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar clean_data.py -- -- --input_table=all

gcloud dataproc jobs submit pyspark  --cluster=bigdata-cluster --region=asia-east2 --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar create_dims.py -- -- --dataset=production

gcloud dataproc jobs submit pyspark  --cluster=bigdata-cluster --region=asia-east2 --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar create_fact.py -- -- --dataset=production
```

- To see the Spark UI:
```bash
gcloud compute ssh bigdata-cluster-m --project=bigdata-405714 --zone=asia-east2-a -- -D 1080 -N

cd "C:\Program Files\Google\Chrome\Application"

chrome.exe --proxy-server="socks5://localhost:1080" --user-data-dir="%Temp%\bigdata-cluster-m" http://bigdata-cluster-m:8088
```

## 5. Kafka

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

# Start producer
python producer1.py
python producer2.py

# Connect to spark instance
gcloud compute ssh bigdata-cluster-m

# Set the external IP of the VM
export KAFKA_ADDRESS=35.220.200.137
export GCP_GCS_BUCKET=dtc_data_lake_bigdata-405714

# Clone the repo
sudo git clone https://github.com/Minhchuyentoancbn/Big-Data.git

# Run consumer
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3 --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar consumer.py
```