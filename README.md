# Big Data Project - NYC Taxi Data Analysis

## Table of Contents
- [0. Pre-requisites](#0-pre-requisites)
- [1. Introduction](#1-introduction)

## 0. Pre-requisites

We assume that you have the following installed on your machine:
- Docker
- Python

## 1. Introduction

### 1.1. Technologies Used

- Python
- Docker
- Postgres

### 1.2. Ingesting NY Taxi Data to Postgres

Dataset:
- https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

Download the dataset from the following [link](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz) and place it in the `data` folder.

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
docker build -t taxi_ingest:v001 .
```

Run Docker image:
```bash
docker run -it --network=pg-network taxi_ingest:v001 --user=root --password=root --host=pg-database --port=5432 --db=ny_taxi --table_name=yellow_taxi_trips
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