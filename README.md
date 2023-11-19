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

### 1.2. Docker

Run the following command to build the docker image
```
docker build -t bigdata:1.0 .
```

Run the following command to run the docker image
```
docker run -it bigdata:1.0
```

### 1.3. Ingesting NY Taxi Data to Postgres

Running Postgres in Docker:
```bash
docker run -it -e POSTGRES_USER="root" -e POSTGRES_PASSWORD="root" -e POSTGRES_DB="ny_taxi" -v G:/School/Bigdata/Project/ny_taxi_postgres_data:/var/lib/postgresql/data -p 5432:5432 postgres:13
```

Install pgcli:
```bash
pip install pgcli
```

Using pgcli to connect to Postgres:
```bash
pgcli -h localhost -p 5432 -u root -d ny_taxi
```

Dataset:
- https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

Download the dataset from the following [link](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz) and place it in the `data` folder.

Run the following command to ingest the data into Postgres:
```bash
python upload_data.py
```