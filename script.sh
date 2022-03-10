#!/bin/sh

# Команды, который выполняет Cloud Run

# Installing dependencies
dbt deps --profiles-dir .

# Checking if it is working
dbt debug --target dev --profiles-dir .
dbt debug --target prod --profiles-dir .

# Running
dbt run --target prod --profiles-dir .
# dbt run --target dev --profiles-dir .

# Testing
dbt test --target prod --profiles-dir .

dbt clean --profiles-dir .