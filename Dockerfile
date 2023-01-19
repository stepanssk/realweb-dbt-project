FROM fishtownanalytics/dbt:1.0.0
WORKDIR /realweb-dbt-project

ARG DBT_PROFILES_DIR="."
ENV DBT_PROFILES_DIR $DBT_PROFILES_DIR

ARG DEV_DATASET="dbt_rsultanov"
ENV DEV_DATASET $DEV_DATASET
#/secrets/dbt_runner_for_realweb
ARG PATH_TO_KEYFILE="./secrets/dbt_runner_for_realweb.json"
ENV PATH_TO_KEYFILE $PATH_TO_KEYFILE

COPY . ./

RUN dbt deps

#RUN pip install --no-cache-dir --upgrade pip && \
#    pip install --upgrade --no-cache-dir fal pyTelegramBotAPI