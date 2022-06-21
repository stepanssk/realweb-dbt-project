FROM golang:1.13 as builder
WORKDIR /app
COPY invoke.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -v -o server

FROM fishtownanalytics/dbt:1.0.0
USER root
WORKDIR /realweb-dbt-project
COPY --from=builder /app/server ./
COPY script.sh ./
COPY . ./
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --upgrade --no-cache-dir fal pyTelegramBotAPI

ENTRYPOINT "./server"