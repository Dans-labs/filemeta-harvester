# Airflow File Metadata DAGs

## Setup

Create default `airflow.cfg` file using:

```bash
docker compose up airflow-init
```

Adjust the config file to use a local file secret backend by setting the following in `airflow.cfg`:

```
[secrets]
backend = airflow.providers.hashicorp.secrets.local_file.LocalFileBackend
backend_kwargs = {"files": ["/opt/airflow/secrets/secrets.toml"]}
```

Create a secrets.toml file in secrets/ directory to store sensitive information like database passwords.
Add credentials for `harvest_db` in secrets/secrets.toml as follows:

```
[connections.harvester_db]
conn_type = "postgres"
host = "harvest-db"
schema = "filemetrix"
login = "USER"
password = "PASSWORD"
port = 5432
```

## Running Airflow with Docker Compose

```
docker compose up
```

Airflow webserver will be available at `http://localhost:8080`.

## Detailed setup

Follow the instructions here to set up Airflow with Docker Compose:

https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#running-airflow-in-docker
