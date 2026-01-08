# filemeta-harvester

This uses Prefect workflow orchestration to run a file metadata harvester.

## Installation

Use Docker to run a Postgres database to store harvesting data; A Prefect UI server to manage/display pipelines; A client with the filemeta-harvester pipline sources.

```bash
docker compose up
```

Goto http://localhost:4200 to access the Prefect UI. Under deployments you should see the piplines for each repository endpoint.
