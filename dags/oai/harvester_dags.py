from airflow import DAG
import tomllib
from pathlib import Path
from datetime import datetime
from oai.harvester_tasks import initialize_db, initialize_file_db, fetch_pids, process_pending_pids, check_endpoint

CONFIG_PATH = Path("/opt/airflow/config/harvester.toml")


def load_config():
    with CONFIG_PATH.open("rb") as f:
        return tomllib.load(f)


config = load_config()
print("Loaded harvester config:", config)
batch_size = config["harvester"]["batch_size"]

def make_oai_harvest_dag(endpoint: dict, batch_size: int) -> DAG:
    ep = endpoint
    dag_id = f"harvest_{endpoint['id']}"

    print(f"Creating DAG: {dag_id}")
    with DAG(
        dag_id=dag_id,
        description=f"OAI harvest for {endpoint['name']}",
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
        tags=["oai", "harvest"],
    ) as dag:

        check_enfpoint_health = check_endpoint(ep["oai_url"], ep["name"], ep.get("metadata_prefix", "oai_dc"))
        init_harvest_table = initialize_db()
        init_file_table = initialize_file_db()
        process_pending_records = process_pending_pids(ep["id"])
        fetch_pids_from_registry = fetch_pids(ep["oai_url"], ep["id"], ep["name"], ep.get("metadata_prefix", "oai_dc"))
        process_new_records = process_pending_pids(ep["id"])
        check_enfpoint_health >> init_harvest_table >> init_file_table >> process_pending_records >> fetch_pids_from_registry >> process_new_records

    return dag


for endpoint in config["endpoints"]:

    with DAG(
        dag_id="test_db_connection",
        description="Test PostgreSQL harvest_db database connection",
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
        tags=["oai", "harvest", "test"],
    ) as test_dag:
        init_harvest_table = initialize_db()
        init_file_table = initialize_file_db()
        init_harvest_table >> init_file_table
    
    dag = make_oai_harvest_dag(endpoint, batch_size)
    print(f"Created DAG: {dag.dag_id} for endpoint: {endpoint['name']}")
    globals()[dag.dag_id] = dag
