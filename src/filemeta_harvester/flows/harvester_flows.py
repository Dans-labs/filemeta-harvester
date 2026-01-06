from prefect import flow
from filemeta_harvester.config import load_endpoints_config
from filemeta_harvester.tasks.harvester_tasks import (
            initialize_db, 
            initialize_file_db, 
            check_endpoint, 
            fetch_pids,
            process_pending_pids
)

endpoints_config = load_endpoints_config()

@flow(log_prints=True)
def filemeta_harvest_flow(endpoint: dict):
    print(f"Starting harvest flow for endpoint: {endpoint['name']}")
    check_endpoint(endpoint["oai_url"], endpoint["name"], endpoint.get("metadata_prefix", "oai_dc"))
    initialize_db()
    initialize_file_db()
    process_pending_pids(endpoint['id'])
    fetch_pids(endpoint['oai_url'], endpoint['id'], endpoint['name'], endpoint.get('metadata_prefix', 'oai_dc'))
    process_pending_pids(endpoint['id'])

if __name__ == "__main__":
    for endpoint in endpoints_config:
        filemeta_harvest_flow(endpoint)
