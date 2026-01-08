from filemeta_harvester.flows.harvester_flows import filemeta_harvest_flow
from filemeta_harvester.config import load_endpoints_config
from multiprocessing import Process

def serve_flow(endpoint):
    filemeta_harvest_flow.serve(
        name=f"filemeta-harvest-{endpoint['id']}",
        parameters={"endpoint": endpoint},
        tags=[endpoint['id']]
    )

if __name__ == "__main__":
    endpoints_config = load_endpoints_config()
    deployed_flows = []
    for endpoint in endpoints_config:
        p = Process(target=serve_flow, args=(endpoint,))
        p.start()
        deployed_flows.append(p)
    for p in deployed_flows:
        p.join()




