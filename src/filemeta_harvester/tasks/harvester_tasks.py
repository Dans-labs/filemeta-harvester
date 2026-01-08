import filefetcher
from prefect import task
from datetime import datetime
from filemeta_harvester.oai.harvester import OAIHarvester 
from filemeta_harvester.db.filestore import FileRecord, FileRawRecord, FileRawRecordStore, FileRecordStore, create_pg_engine
from filemeta_harvester.db.pidstore import PIDStore
from filemeta_harvester.config import load_config
from time import sleep


@task(log_prints=True)
def initialize_db():
    """
    Initialize the PID store schema in the database.
    """

    db = load_config()

    dsn = f"host={db.host} dbname={db.name} user={db.user} password={db.password} port={db.port}"
    store = PIDStore(dsn)
    store.init_schema()
    print("Harvest schema initialized.")

@task(log_prints=True)
def initialize_file_db():
    """
    Initialize the file record store schema in the database.
    """

    db = load_config()
    pg_dsn = f"postgresql://{db.user}:{db.password}@{db.host}:{db.port}/{db.name}"
    file_store = FileRecordStore(create_pg_engine(pg_dsn))
    file_store.init_schema()
    print("File schema initialized.")
    raw_file_store = FileRawRecordStore(create_pg_engine(pg_dsn))
    raw_file_store.init_schema()
    print("Raw File schema initialized.")

@task(log_prints=True)
def check_endpoint(endpoint_url, name, prefix):
    """
    Check the health of the OAI-PMH endpoint by performing an Identify request.
    """
    
    harvester = OAIHarvester(endpoint_url, prefix)
    identity = harvester.identify()
    print(f"Identified endpoint '{name}': {identity}")
    return True

@task(log_prints=True)
def fetch_pids(endpoint_url, endpoint_id, name, prefix):
    """
    Fetch PIDs from the OAI-PMH endpoint starting from the last done timestamp.
    """
    print(f"Fetching PIDs from endpoint '{name}' ({endpoint_url}) with prefix '{prefix}'")
    sleep(60)  # DEV
    return 

    db = load_config()
    dsn = f"host={db.host} dbname={db.name} user={db.user} password={db.password} port={db.port}"
    store = PIDStore(dsn)
    last_done = store.get_most_recent_timestamp(endpoint_id)
    if last_done:
        print(f"Resuming from last done timestamp: {last_done}")
        dt = datetime.fromisoformat(last_done)
        #last_done = f"{dt.date().isoformat()}T00:00:00Z"
        last_done = dt.date().isoformat()
    harvester = OAIHarvester(endpoint_url, prefix)
    print(f"Last done timestamp: {last_done}")
    pids = harvester.get_pid_list(from_date=last_done)
    store.save_pids(endpoint_id, pids)
    print(f"Fetched {len(pids)} PIDs")
    return len(pids)

def strip_pid(pid):
    """
    Strip common PID prefixes from a PID string.
    
    Args:
        pid (str): the PID string to strip
    
    Returns:
        str: stripped PID
    """

    prefixes = ["doi:", "hdl:", "ark:/"]
    for prefix in prefixes:
        if pid.startswith(prefix):
            return pid[len(prefix):]
    return pid

@task(log_prints=True)
def process_pending_pids(endpoint_id):
    """
    Process pending PIDs: fetch file records and create file entries in the database.
    """
    print(f"Processing pending PIDs for endpoint ID: {endpoint_id}")
    return

    db = load_config()
    dsn = f"host={db.host} dbname={db.name} user={db.user} password={db.password} port={db.port}"
    store = PIDStore(dsn)
    pending = store.get_pending_pids(endpoint_id)
    pg_dsn = f"postgresql://{db.user}:{db.password}@{db.host}:{db.port}/{db.name}"
    file_store = FileRecordStore(create_pg_engine(pg_dsn))
    raw_file_store = FileRawRecordStore(create_pg_engine(pg_dsn))
    failed_cnt = 0
    done_cnt = 0

    for pid in pending:
        try:
            files = filefetcher.file_records(strip_pid(pid))
            raw_files = filefetcher.file_raw_records(strip_pid(pid))
            raw_record = FileRawRecord(
                dataset_pid=strip_pid(pid),
                raw_metadata=raw_files,
            )
            raw_file_store.create_one(raw_record)
            record_list = []
            for f in files:
                record = FileRecord(
                    name=f.get("name"),
                    dataset_pid=f.get("dataset_pid"),
                    link=f.get("link"),
                    size=int(f.get("size")),
                    mime_type=f.get("mime_type"),
                    ext=f.get("ext"),
                    checksum_value=f.get("checksum_value"),
                    checksum_type=f.get("checksum_type"),
                    access_request=f.get("access_request"),
                    publication_date=f.get("publication_date"),
                    embargo=f.get("embargo"),
                    file_pid=f.get("file_pid"),
                )
                record_list.append(record)
            file_store.create_many(record_list)

        except Exception as e:
            print(f"Error creating file record for PID {pid}: {e}")
            store.mark_failed(endpoint_id, pid)
            failed_cnt += 1
            continue

        store.mark_done(endpoint_id, pid)
        done_cnt += 1
        # DEV
        # break
    return {"done": done_cnt, "failed": failed_cnt}

