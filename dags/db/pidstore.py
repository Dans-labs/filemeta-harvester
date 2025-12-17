import psycopg
from psycopg.rows import dict_row

class PIDStore:
    def __init__(self, dsn):
        self.dsn = dsn

    def init_schema(self):
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS harvest_pids (
                        endpoint_id TEXT NOT NULL,
                        pid TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'pending',
                        datestamp TIMESTAMPTZ,
                        updated_at TIMESTAMPTZ DEFAULT now(),
                        PRIMARY KEY (endpoint_id, pid)
                    )
                """)

    def get_most_recent_timestamp(self, endpoint_id):
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT MAX(datestamp) AS last_done
                    FROM harvest_pids
                    WHERE endpoint_id = %s
                """, (endpoint_id,))
                row = cur.fetchone()
                if row and row["last_done"]:
                    return row["last_done"].isoformat()
                return None

    def save_pids(self, endpoint_id, pids):
        """
        Save list of PIDs to the database.
        pids = [{'pid': str, 'datestamp': str}, ...]
        """
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                for record in pids:
                    cur.execute("""
                        INSERT INTO harvest_pids(endpoint_id, pid, status, datestamp)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT(endpoint_id, pid) DO NOTHING
                    """, (
                        endpoint_id,
                        record['pid'],
                        'pending',               # explicitly set status
                        record.get('datestamp')
                    ))

    def get_pending_pids(self, endpoint_id):
        """
        Retrieve PIDs not yet harvested
        """
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT pid FROM harvest_pids
                    WHERE endpoint_id = %s AND status = 'pending'
                """, (endpoint_id,))
                return [row['pid'] for row in cur.fetchall()]

    def mark_done(self, endpoint_id, pid):
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE harvest_pids
                    SET status = 'done',  updated_at = now()
                    WHERE endpoint_id = %s AND pid = %s
                """, (endpoint_id, pid))
    
    def mark_failed(self, endpoint_id, pid):
        with psycopg.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE harvest_pids
                    SET status = 'error',  updated_at = now()
                    WHERE endpoint_id = %s AND pid = %s
                """, (endpoint_id, pid))

