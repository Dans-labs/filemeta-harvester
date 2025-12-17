FROM apache/airflow:3.1.4

RUN pip install --no-cache-dir \
    git+https://github.com/dans-labs/datahugger.git@main \
    git+https://github.com/dans-labs/filefetcher.git@master \
    requests \
    sickle \
    psycopg[binary] \
    sqlmodel

