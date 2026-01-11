import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import petl as etl

from sqlalchemy.dialects.mysql import insert
from sqlalchemy import MetaData, Table
from datetime import datetime

from json import dumps
from httplib2 import Http

# load .env
import os
from os.path import join, dirname
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), ".env")
load_dotenv(dotenv_path)

Webhooks_GCHAT = os.environ.get("Webhooks_GCHAT")
Airflow_Web = os.environ.get("Airflow_Web")

def upsertDBv2(df, tableName, con, excludedColumns=[], metadata_obj="-"):
    startTime = datetime.now()
    print(
        "{startTime} - Start upsertDBv2 to {host}.{dbName}.{tableName}".format(
            tableName=tableName,
            dbName=con.url.database,
            host=con.url.host,
            startTime=startTime,
        )
    )

    # Get table object
    if metadata_obj == "-":
        metadata_obj = MetaData(bind=con.connect())
    else:
        print("Using custom metadata_obj")

    with con.begin() as conn:
        table = Table(tableName, metadata_obj, autoload_with=conn)

    # Drop excluded columns
    allExcludedColumns = (
        ["createdAt", "updatedAt", "editDate", "editedDate"]
        + [col.name for col in table._columns if col.name not in df.columns.tolist()]
        + excludedColumns
    )
    print("Columns to be excluded: ")
    for col in [col for col in table._columns if col.name in allExcludedColumns]:
        print("\t{}".format(col))
        table._columns.remove(col)

    # Reformat and clean df
    records = df.fillna("-").to_dict(orient="records")
    records_cleaned = [
        {
            key: (val if val not in ["-", "None", "", "NaT"] else None)
            for key, val in x.items()
        }
        for x in records
    ]

    # Construct insert update (upsert) query
    insert_stmt = insert(table)
    update_dict = {x.name: x for x in insert_stmt.inserted}
    upsert_stmt = insert_stmt.on_duplicate_key_update(update_dict)

    # Execute query with cleaned records
    with con.begin() as conn:
        print(
            "Upserting {len} rows to {host}.{dbName}.{tableName}...".format(
                tableName=table.name,
                dbName=con.url.database,
                host=con.url.host,
                len=len(df),
            )
        )
        conn.execute(upsert_stmt, records_cleaned)

    endTime = datetime.now()
    delta = endTime - startTime
    print(
        "{endTime} - Upsert Done :). Duration: {delta}".format(
            delta=delta, endTime=endTime
        )
    )

def upsertDBv3(df, tableName, con, excludedColumns=[], metadata_obj="-"):
    startTime = datetime.now()
    print(
        "{startTime} - Start upsertDBv2 to {host}.{dbName}.{tableName}".format(
            tableName=tableName,
            dbName=con.url.database,
            host=con.url.host,
            startTime=startTime,
        )
    )

    # Get table object
    if metadata_obj == "-":
        metadata_obj = MetaData(bind=con.connect())
    else:
        print("Using custom metadata_obj")

    with con.begin() as conn:
        table = Table(tableName, metadata_obj, autoload_with=conn)

    # Drop excluded columns
    allExcludedColumns = (
        ["createdAt", "updatedAt", "editDate", "editedDate"]
        + [col.name for col in table._columns if col.name not in df.columns.tolist()]
        + excludedColumns
    )
    print("Columns to be excluded: ")
    for col in [col for col in table._columns if col.name in allExcludedColumns]:
        print("\t{}".format(col))
        table._columns.remove(col)

    NOT_NULL_COLUMNS = [col.name for col in table.columns if not col.nullable]

    # Reformat and clean df
    records = df.to_dict(orient="records")
    records_cleaned = []
    for x in records:
        row = {}
        for key, val in x.items():
            if key in NOT_NULL_COLUMNS:
                # Untuk NOT NULL, biarkan "" menjadi ""
                row[key] = val if val not in ["-", "None", "NaT"] else ""
            else:
                # Kolom lain boleh menjadi NULL
                row[key] = val if val not in ["-", "None", "", "NaT"] else None
        records_cleaned.append(row)


    # Construct insert update (upsert) query
    insert_stmt = insert(table)
    update_dict = {x.name: x for x in insert_stmt.inserted}
    upsert_stmt = insert_stmt.on_duplicate_key_update(update_dict)

    # Execute query with cleaned records
    with con.begin() as conn:
        print(
            "Upserting {len} rows to {host}.{dbName}.{tableName}...".format(
                tableName=table.name,
                dbName=con.url.database,
                host=con.url.host,
                len=len(df),
            )
        )
        conn.execute(upsert_stmt, records_cleaned)

    endTime = datetime.now()
    delta = endTime - startTime
    print(
        "{endTime} - Upsert Done :). Duration: {delta}".format(
            delta=delta, endTime=endTime
        )
    )

def fetch_schema(con, sql):
    """Fungsi untuk eksekusi query per engine."""
    df = pd.read_sql(con=con['engine'], sql=sql)
    df = df[df['dbName'].str[:4] == 'fnb_']  # filter fnb_
    df['con'] = con['name']
    return df

def load_schema(cons, schema, max_workers=8, batch_size=5):
    print("Loading Schema (ThreadPool + Batching + Multi-Engine)...")

    results = []

    # Batching kons
    for i in range(0, len(cons), batch_size):
        batch = cons[i:i+batch_size]
        print(f"\tProcessing batch {i//batch_size + 1} ({len(batch)} engines)...")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_con = {
                executor.submit(fetch_schema, con, schema): con 
                for con in batch
            }

            for future in as_completed(future_to_con):
                con = future_to_con[future]
                try:
                    df = future.result()
                    results.append(df)
                except Exception as e:
                    print(f"\tError on {con['name']}: {e}")

    # Gabung semua hasil
    if results:
        return pd.concat(results, ignore_index=True)
    else:
        return pd.DataFrame()

def run_query(engine, query):
    """Execute query and return dataframe."""
    return etl.fromdb(dbo=engine, query=query).todataframe()

def extract_data_parallel(dfListDB, cons, max_workers=10, batch_size=20):
    print(f"{datetime.today()} - Extracting data (ThreadPool + Batching + Multi-Engine)...")

    df_results = []

    # Pre-map engine per name (lebih cepat lookup)
    engine_map = {c['name']: c['engine'] for c in cons}

    total = len(dfListDB)
    total_batches = (total + batch_size - 1) // batch_size

    for batch_idx, start in enumerate(range(0, total, batch_size), 1):
        end = min(start + batch_size, total)
        batch = dfListDB.iloc[start:end]

        print(f"\t{datetime.today().time()} - Processing batch {batch_idx}/{total_batches} ({len(batch)} rows)...")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}

            for idx, row in batch.iterrows():
                engine = engine_map[row["con"]]
                query = row["query"]
                fut = executor.submit(run_query, engine, query)
                futures[fut] = row

            # Collect responses
            for fut in as_completed(futures):
                row = futures[fut]

                try:
                    temp = fut.result()

                    # === APPLY FILTERING DI SINI ===
                    temp = temp[~temp["salesNum"].isna()]       # Remove NULL salesNum
                    temp = temp[temp["menuID"].str.contains(",")]  # menuID must contain comma

                    df_results.append(temp)

                except Exception as e:
                    print(f"\tError con={row['con']}, db={row['dbName']}: {e}")

    # Merge final results
    if df_results:
        return pd.concat(df_results, ignore_index=True)
    else:
        return pd.DataFrame()

def sendMessageToGoogleChat(
    url=Webhooks_GCHAT,
    message=f"{datetime.now()} - TESTING",
):
    message_headers = {"Content-Type": "application/json; charset=UTF-8"}
    http_obj = Http()
    app_message = {"text": message}
    response = http_obj.request(
        uri=url,
        method="POST",
        headers=message_headers,
        body=dumps(app_message),
    )

def airflow_task_failure_alert(context):
    sendMessageToGoogleChat(
        message=f"ALERT: An Airflow task has failed\ntask_instance_key_str: {context['task_instance_key_str']}\n{Airflow_Web}/dags/{context['task_instance_key_str'].split('__')[0]}/grid",
        url=Webhooks_GCHAT,
    )
