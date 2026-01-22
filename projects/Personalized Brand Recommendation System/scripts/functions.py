import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import petl as etl

from sqlalchemy.dialects.mysql import insert
from sqlalchemy import MetaData, Table
from datetime import datetime
from db import Connections
from itertools import combinations

import json
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
    
MAX_WORKERS_PARSING = 20
BATCH_SIZE_PARSING = 5000

def extract_batch(df_batch):
    """Extract salesNum & menuID dari 1 batch dataframe."""
    results = []

    for _, row in df_batch.iterrows():
        salesNum = row['salesNum']
        data = json.loads(row['orderData'])

        for m in data.get('salesMenu', []):
            results.append((salesNum, m.get('menuID')))

    return results

def process_order_data(dfResult):
    batches = [
        dfResult[i:i+BATCH_SIZE_PARSING]
        for i in range(0, len(dfResult), BATCH_SIZE_PARSING)
    ]

    all_results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_PARSING) as executor:
        futures = [executor.submit(extract_batch, batch) for batch in batches]

        for f in futures:
            all_results.extend(f.result())

    df_salesMenu = pd.DataFrame(all_results, columns=['salesNum', 'menuID'])

    df_grouped = (
        df_salesMenu
        .groupby('salesNum')['menuID']
        .agg(lambda x: ', '.join(map(str, x)))
        .reset_index()
    )

    df_multi = df_grouped[df_grouped['menuID'].str.contains(',')]

    df_final = df_multi.merge(
        dfResult.drop(columns=['orderData']),
        on='salesNum',
        how='left'
    )

    return df_final

dwh_menu = Connections.dwh('esb_devMenuNewsletter')

def process_outlet(data, dfMenuPairing, column_mapping1, column_mapping2):
    dbName = data['dbName']
    companyCode = data['companyCode']
    branchID = data['branchID']
    branchCode = data['branchCode']

    dfPairFilt = dfMenuPairing[
        (dfMenuPairing['dbName'] == dbName) &
        (dfMenuPairing['branchID'] == branchID)
    ]

    pairings = []
    for menu_ids in dfPairFilt['menuIDSplit']:
        pairings.extend(combinations(menu_ids, 2))

    if not pairings:
        return pd.DataFrame()

    pairing_counts = pd.DataFrame()
    pairing_counts[['menuID', 'pairingCount']] = (
        pd.Series(pairings).value_counts().reset_index()
    )

    pairing_counts[['menuID1', 'menuID2']] = pairing_counts['menuID'].apply(pd.Series)
    pairing_counts = pairing_counts.drop('menuID', axis=1)

    # mirror pairs
    pairing_counts1 = pairing_counts.rename(columns=column_mapping1).copy()
    pairing_counts2 = pairing_counts.rename(columns=column_mapping2).copy()

    df_pairingMenu = pd.concat([pairing_counts1, pairing_counts2], ignore_index=True)
    df_pairingMenu = df_pairingMenu.groupby(['menuID', 'menuIDRelated'], as_index=False).agg({'pairingCount': 'sum'})
    df_pairingMenu['menuRelatedRank'] = df_pairingMenu.groupby('menuID')['pairingCount'].rank(
        ascending=False, method='first'
    )

    df_pairingMenuTop = df_pairingMenu[df_pairingMenu['menuRelatedRank'] <= 5]

    # add metadata
    df_pairingMenuTop = df_pairingMenuTop.assign(
        companyCode=companyCode,
        branchCode=branchCode,
        dbName=dbName,
        branchID=branchID
    )

    return df_pairingMenuTop

def process_company(row, column_mapping1, column_mapping2):
    dfMenuPairing = etl.fromdb(
        dbo=dwh_menu,
        query=row.query
    ).todataframe()

    # split menuID
    dfMenuPairing['menuIDSplit'] = dfMenuPairing['menuID'].apply(
        lambda x: list(
            set(int(item.strip(',')) for item in x.split(', ') if item.strip(','))
        )
    )

    df_outlet = dfMenuPairing[['dbName', 'companyCode', 'branchCode', 'branchID']] \
                    .drop_duplicates().reset_index(drop=True)

    result_outlet = []

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = []
        for _, data in df_outlet.iterrows():
            futures.append(
                executor.submit(
                    process_outlet,
                    data,
                    dfMenuPairing,
                    column_mapping1,
                    column_mapping2
                )
            )

        for f in futures:
            result_outlet.append(f.result())

    if result_outlet:
        return pd.concat(result_outlet, ignore_index=True)
    return pd.DataFrame()

def run_parallel(df_uniqueCompany, batch_size=5):
    print(f"{datetime.today()} - Extracting data...")

    column_mapping1 = {'menuID1': 'menuID', 'menuID2': 'menuIDRelated'}
    column_mapping2 = {'menuID2': 'menuID', 'menuID1': 'menuIDRelated'}

    df_results = []
    total = len(df_uniqueCompany)

    # batching per perusahaan
    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        batch = df_uniqueCompany.iloc[start:end]

        print(f"\nProcessing batch {start//batch_size + 1} ({end}/{total})...")

        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            futures = [
                executor.submit(process_company, row, column_mapping1, column_mapping2)
                for row in batch.itertuples()
            ]

            for f in futures:
                df_results.append(f.result())

    return pd.concat(df_results, ignore_index=True)

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
