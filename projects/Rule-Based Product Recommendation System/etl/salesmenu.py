# %%
from pathlib import Path
import sys

# cari parent yang punya folder 'scripts' (naik sampai root)
p = Path.cwd().resolve()
for parent in [p] + list(p.parents):
    if (parent / "scripts").is_dir():
        project_root = parent
        break
else:
    raise RuntimeError("Could not find 'scripts' folder in parent path")

sys.path.insert(0, str(project_root))
print("Added to sys.path:", project_root)

# %%
import pandas as pd

# %%
from scripts.functions import load_schema, extract_data_parallel, upsertDBv2
from scripts.db import Connections

QUERY_DIR = project_root / "query"

# %%
count_week = 2

# %%
# Create connections
db1 = Connections.db1()
db2 = Connections.db2()
db3 = Connections.db3()
db4 = Connections.db4()

# dwh = Connections.dwh()
billing = Connections.billing('esb_billing')

cons = [
    {'name': 'db1', 'engine':db1}, 
    {'name': 'db2', 'engine':db2}, 
    {'name': 'db3', 'engine':db3}, 
    {'name': 'db4', 'engine':db4}, 
]

cons = [con for con in cons if con['engine'] != 'error']

# %%
schemaSalesMenu = (QUERY_DIR / "schemaSalesMenu.sql").read_text()
querySalesMenu = (QUERY_DIR / "querySalesMenu.sql").read_text()
queryBilling = (QUERY_DIR / "queryBilling.sql").read_text()

# %%
dbsSalesMenu = load_schema(
    cons=cons,
    schema=schemaSalesMenu,
    max_workers=8,
    batch_size=5
)

# %%
compDetSalesMenu = pd.read_sql(con=billing, sql=queryBilling)

dfSalesMenu = dbsSalesMenu.merge(right=compDetSalesMenu, how='left', on='dbName')
dfSalesMenu = dfSalesMenu[['con','dbName','companyCode','companyName','companyGroupName','isBilled']]
dfSalesMenu['companyName'] = dfSalesMenu.apply(lambda row: row.companyName.replace('"','').replace("'","") if pd.isna(row.companyName) == False else None, axis=1)
dfSalesMenu['companyGroupName'] = dfSalesMenu.apply(lambda row: row.companyGroupName.replace('"','').replace("'","") if pd.isna(row.companyGroupName) == False else None, axis=1)

# %%
dfSalesMenu['query'] = dfSalesMenu.apply(lambda row: querySalesMenu.format(
    con=row['con'], 
    dbName = row['dbName'], 
    companyCode = row['companyCode'], 
    startWeek = count_week,
    endWeek = 0
), axis=1)

# %%
len_dfSalesMenu = -(-len(dfSalesMenu) // 1000)
for i in range(len_dfSalesMenu):
    start_idx = i * 1000
    end_idx = (i + 1) * 1000
    sub_df = dfSalesMenu[start_idx:end_idx]
    print("Processing Part {prt}".format(prt=i+1))

    dfResultUpdatedMenuSteroid = extract_data_parallel(
        dfListDB=sub_df,
        cons=cons,
        max_workers=10,
        batch_size=20
    )

    dwh_steroid = Connections.dwh('esb_devMenuNewsletter')
    upsertDBv2(dfResultUpdatedMenuSteroid, 'fact_menurelated', dwh_steroid)
    dfResultUpdatedMenuSteroid = pd.DataFrame()


