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
from scripts.functions import upsertDBv2, run_parallel
from scripts.db import Connections

QUERY_DIR = project_root / "query"

# %%
dwh_menu = Connections.dwh('esb_devMenuNewsletter')

# %%
queryGetUniqueCompany = (QUERY_DIR / "queryGetUniqueCompany.sql").read_text()
queryMenuRelated = (QUERY_DIR / "queryMenuRelated.sql").read_text()
queryTruncate = (QUERY_DIR / "queryTruncate.sql").read_text()

# %%
df_uniqueCompany = pd.read_sql(con=dwh_menu, sql=queryGetUniqueCompany)

# %%
df_uniqueCompany['query'] = df_uniqueCompany.apply(lambda row: queryMenuRelated.format(
    dbName = row['dbName']
), axis=1)

# %%
dwh_menu.execute(queryTruncate)

# %%
len_df_uniqueCompany = -(-len(df_uniqueCompany) // 400)
for i in range(len_df_uniqueCompany):
    start_idx = i * 400
    end_idx = (i + 1) * 400
    sub_df = df_uniqueCompany[start_idx:end_idx]
    print("Processing Part {prt}".format(prt=i+1))

    df_menuPair = run_parallel(sub_df, batch_size=10)

    dwh_menu = Connections.dwh('esb_devMenuNewsletter')
    upsertDBv2(df_menuPair, 'fact_topmenurelated', dwh_menu)
    df_menuPair = pd.DataFrame()


