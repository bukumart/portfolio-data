# scripts/data_utils.py
import pandas as pd
from pathlib import Path

def load_data():
    base_dir = Path(__file__).resolve().parent.parent
    data_dir = base_dir / "datasets"

    df1 = pd.read_csv(data_dir / "index_1.csv")
    df2 = pd.read_csv(data_dir / "index_2.csv")

    df1['datetime'] = pd.to_datetime(df1['datetime'])
    df2['datetime'] = pd.to_datetime(df2['datetime'])
    df1['date'] = pd.to_datetime(df1['date']).dt.date
    df2['date'] = pd.to_datetime(df2['date']).dt.date

    df = pd.concat([df1, df2], ignore_index=True)
    return df


def preprocess_data(df: pd.DataFrame):
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]

    if 'datetime' in df.columns:
        df['datetime'] = pd.to_datetime(df['datetime'])
    elif 'date' in df.columns:
        df['datetime'] = pd.to_datetime(df['date'])

    # df['date_only'] = df['datetime'].dt.date
    df['hour'] = df['datetime'].dt.hour
    df['dayofweek'] = df['datetime'].dt.day_name()

    if 'money' in df.columns:
        df['money'] = pd.to_numeric(df['money'], errors='coerce').fillna(0)
    else:
        df['money'] = 0

    # Convertion from UAH to IDR
    rate = 397.88
    df["money_idr"] = df["money"] * rate
    df["money_idr"] = df["money_idr"].round(2)

    df = df.rename(columns={
        "date": "Date",
        "cash_type": "Payment Type",
        "money_idr": "Payment Total",
        "coffee_name": "Menu Name",
        "hour": "Hour",
        "dayofweek": "Day"
    })

    return df.drop(columns=["money"])
