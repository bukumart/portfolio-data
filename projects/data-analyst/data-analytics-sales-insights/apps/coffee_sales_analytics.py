import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from io import StringIO

from pathlib import Path
import sys

# cari parent yang punya folder 'scripts' (naik sampai root)
p = Path.cwd().resolve()
for parent in [p] + list(p.parents):
    if (parent / "scripts").is_dir():
        project_root = parent
        break
else:
    raise RuntimeError("Tidak menemukan folder 'scripts' di parent path")

# tambahkan ke sys.path (paling depan agar prioritas import)
sys.path.insert(0, str(project_root))
print("Added to sys.path:", project_root)

from scripts.data_utils import load_data, preprocess_data

st.set_page_config(page_title="Sales Overview", layout="wide")

# ------------------------- UI: Sidebar -------------------------
st.sidebar.title("Data & Filters")
df = load_data()

# preprocess
df = preprocess_data(df)

# date range filter
min_date = df['Date'].min()
max_date = df['Date'].max()
start_date, end_date = st.sidebar.date_input("Date range", value=(min_date, max_date), min_value=min_date, max_value=max_date)

# product filter
products = sorted(df['Menu Name'].unique().tolist())
selected_products = st.sidebar.multiselect("Menu Name", options=products, default=products)

# payment filter
payment_types = sorted(df['Payment Type'].unique().tolist())
selected_payments = st.sidebar.multiselect("Payment types", options=payment_types, default=payment_types)

# apply filters
mask = (
    (df['Date'] >= start_date) &
    (df['Date'] <= end_date) &
    (df['Menu Name'].isin(selected_products)) &
    (df['Payment Type'].isin(selected_payments))
)
df_filtered = df.loc[mask].copy()

# ------------------------- Top KPIs -------------------------
st.title("Sales Overview Dashboard ☕")

kpi1, kpi2, kpi3 = st.columns(3)
with kpi1:
    total_revenue = df_filtered['Payment Total'].sum()
    st.subheader("Total Revenue")
    st.metric(label="Rp (approx)", value=f"{total_revenue:,.2f}")
with kpi2:
    total_tx = len(df_filtered)
    st.subheader("Total Transactions")
    st.metric(label="Transactions", value=total_tx)
with kpi3:
    aov = df_filtered['Payment Total'].mean() if total_tx>0 else 0
    st.subheader("Average Order Value")
    st.metric(label="Rp (avg)", value=f"{aov:,.2f}")

# ------------------------- Charts -------------------------
st.markdown("---")
col1, col2 = st.columns((2,1))

# left: daily revenue and top products
with col1:
    st.subheader("Daily Revenue")
    if df_filtered.empty:
        st.info("No data for selected filters")
    else:
        daily = df_filtered.groupby('Date', as_index=False)['Payment Total'].sum()
        fig_daily = px.line(daily, x='Date', y='Payment Total', markers=True, title='Daily Revenue')
        fig_daily.update_yaxes(tickformat=",.0f", separatethousands=True, hoverformat=",.2f")
        fig_daily.update_layout(xaxis_title='Date', yaxis_title='Revenue')
        st.plotly_chart(fig_daily, use_container_width=True)

    st.subheader("Transactions per Product")
    prod = df_filtered.groupby('Menu Name', as_index=False).agg({'Payment Total':'sum','datetime':'count'}).rename(columns={'datetime':'Transactions'})
    prod = prod.sort_values('Transactions', ascending=False)
    fig_prod = px.bar(prod, x='Menu Name', y='Transactions', title='Transactions per Product')
    st.plotly_chart(fig_prod, use_container_width=True)
    prod = prod.reset_index(drop=True)
    prod.index = prod.index + 1
    prod["Payment Total"] = prod["Payment Total"].apply(lambda x: f"{x:,.2f}")
    st.dataframe(prod)

# right: payment breakdown & hour of day
with col2:
    st.subheader("Payment Type Breakdown")
    if not df_filtered.empty:
        pay = df_filtered.groupby('Payment Type', as_index=False)['Payment Total'].sum()
        fig_pay = px.pie(pay, names='Payment Type', values='Payment Total', title='Revenue by Payment Type')
        st.plotly_chart(fig_pay, use_container_width=True)
    
    st.subheader("Transactions by Hour")
    hour = df_filtered.groupby('Hour', as_index=False).size().rename(columns={'size':'Transactions'}) if not df_filtered.empty else pd.DataFrame({'Hour':[], 'Transactions':[]})
    fig_hour = px.bar(hour, x='Hour', y='Transactions', title='Transactions by Hour')
    fig_hour.update_layout(xaxis_title='Hour of day')
    st.plotly_chart(fig_hour, use_container_width=True)

st.markdown("---")

# Heatmap of busiest hour x day
st.subheader("Heatmap: Day of Week vs Hour")
if not df_filtered.empty:
    pivot = df_filtered.pivot_table(index='Day', columns='Hour', values='Payment Total', aggfunc='count', fill_value=0)
    # reorder days
    days_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    pivot = pivot.reindex(days_order).fillna(0)
    fig_heat = px.imshow(pivot, labels=dict(x="Hour", y="Day of Week", color="Transactions"), aspect="auto", title='Transactions heatmap')
    st.plotly_chart(fig_heat, use_container_width=True)
else:
    st.info("No data to show heatmap for selected filters")

st.markdown("---")

# Raw data and download
with st.expander("Show filtered raw data"):
    st.dataframe(df_filtered)
    # CSV download
    csv = df_filtered.to_csv(index=False)
    st.download_button("Download CSV", data=csv, file_name="filtered_sales.csv", mime='text/csv')

st.markdown("---")