# eurostat_households_shares_2025.py
# Requirements: python -m pip install pandas requests python-dateutil

import io, csv, sys, gzip, requests, pandas as pd
from datetime import datetime

EU15_WITH_UK = ["AT","BE","DE","DK","ES","FI","FR","EL","IE","IT","LU","NL","PT","SE","UK"]
EU15_NO_UK   = ["AT","BE","DE","DK","ES","FI","FR","EL","IE","IT","LU","NL","PT","SE"]

BASE = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/NASQ_10_F_BS"
KEY  = "Q.MIO_EUR.S14.ASS.F+F2+F3+F5"
def fetch(geos):
    url = f"{BASE}/{KEY}.{'+'.join(geos)}?format=SDMX-CSV&compressed=false"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return pd.read_csv(io.StringIO(r.text))

def latest_2025_q(df):
    # keep only 2025 quarters; pick the max chronological
    d = df[df["TIME_PERIOD"].astype(str).str.startswith("2025-")]
    if d.empty:
        raise SystemExit("V datasetu zatím nejsou kvartály roku 2025 pro zadané GEO.")
    # sort by quarter order
    order = {"Q1":1,"Q2":2,"Q3":3,"Q4":4}
    d["QORD"] = d["TIME_PERIOD"].str[-2:].map(order)
    qmax = d["QORD"].max()
    sel = d[d["QORD"]==qmax]
    return sel, sel["TIME_PERIOD"].iloc[0]

def pivot_vals(df):
    # rows: (na_item, geo), cols: time
    keep = df[["na_item","geo","TIME_PERIOD","OBS_VALUE"]].copy()
    keep["OBS_VALUE"] = pd.to_numeric(keep["OBS_VALUE"], errors="coerce")
    return keep

def compute_shares(block, label):
    # block: rows for a set of geos at one quarter
    agg = block.groupby(["na_item"], as_index=False)["OBS_VALUE"].sum()
    F  = float(agg.loc[agg["na_item"]=="F","OBS_VALUE"])
    F2 = float(agg.loc[agg["na_item"]=="F2","OBS_VALUE"])
    F3 = float(agg.loc[agg["na_item"]=="F3","OBS_VALUE"])
    F5 = float(agg.loc[agg["na_item"]=="F5","OBS_VALUE"])
    dep = 100*F2/F
    cap = 100*(F3+F5)/F
    return pd.DataFrame([{"area":label,"AF2_deposits_pct":dep,"AF3_5_cap_market_pct":cap,"check_sum_pct":dep+cap}])

# --- CZ ---
cz = fetch(["CZ"])
cz25, q_cz = latest_2025_q(cz)
cz_keep = pivot_vals(cz25)
F  = float(cz_keep.loc[cz_keep["na_item"]=="F","OBS_VALUE"])
F2 = float(cz_keep.loc[cz_keep["na_item"]=="F2","OBS_VALUE"])
F3 = float(cz_keep.loc[cz_keep["na_item"]=="F3","OBS_VALUE"])
F5 = float(cz_keep.loc[cz_keep["na_item"]=="F5","OBS_VALUE"])
row_cz = pd.DataFrame([{
    "area":"CZ",
    "quarter": q_cz.replace("2025-","2025"),
    "AF2_deposits_pct": 100*F2/F,
    "AF3_5_cap_market_pct": 100*(F3+F5)/F
}])

# --- EU-15 incl. UK ---
eu15u = fetch(EU15_WITH_UK)
eu15u_25, q_eu = latest_2025_q(eu15u)
eu15u_keep = pivot_vals(eu15u_25)
row_eu15u = compute_shares(eu15u_keep, "EU-15 (vč. UK)")
row_eu15u["quarter"] = q_eu.replace("2025-","2025")

# --- EU-15 bez UK ---
eu15 = fetch(EU15_NO_UK)
eu15_25, q_eu2 = latest_2025_q(eu15)
eu15_keep = pivot_vals(eu15_25)
row_eu15 = compute_shares(eu15_keep, "EU-15 (bez UK)")
row_eu15["quarter"] = q_eu2.replace("2025-","2025")

# --- Výstupy ---
out_tab = pd.concat([row_cz, row_eu15u[["area","quarter","AF2_deposits_pct","AF3_5_cap_market_pct"]],
                     row_eu15[["area","quarter","AF2_deposits_pct","AF3_5_cap_market_pct"]]], ignore_index=True)

# Zaokrouhlení na 1 desetinné místo pro reporting
out_tab["AF2_deposits_pct"] = out_tab["AF2_deposits_pct"].round(1)
out_tab["AF3_5_cap_market_pct"] = out_tab["AF3_5_cap_market_pct"].round(1)

# Uložení finální tabulky + zdrojových hodnot pro audit
out_tab.to_csv("households_shares_2025Q_latest.csv", index=False, encoding="utf-8")
pd.concat([cz_keep.assign(area="CZ", quarter=q_cz),
           eu15u_keep.assign(area="EU-15 (vč. UK)", quarter=q_eu),
           eu15_keep.assign(area="EU-15 (bez UK)", quarter=q_eu2)]
         ).to_csv("households_sources_2025Q_latest.csv", index=False, encoding="utf-8")

print("OK -> households_shares_2025Q_latest.csv ; households_sources_2025Q_latest.csv")
