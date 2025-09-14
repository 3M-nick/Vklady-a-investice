# eurostat_households_shares_2025.py
# Běží v GitHub Actions (Python 3.11).
# Výstupy:
#   1) households_shares_2025Q_latest.csv         – finální % (CZ, EU-15 vč./bez UK)
#   2) households_sources_2025Q_latest.csv        – zdrojové nominály v MIO_EUR (včetně F51/F52)
#   3) households_af5_breakdown_2025Q_latest.csv  – rozpad AF.5 (F51/F52) jako % z AF.5 i z F

import io
import requests
import pandas as pd

EU15_WITH_UK = ["AT","BE","DE","DK","ES","FI","FR","EL","IE","IT","LU","NL","PT","SE","UK"]
EU15_NO_UK   = ["AT","BE","DE","DK","ES","FI","FR","EL","IE","IT","LU","NL","PT","SE"]

BASE = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/NASQ_10_F_BS"
# ✳️ Přidán rozpad AF.5 -> F51 (equity) a F52 (investment fund shares/units)
KEY  = "Q.MIO_EUR.S14.ASS.F+F2+F3+F5+F51+F52"

def fetch(geos):
    url = f"{BASE}/{KEY}.{'+'.join(geos)}?format=SDMX-CSV&compressed=false"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    df.columns = [c.strip().upper() for c in df.columns]  # normalize col names
    # Ujisti se, že OBS_VALUE je numerické
    if "OBS_VALUE" in df.columns:
        df["OBS_VALUE"] = pd.to_numeric(df["OBS_VALUE"], errors="coerce")
    return df

def latest_2025_q(df):
    # vyber jen rok 2025 a najdi nejnovější dostupný kvartál
    d = df[df["TIME_PERIOD"].astype(str).str.startswith("2025-")].copy()
    if d.empty:
        raise SystemExit("V datasetu zatím nejsou kvartály roku 2025 pro zadané GEO.")
    order = {"Q1":1,"Q2":2,"Q3":3,"Q4":4}
    d["QORD"] = d["TIME_PERIOD"].str[-2:].map(order)
    qmax = d["QORD"].max()
    sel = d[d["QORD"]==qmax].copy()
    return sel, sel["TIME_PERIOD"].iloc[0]

def shares_from_block(block, label):
    # block = řádky pro 1 kvartál, víc GEO; spočte agregáty a % pro AF.2 a (AF.3+AF.5)
    keep = block[["NA_ITEM","GEO","TIME_PERIOD","OBS_VALUE"]].copy()
    agg = keep.groupby(["NA_ITEM"], as_index=False)["OBS_VALUE"].sum()

    def g(code):
        v = agg.loc[agg["NA_ITEM"]==code,"OBS_VALUE"]
        return float(v.iloc[0]) if not v.empty else 0.0

    F   = g("F")
    F2  = g("F2")
    F3  = g("F3")
    F5  = g("F5")
    F51 = g("F51")
    F52 = g("F52")

    # hlavní podíly
    dep_pct = 100 * F2 / F if F else 0.0
    cap_pct = 100 * (F3 + F5) / F if F else 0.0

    # rozpad AF.5
    # podíly z AF.5
    f51_of_f5_pct = 100 * F51 / F5 if F5 else 0.0
    f52_of_f5_pct = 100 * F52 / F5 if F5 else 0.0
    # podíly z F (celkových aktiv)
    f51_of_f_pct = 100 * F51 / F if F else 0.0
    f52_of_f_pct = 100 * F52 / F if F else 0.0

    main = pd.DataFrame([{
        "area": label,
        "quarter": keep["TIME_PERIOD"].iloc[0].replace("2025-","2025"),
        "AF2_deposits_pct": round(dep_pct, 1),
        "AF3_5_cap_market_pct": round(cap_pct, 1),
        "check_sum_pct": round(dep_pct + cap_pct, 1)
    }])

    breakdown = pd.DataFrame([{
        "area": label,
        "quarter": keep["TIME_PERIOD"].iloc[0].replace("2025-","2025"),
        "F51_equity_of_AF5_pct": round(f51_of_f5_pct, 1),
        "F52_funds_of_AF5_pct": round(f52_of_f5_pct, 1),
        "F51_equity_of_F_pct": round(f51_of_f_pct, 1),
        "F52_funds_of_F_pct": round(f52_of_f_pct, 1)
    }])

    return main, keep, breakdown

# --- CZ ---
cz_all = fetch(["CZ"])
cz_2025, _ = latest_2025_q(cz_all)
row_cz, src_cz, br_cz = shares_from_block(cz_2025, "CZ")

# --- EU-15 včetně UK ---
eu15u_all = fetch(EU15_WITH_UK)
eu15u_2025, _ = latest_2025_q(eu15u_all)
row_eu15u, src_eu15u, br_eu15u = shares_from_block(eu15u_2025, "EU-15 (vč. UK)")

# --- EU-15 bez UK ---
eu15_all = fetch(EU15_NO_UK)
eu15_2025, _ = latest_2025_q(eu15_all)
row_eu15, src_eu15, br_eu15 = shares_from_block(eu15_2025, "EU-15 (bez UK)")

# --- Výstupy ---
# 1) Přehledová tabulka s hlavními podíly
out_main = pd.concat([row_cz, row_eu15u, row_eu15], ignore_index=True)
out_main.to_csv("households_shares_2025Q_latest.csv", index=False, encoding="utf-8")

# 2) Zdrojové nominály (audit) – nyní včetně F51/F52
out_sources = pd.concat([
    src_cz.assign(area="CZ"),
    src_eu15u.assign(area="EU-15 (vč. UK)"),
    src_eu15.assign(area="EU-15 (bez UK)")
], ignore_index=True)
out_sources.to_csv("households_sources_2025Q_latest.csv", index=False, encoding="utf-8")

# 3) Breakdown AF.5 (F51/F52)
out_br = pd.concat([br_cz, br_eu15u, br_eu15], ignore_index=True)
out_br.to_csv("households_af5_breakdown_2025Q_latest.csv", index=False, encoding="utf-8")

# Konzolový náhled
print("=== Main shares ===")
print(out_main.to_string(index=False))
print("\n=== AF.5 breakdown ===")
print(out_br.to_string(index=False))
print("\nOK -> households_shares_2025Q_latest.csv ; households_sources_2025Q_latest.csv ; households_af5_breakdown_2025Q_latest.csv")
