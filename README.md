# Vklady a investice – S.14 (domácnosti): podíly v AF.2 a (AF.3+AF.5), 2025Qx

Repo stahuje data z **Eurostat – `nasq_10_f_bs` (Financial balance sheets, quarterly)**,
vybere nejnovější kvartál 2025, agreguje **EU-15 (včetně i bez UK)** a spočítá podíly:

- CZ – AF.2 / F
- CZ – (AF.3 + AF.5) / F
- EU-15 – AF.2 / F
- EU-15 – (AF.3 + AF.5) / F

## Jak spustit přes GitHub Actions
1. Otevři záložku **Actions**.
2. Vyber workflow **Fetch Eurostat & Build CSV**.
3. Klikni **Run workflow** a počkej na dokončení (zelená fajfka).

### Výstupní soubory
- `households_shares_2025Q_latest.csv` – finální tabulka (4 hodnoty v %, u každé kvartál).
- `households_sources_2025Q_latest.csv` – zdrojové nominály (MIO_EUR) a mezivýpočty.

## Metodika (stručně)
- Dataset: Eurostat `nasq_10_f_bs` (ESA 2010), `freq=Q`, `unit=MIO_EUR`, `sector=S14`, `finpos=ASS`,
  `na_item ∈ {F, F2, F3, F5}` (F = Total financial assets).
- EU-15 agregace: nejprve **sečíst nominály v EUR** napříč zeměmi, až pak podíly. Verze vč./bez UK.
- Kontrola: `AF.2 + (AF.3+AF.5) ≤ 100 %` – zbytek tvoří AF.1, AF.4, AF.6, AF.7, AF.8.
