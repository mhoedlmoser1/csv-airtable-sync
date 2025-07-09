#sync_airtable.py
# !/usr/bin/env python3
"""


A one‐shot script to sync your CSV files (stock & product) with an Airtable base.
Designed for local testing; easy to refactor into a Cloud Run function later.
"""

import os
import sys
import csv
import math
import logging
import requests
from airtable import Airtable
import time
from itertools import islice

# ─── Configuration ─────────────────────────────────────────────────────────────

API_KEY         = os.environ.get('API_KEY')
BASE_ID         = os.environ.get('BASE_ID')
TABLE_NAME      = "productSetTEST"
STOCK_CSV_URL   = "https://assets.drinkshipping.eu/feeds/H-000000000005/0110d52d-aa4d-4197-96a7-0cef2901322b/stock.csv"
PRODUCT_CSV_URL = "https://assets.drinkshipping.eu/feeds/H-000000000005/0110d52d-aa4d-4197-96a7-0cef2901322b/productdata.csv"

# Fields you never want overwritten in Airtable
EXCLUDE_FIELDS = [
    # e.g. 'Foto Flasche', 'Manual Notes'
    "Land",
    "Rebsorten_label",
    "metafield.custom.rebsorte"
]

# Map CSV column → Airtable field
CSV_TO_AIRTABLE_FIELD_MAP = {
    "sku": "variants.sku",
    "Produktname lang": "title",
    "Kategorie": "Kategorie",
    "Alkoholgehalt in %": "metafield.custom.alkoholgehalt",
    "Allergene": "metafield.custom.allergene",
    "Aromen": "Aroma",
    "Artikelbeschreibung lang": "descriptionHtml",
    "Bio": "metafield.custom.bio",
    "Cuvee": "metafield.custom.cuvee",
    "Cuveebestand": "Cuveebestand",
    "EAN": "variants.barcode",
    "Einheit": "Einheit",
    "Einwegpfand": "Einwegpfand",
    "Foodpairing": "Foodpairing",
    "Foto Flasche": "Foto Flasche",
    "Foto Geschenkkarton": "Foto Geschenkkarton",
    "Foto Verkaufseinheit": "Foto Verkaufseinheit",
    "Gebinde": "Gebinde",
    "Gebindeart Einweg/Mehrweg": "metafield.custom.gebindegr_e",
    "Gebindegröße": "Gebindegröße",
    "Im Sortiment seit": "Im Sortiment seit",
    "Inhaltsangabe": "Inhaltsangabe",
    "Jahrgang": "Jahrgang",
    "Jahrgangskennnummer": "metafield.custom.jahrgangskennung",
    "Klassifizierung": "Klassifizierung",
    "Land": "Land",
    "Marke_code": "Marke_code",
    "Marke_label": "vendor",
    "Menge in Liter": "metafield.custom.mengeinliter",
    "Produktname kurz": "Produktname kurz",
    "Rarität": "metafield.custom.rarit_t",
    "Rebsorten_code": "Rebsorten_code",
    "Rebsorten_label": "metafield.custom.rebsorte",
    "Region_code": "Region_code",
    "Region_label": "metafield.custom.region",
    "Subregion_code": "Subregion_code",
    "Subregion_label": "metafield.custom.subregion",
    "Überkarton": "metafield.custom._berkarton",
    "Verschlussart": "metafield.custom.verschluss",
    "Weinfarbe": "metafield.custom.weinfarbe",
    "Weingeschmack": "metafield.custom.rests_e",
    # from stock.csv, renamed here to trigger compute_price:
    "Einkaufspreis netto": "variants.price",
}

# Normalize mapping keys for lookups against lowercase CSV headers:
CSV_FIELD_MAP_LOWER = {
    k.strip().lower(): v for k, v in CSV_TO_AIRTABLE_FIELD_MAP.items()
}
SKU_CSV_FIELD = "sku"
SKU_AT_FIELD = CSV_FIELD_MAP_LOWER[SKU_CSV_FIELD]

# ─── Helper Functions ──────────────────────────────────────────────────────────


def compute_price(data: dict) -> float:
    """
    1) Compute pre‐tax markup:
       - cost < 6       : divide by 0.60
       - 6 ≤ cost ≤ 11  : divide by 0.72
       - cost > 11      : divide by 0.75
      Then floor, add .45 if frac ≤ .69 else .89.

    2) Apply 20% tax.

    3) On the taxed result, floor again and
       add .45 if its frac ≤ .69 else .89,
       so final decimals are always .45 or .89.
    """
    # pull & normalize your CSV value (lowercase header)
    raw_cost = data.get("einkaufspreis netto", "") or "0"
    cost = float(raw_cost.replace(",", "."))


def calculate_net_price(cost: float) -> float:
    """
    Given a net cost, applies your tiered markup and
    rounds up to the nearest .45/.89, *before* tax.
    """
    if cost < 6:
        div = 0.60
    elif cost <= 11:
        div = 0.72
    else:
        div = 0.75

    raw = cost / div
    base = math.floor(raw)
    frac = raw - base
    add = 0.45 if frac <= 0.69 else 0.89
    return base + add


def compute_price_breakdown(data: dict) -> dict[str, float]:
    """
    Reads 'einkaufspreis netto' from `data`, and returns:
      - price_pre_tax   (float): net sale-price before tax
      - final_price     (float): with 20% VAT, ending .45/.89
      - margin_pre_tax  (float): (final_price / 1.2) - cost
    """
    # 1) Parse your raw cost (normalize commas to dots)
    raw_cost = data.get("einkaufspreis netto", "") or "0"
    cost = float(raw_cost.replace(",", "."))

    # 2) Compute net (pre-tax) price
    price_pre_tax = calculate_net_price(cost)

    # 3) Compute margin as what you actually keep net of VAT
    #    i.e. back out the 20% VAT from the final price, then subtract cost
    taxed_raw = price_pre_tax * 1.2
    base2 = math.floor(taxed_raw)
    frac2 = taxed_raw - base2
    add2 = 0.45 if frac2 <= 0.69 else 0.89
    final_price = base2 + add2

    margin_pre_tax = (final_price / 1.2) - cost

    # 4) Apply 20% tax, then round to .45/.89 again
    taxed_raw = price_pre_tax * 1.2
    base2 = math.floor(taxed_raw)
    frac2 = taxed_raw - base2
    add2 = 0.45 if frac2 <= 0.69 else 0.89
    final_price = base2 + add2

    return {
        "price_pre_tax": round(price_pre_tax, 2),
        "final_price": round(final_price, 2),
        "margin_pre_tax": round(margin_pre_tax, 2),
    }


def fetch_csv_rows(url: str) -> list[dict]:
    """
    GETs a semicolon-delimited CSV from `url` and returns a list of row-dicts
    whose keys are all lowercased and whitespace-stripped.
    """
    resp = requests.get(url)
    resp.raise_for_status()

    lines = resp.text.splitlines()
    reader = csv.DictReader(lines, delimiter=";")

    normalized = []
    for raw in reader:
        row = {k.strip().lower(): v for k, v in raw.items()}
        if row.get(SKU_CSV_FIELD):
            normalized.append(row)
    return normalized


def merge_csv_data(stock: list[dict], products: list[dict]) -> dict[str, dict]:
    prod_map = {r[SKU_CSV_FIELD]: r for r in products if SKU_CSV_FIELD in r}
    stock_map = {r[SKU_CSV_FIELD]: r for r in stock if SKU_CSV_FIELD in r}
    merged = {}
    for sku, prod in prod_map.items():
        merged[sku] = {**prod, **stock_map.get(sku, {})}
    return merged


def fetch_airtable_records(client: Airtable) -> dict[str, dict]:
    recs = client.get_all()
    return {r["fields"][SKU_AT_FIELD]: r for r in recs if SKU_AT_FIELD in r["fields"]}


def build_fields(data: dict, include_excluded: bool = False) -> dict:
    """
    Build Airtable payload:
      - Map CSV fields → Airtable fields
      - Compute price for 'einkaufspreis netto'
      - Cast values to str
      - Skip EXCLUDE_FIELDS
    """
    out = {}
    breakdown = compute_price_breakdown(data)

    # 1) Map & compute existing CSV→Airtable fields…
    for csv_col, at_field in CSV_FIELD_MAP_LOWER.items():
        if not include_excluded and at_field in EXCLUDE_FIELDS:
            continue

        if csv_col == "einkaufspreis netto":
            # shove the final, taxed price into variants.price
            val = breakdown["final_price"]
        else:
            val = data.get(csv_col)

        if val not in (None, ""):
            out[at_field] = str(val)

    # 2) Raw cost
    raw = data.get("einkaufspreis netto")
    if raw not in (None, ""):
        out["Einkaufspreis netto"] = raw

    # 3) Your net margin (pre-tax) into the price.margin field
    out["price.margin"] = str(breakdown["margin_pre_tax"])

    return out


# ─── Batched Airtable Client ─────────────────────────────────────────────────


class BatchedAirtable(Airtable):
    """
    Subclass of Airtable that supports batched create/update/delete
    with built-in rate limiting.
    """

    RATE_LIMIT_DELAY = 0.2  # seconds between requests (5 req/sec)
    BATCH_SIZE = 10  # max records per batch

    def _base_url(self) -> str:
        return f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}"

    def _chunks(self, iterable):
        it = iter(iterable)
        while True:
            batch = list(islice(it, self.BATCH_SIZE))
            if not batch:
                return
            yield batch

    def batch_update(self, records: list[dict]):
        for batch in self._chunks(records):
            payload = {"records": batch}
            self.session.patch(self._base_url(), json=payload)
            time.sleep(self.RATE_LIMIT_DELAY)

    def batch_create(self, records: list[dict]):
        for batch in self._chunks(records):
            payload = {"records": batch}
            self.session.post(self._base_url(), json=payload)
            time.sleep(self.RATE_LIMIT_DELAY)

    def batch_delete(self, ids: list[str]):
        for batch in self._chunks(ids):
            params = [("records[]", rid) for rid in batch]
            self.session.delete(self._base_url(), params=params)
            time.sleep(self.RATE_LIMIT_DELAY)


# ─── Main ─────────────────────────────────────────────────────────────────────


def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    # Validate env vars
    for var in ("API_KEY", "BASE_ID", "TABLE_NAME", "STOCK_CSV_URL", "PRODUCT_CSV_URL"):
        if not globals().get(var):
            logging.error(f"Missing required env var: {var}")
            sys.exit(1)

    client = BatchedAirtable(BASE_ID, TABLE_NAME, API_KEY)

    # Load & merge CSVs
    stock_rows = fetch_csv_rows(STOCK_CSV_URL)
    product_rows = fetch_csv_rows(PRODUCT_CSV_URL)
    merged = merge_csv_data(stock_rows, product_rows)
    logging.info(f"Merged {len(merged)} SKUs from CSVs")

    # Fetch Airtable
    airtbl = fetch_airtable_records(client)
    logging.info(f"Fetched {len(airtbl)} SKUs from Airtable")

    # Determine actions
    skus_csv = set(merged)
    skus_at = set(airtbl)
    to_update = skus_csv & skus_at
    to_create = skus_csv - skus_at
    to_delete = skus_at - skus_csv

    logging.info(
        f"Batches → Update: {len(to_update)}, Create: {len(to_create)}, Delete: {len(to_delete)}"
    )

    # Prepare payloads
    updates, creations, deletions = [], [], []

    for sku in to_update:
        rec = airtbl[sku]
        fields = build_fields(merged[sku])
        if fields:
            updates.append({"id": rec["id"], "fields": fields})

    for sku in to_create:
        fields = build_fields(merged[sku], include_excluded=True)
        fields[SKU_AT_FIELD] = sku
        creations.append({"fields": fields})

    for sku in to_delete:
        deletions.append(airtbl[sku]["id"])

    # Execute batches
    client.batch_update(updates)
    client.batch_create(creations)
    client.batch_delete(deletions)

    logging.info("Sync completed successfully.")


if __name__ == "__main__":
    main()

