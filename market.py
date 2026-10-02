from contextlib import contextmanager
import os
import re
import json
import io
import hashlib
import csv
import requests
from functools import lru_cache
from pathlib import Path
from dotenv import load_dotenv
from pypdf import PdfReader
import numpy as np
import pandas as pd

SOURCE_DIR = Path(str(Path(__file__).resolve().parent / "data/source"))
OUTPUT_DIR = Path(str(SOURCE_DIR.parent / "processed"))
CACHE_DIR = Path(str(SOURCE_DIR / ".cache"))
YEARS = range(2007, 2025)
BAY_COUNTY_FIPS = {
    "Alameda": "06001",
    "Contra Costa": "06013",
    "Marin": "06041",
    "Napa": "06055",
    "San Francisco": "06075",
    "San Mateo": "06081",
    "Santa Clara": "06085",
    "Solano": "06095",
    "Sonoma": "06097",
}
STOCK = {
    "household_population": "B25008_001E",
    "housing_units": "B25001_001E",
    "occupied_housing_units": "B25002_002E",
    "vacant_housing_units": "B25002_003E",
    "owner_households": "B25003_002E",
    "renter_households": "B25003_003E",
}
QCEW_YEARS = range(2007, 2026)
QCEW_PDF_PATHS = sorted(SOURCE_DIR.glob("BLS_qcew*.pdf"))
QCEW_TYPE_RENAMES = {
    "all_employees": "employment",
    "number_of_establishments": "establishments",
    "total_wages_in_thousands": "total_wages_thousands",
    "average_annual_pay": "average_annual_pay",
}
STOCK.update({f"{k}_moe90": v[:-1] + "M" for k, v in list(STOCK.items())})
SESSION = requests.Session()
load_dotenv(SOURCE_DIR.parent.parent / ".env")
MARKET_CACHE_DIR = CACHE_DIR / "market"
BAY_COUNTIES = list(BAY_COUNTY_FIPS)
FIPS_TO_BAY_COUNTY = {fips: county for county, fips in BAY_COUNTY_FIPS.items()}
ZORI_PATH = Path(str(SOURCE_DIR / "County_zori_uc_sfrcondomfr_sm_month.csv"))


@contextmanager
def atomic_path(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".part")
    try:
        yield temporary
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def write_parquet(frame, name):
    with atomic_path(OUTPUT_DIR / name) as temporary:
        frame.to_parquet(temporary, index=False, compression="zstd")


def download(url, path, *, params=None, force=False):
    if path.exists() and (not force):
        return path
    with atomic_path(path) as temporary:
        with SESSION.get(
            url, params=params, stream=True, timeout=(30, 1800)
        ) as response:
            response.raise_for_status()
            with temporary.open("wb") as file:
                for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                    file.write(chunk)
    return path


def census_json(url, query, path, *, force=False):
    if path.exists() and (not force):
        return json.loads(path.read_text())
    key = os.getenv("CENSUS_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "CENSUS_API_KEY is missing. Run run_scripts.sh or export the key before running this script."
        )
    with SESSION.get(url, params={**query, "key": key}, timeout=120) as response:
        if not response.ok:
            raise RuntimeError(
                f"Census request failed: HTTP {response.status_code} ({url})"
            )
        payload = response.json()
    with atomic_path(path) as temporary:
        temporary.write_text(json.dumps(payload), encoding="utf-8")
    return payload


@lru_cache(maxsize=1)
def load_cpi():
    pieces = [
        chunk.drop_duplicates()
        for chunk in pd.read_csv(
            SOURCE_DIR / "usa_00083.csv", usecols=["YEAR", "CPI99"], chunksize=250000
        )
    ]
    frame = (
        pd.concat(pieces, ignore_index=True)
        .drop_duplicates()
        .rename(columns={"YEAR": "year"})
    )
    frame = frame.loc[frame["year"].isin(YEARS)].copy()
    frame["inflation_to_2024"] = (
        frame["CPI99"] / frame.loc[frame["year"].eq(2024), "CPI99"].iloc[0]
    )
    return frame[["year", "inflation_to_2024"]].sort_values("year")


def build_macro_year():
    rates = build_mortgage_rates()
    annual = rates.groupby("year", as_index=False).agg(
        mortgage_rate_30yr=("mortgage_rate_30yr", "mean"),
        mortgage_rate_observations=("mortgage_rate_30yr", "count"),
    )
    return load_cpi().merge(annual, on="year", how="left", validate="one_to_one")


def build_mortgage_rates():
    rates = fred_series("MORTGAGE30US", "mortgage_rate_30yr")
    rates["year"] = rates["date"].dt.year
    rates["source"] = "FRED_MORTGAGE30US"
    return rates.loc[rates.year.isin(YEARS)].sort_values("date")


def build_hpi_county():
    hpi = pd.read_csv(SOURCE_DIR / "hpi_at_county.csv")
    hpi["county"] = hpi["County"].str.replace(" County", "", regex=False)
    hpi["year"] = pd.to_numeric(hpi["Year"], errors="coerce")
    hpi = hpi.loc[hpi["State"].eq("CA") & hpi["county"].isin(BAY_COUNTIES)].copy()
    hpi["county_fips"] = hpi["county"].map(BAY_COUNTY_FIPS)
    hpi["hpi"] = pd.to_numeric(hpi["HPI with 2000 base"], errors="coerce").where(
        lambda x: x.gt(0)
    )
    hpi["hpi_nominal"] = hpi["hpi"]
    hpi["source"] = "FHFA_county_HPI"
    return hpi[["county_fips", "year", "hpi", "hpi_nominal", "source"]].sort_values(
        ["county_fips", "year"]
    )


def build_acs_county_stock():
    frames = []
    for year in YEARS:
        if year == 2020:
            continue
        frame = census_get(year, {"for": "county:*", "in": "state:06"}, STOCK)
        frame["county_fips"] = frame["state"].astype(str).str.zfill(2) + frame[
            "county"
        ].astype(str).str.zfill(3)
        frame = frame.loc[frame["county_fips"].isin(BAY_COUNTY_FIPS.values())].copy()
        for col in STOCK:
            frame[col] = frame[col].where(frame[col].ge(0))
        frame["year"], frame["acs_product"] = (year, "acs1")
        frames.append(frame[["county_fips", "year", "acs_product", *STOCK]])
    return pd.concat(frames, ignore_index=True)


def build_boe():
    keys = ["Assessment Year From", "Assessment Year To", "County"]
    tables = []
    for path in sorted(SOURCE_DIR.glob("PropTax*.csv")):
        table = pd.read_csv(path)
        table["County"] = table["County"].str.strip().str.removesuffix(" County")
        table = table.loc[table.County.isin(BAY_COUNTIES)].copy()
        table["available_" + path.stem] = True
        tables.append(table)
    if not tables:
        raise FileNotFoundError("No PropTax CSV tables in SOURCE_DIR")
    result = tables[0]
    for table in tables[1:]:
        result = result.merge(table, on=keys, how="outer", validate="one_to_one")
    result = result.rename(
        columns={column: normalize_name(column) for column in result}
    )
    result = result.rename(columns={"multi_family_transfers": "multifamily_transfers"})
    result["county_fips"] = result.county.map(BAY_COUNTY_FIPS)
    for column in [c for c in result if c.startswith("available_")]:
        result[column] = result[column].astype("boolean").fillna(False).astype(bool)
    start = result.assessment_year_from.astype(int)
    result["workload_period_start"] = pd.to_datetime((start - 1).astype(str) + "-07-01")
    result["workload_period_end"] = pd.to_datetime(start.astype(str) + "-06-30")
    result["assessment_period_start"] = pd.to_datetime(start.astype(str) + "-07-01")
    result["assessment_period_end"] = pd.to_datetime(
        result.assessment_year_to.astype(int).astype(str) + "-06-30"
    )
    result["assessment_lien_date"] = pd.to_datetime(start.astype(str) + "-01-01")
    result["period_start"] = result.workload_period_start
    result["period_end"] = result.workload_period_end
    result["reference_period"] = (
        "Table_F_prior_fiscal_year; values_and_parcels_assessment_roll"
    )
    result["source"] = "BOE_PropTax_tables"
    result["transfer_measure"] = (
        "assessor_change_in_ownership_workload_not_market_sales"
    )
    return result.sort_values(["county_fips", "assessment_year_from"])


def build_rent():
    path = ZORI_PATH
    if not path.exists():
        return None
    frame = pd.read_csv(path)
    if "State" in frame:
        frame = frame.loc[frame["State"].eq("CA")].copy()
    if {"StateCodeFIPS", "MunicipalCodeFIPS"}.issubset(frame):
        state = (
            pd.to_numeric(frame["StateCodeFIPS"], errors="coerce")
            .astype("Int64")
            .astype("string")
            .str.zfill(2)
        )
        county = (
            pd.to_numeric(frame["MunicipalCodeFIPS"], errors="coerce")
            .astype("Int64")
            .astype("string")
            .str.zfill(3)
        )
        frame["county_fips"] = state + county
    elif {"RegionName", "State"}.issubset(frame):
        frame["county_fips"] = (
            frame["RegionName"]
            .str.replace(" County", "", regex=False)
            .map(BAY_COUNTY_FIPS)
        )
    else:
        raise ValueError(
            "ZORI input must identify California counties; ZIP/city series require their own support definition"
        )
    frame = frame.loc[frame["county_fips"].isin(BAY_COUNTY_FIPS.values())].copy()
    dates = [c for c in frame if re.fullmatch("\\d{4}-\\d{2}-\\d{2}", c)]
    result = frame[["county_fips", *dates]].melt(
        id_vars="county_fips", var_name="date", value_name="asking_rent_nominal"
    )
    result["date"] = pd.to_datetime(result["date"])
    result["asking_rent_nominal"] = pd.to_numeric(
        result["asking_rent_nominal"], errors="coerce"
    ).where(lambda x: x.gt(0))
    result = result.loc[result["date"].dt.year.isin(YEARS)].copy()
    result["source"] = "ZORI_all_homes_monthly"
    return result.sort_values(["county_fips", "date"])


def build_county_year():
    county = pd.MultiIndex.from_product(
        [BAY_COUNTY_FIPS.values(), YEARS], names=["county_fips", "year"]
    ).to_frame(index=False)
    for frame in [
        build_acs_county_stock(),
        build_hpi_county().drop(columns="source"),
        build_bps_county().drop(columns=["county", "source"]),
    ]:
        county = county.merge(
            frame, on=["county_fips", "year"], how="left", validate="one_to_one"
        )
    county["county"] = county["county_fips"].map(FIPS_TO_BAY_COUNTY)
    county["acs_product"] = county["acs_product"].fillna("not_available")
    return county


def normalize_name(value):
    return re.sub("[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")


def cached_text(url, path):
    return download(url, path).read_text(encoding="utf-8")


def fred_series(series_id, value_name):
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
    text = cached_text(url, MARKET_CACHE_DIR / f"fred_{series_id}.csv")
    frame = pd.read_csv(io.StringIO(text))
    frame = frame.rename(columns={"observation_date": "date", series_id: value_name})
    frame["date"] = pd.to_datetime(frame["date"])
    frame[value_name] = pd.to_numeric(frame[value_name], errors="coerce")
    return frame


def parse_qcew_annual_value(value):
    value = str(value).strip()
    value = re.sub("\\s*\\(P\\)\\s*$", "", value)
    value = value.replace(",", "").replace("$", "").strip()
    if value in {"", "-", "--", "---", "NA", "N/A"}:
        return np.nan
    return pd.to_numeric(value, errors="coerce")


def parse_qcew_pdf(path):
    rows = []
    metadata = {}
    pending_field = None
    in_table = False
    fields = {
        "Series Id": "series_id",
        "Area": "area",
        "Industry": "industry",
        "Owner": "owner",
        "Type": "type",
    }
    reader = PdfReader(path, strict=False)
    for page_number, page in enumerate(reader.pages, start=1):
        text = page.extract_text(extraction_mode="layout") or ""
        lines = text.splitlines()
        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue
            if pending_field is not None:
                if not any((stripped.startswith(f"{label}:") for label in fields)):
                    metadata[pending_field] = stripped
                    pending_field = None
                    continue
                pending_field = None
            metadata_match = re.match(
                "^\\s*(Series Id|Area|Industry|Owner|Type):\\s*(.*?)\\s*$", line
            )
            if metadata_match:
                label, value = metadata_match.groups()
                field = fields[label]
                if field == "series_id":
                    metadata = {}
                    in_table = False
                if value:
                    metadata[field] = value.strip()
                else:
                    pending_field = field
                continue
            if (
                "Year" in stripped
                and re.search("\\bAnnual\\b", stripped, flags=re.IGNORECASE)
                and (
                    {"series_id", "area", "industry", "owner", "type"}
                    <= metadata.keys()
                )
            ):
                in_table = True
                continue
            if not in_table:
                continue
            row_match = re.match(
                "^\\s*(2[ \\t]*0[ \\t]*\\d[ \\t]*\\d)\\s+(.*?)\\s*$", line
            )
            if not row_match:
                continue
            year_text = row_match.group(1)
            digit_gap = max(
                (len(gap) for gap in re.findall("[ \\t]+", year_text)), default=0
            )
            year = int(re.sub("[ \\t]+", "", year_text))
            if year not in QCEW_YEARS:
                continue
            values = re.sub(
                "\\s*\\(P\\)\\s*$", "", row_match.group(2).strip(), flags=re.IGNORECASE
            )
            cells = [
                re.sub("[ \\t]+", "", cell)
                for cell in re.split(f"[ \\t]{{{max(2, digit_gap + 1)},}}", values)
                if cell.strip()
            ]
            if not cells:
                continue
            annual_value = parse_qcew_annual_value(cells[-1])
            rows.append(
                {
                    **metadata,
                    "year": year,
                    "value": annual_value,
                    "source_pdf": path.name,
                    "source_page": page_number,
                }
            )
    return pd.DataFrame(rows)


def build_qcew_panel():
    parsed = pd.concat(
        [parse_qcew_pdf(path) for path in QCEW_PDF_PATHS], ignore_index=True
    )
    parsed["county"] = (
        parsed["area"]
        .str.replace("\\s+County,\\s+California$", "", regex=True)
        .str.strip()
    )
    parsed["county_fips"] = parsed["county"].map(BAY_COUNTY_FIPS)
    parsed["type"] = parsed["type"].map(normalize_name).replace(QCEW_TYPE_RENAMES)
    keys = ["county_fips", "county", "year", "owner", "industry"]
    panel = parsed.pivot(index=keys, columns="type", values="value").reset_index()
    panel.columns.name = None
    panel["total_wages"] = panel["total_wages_thousands"] * 1000
    return panel.sort_values(["county_fips", "year", "owner", "industry"])


def build_qcew():
    result = build_qcew_panel().merge(
        load_cpi(), on="year", how="left", validate="many_to_one"
    )
    result["ownership"] = result["owner"].map(normalize_name)
    result["sector"] = result["industry"].map(normalize_name)
    result["is_total"] = result["sector"].eq("total_all_industries")
    result["source"] = "BLS_QCEW_publication_series_PDF"
    result["total_wages_2024"] = result["total_wages"] * result["inflation_to_2024"]
    result["average_annual_pay_2024"] = (
        result["average_annual_pay"] * result["inflation_to_2024"]
    )
    keys = ["county_fips", "year", "ownership"]
    totals = result.loc[
        result["is_total"], keys + ["employment", "total_wages"]
    ].rename(
        columns={"employment": "ownership_employment", "total_wages": "ownership_wages"}
    )
    result = result.merge(totals, on=keys, how="left", validate="many_to_one")
    result["employment_share"] = (
        result["employment"] / result["ownership_employment"]
    ).where(~result["is_total"])
    result["payroll_share"] = (result["total_wages"] / result["ownership_wages"]).where(
        ~result["is_total"]
    )
    sectors = (
        result.loc[~result["is_total"]]
        .groupby(keys)[["employment", "total_wages"]]
        .sum(min_count=1)
        .add_prefix("reported_sector_")
        .reset_index()
    )
    result = result.merge(sectors, on=keys, how="left", validate="many_to_one")
    result["unallocated_employment"] = (
        result["ownership_employment"] - result["reported_sector_employment"]
    )
    result["unallocated_wages"] = (
        result["ownership_wages"] - result["reported_sector_total_wages"]
    )
    return result.sort_values(["county_fips", "year", "ownership", "sector"])


def bps_column_names(text, metadata_columns):
    lines = text.splitlines()
    top = next(csv.reader([lines[0]]))
    bottom = next(csv.reader([lines[1]]))
    top += [""] * (len(bottom) - len(top))
    names = [normalize_name(f"{top[i]}_{bottom[i]}") for i in range(metadata_columns)]
    for start in range(metadata_columns, len(bottom), 3):
        group_top = top[start : start + 3]
        group_bottom = bottom[start : start + 3]
        group = next(
            (value.strip() for value in group_top if value.strip()), f"group_{start}"
        )
        names.extend((normalize_name(f"{group}_{value}") for value in group_bottom))
    return names[: len(bottom)]


def standardize_bps_units(frame):
    rename = {
        "1_unit_units": "one_unit_units",
        "2_units_units": "two_unit_units",
        "3_4_units_units": "three_four_unit_units",
        "5_units_units": "five_plus_unit_units",
        "101_units": "one_unit_units",
        "103_units": "two_unit_units",
        "104_units": "three_four_unit_units",
        "105_units": "five_plus_unit_units",
    }
    for source, target in rename.items():
        if source in frame.columns and target not in frame.columns:
            frame[target] = frame[source]
    candidates = {
        target: [column for column in frame.columns if column.endswith(source)]
        for source, target in [
            ("1_unit_units", "one_unit_units"),
            ("2_units_units", "two_unit_units"),
            ("3_4_units_units", "three_four_unit_units"),
            ("5_units_units", "five_plus_unit_units"),
        ]
    }
    for target, columns in candidates.items():
        if target not in frame.columns and columns:
            frame[target] = frame[columns[0]]
    for column in [
        "one_unit_units",
        "two_unit_units",
        "three_four_unit_units",
        "five_plus_unit_units",
    ]:
        frame[column] = pd.to_numeric(frame.get(column), errors="coerce")
    frame["single_family_units_authorized"] = frame["one_unit_units"]
    frame["multifamily_units_authorized"] = frame[
        ["two_unit_units", "three_four_unit_units", "five_plus_unit_units"]
    ].sum(axis=1, min_count=3)
    frame["units_authorized_total"] = frame[
        [
            "one_unit_units",
            "two_unit_units",
            "three_four_unit_units",
            "five_plus_unit_units",
        ]
    ].sum(axis=1, min_count=4)
    return frame


def build_bps_county():
    frames = []
    for year in YEARS:
        text = cached_text(
            f"https://www2.census.gov/econ/bps/County/co{year}a.txt",
            MARKET_CACHE_DIR / "bps" / f"county_{year}.txt",
        )
        frame = pd.read_csv(
            io.StringIO(text), skiprows=2, names=bps_column_names(text, 6), dtype=str
        )
        frame.columns = [normalize_name(column) for column in frame.columns]
        state_col = next(
            (
                column
                for column in frame.columns
                if column in {"fips_state", "state_fips", "state"}
            )
        )
        county_col = next(
            (
                column
                for column in frame.columns
                if column in {"fips_county", "county_fips", "county"}
            )
        )
        frame["county_fips"] = frame[state_col].astype("string").str.zfill(2) + frame[
            county_col
        ].astype("string").str.zfill(3)
        frame = frame.loc[frame["county_fips"].isin(BAY_COUNTY_FIPS.values())].copy()
        frame["county"] = frame["county_fips"].map(FIPS_TO_BAY_COUNTY)
        frame["year"] = year
        frame = standardize_bps_units(frame)
        permit_columns = [
            c for c in frame if c.endswith(("_bldgs", "_units", "_value"))
        ]
        frame[permit_columns] = frame[permit_columns].apply(pd.to_numeric)
        frame["source"] = "Census_BPS_annual_county"
        frames.append(
            frame[
                [
                    "county_fips",
                    "county",
                    "year",
                    *permit_columns,
                    "single_family_units_authorized",
                    "multifamily_units_authorized",
                    "units_authorized_total",
                    "source",
                ]
            ]
        )
    return pd.concat(frames, ignore_index=True)


def census_get(year, geography, vars_dict):
    url = f"https://api.census.gov/data/{year}/acs/acs1"
    query = {"get": "NAME," + ",".join(vars_dict.values()), **geography}
    digest = hashlib.sha256(
        json.dumps({"year": year, "query": query}, sort_keys=True).encode()
    ).hexdigest()[:16]
    cache_path = CACHE_DIR / "census/county" / f"stock_{year}_{digest}.json"
    payload = census_json(url, query, cache_path)
    frame = pd.DataFrame(payload[1:], columns=payload[0]).rename(
        columns={value: key for key, value in vars_dict.items()}
    )
    for column in vars_dict:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def build_zhvi():
    parts = []
    for tier, suffix in [("lower", "0.0_0.33"), ("upper", "0.67_1.0")]:
        path = SOURCE_DIR / f"County_zhvi_uc_sfrcondo_tier_{suffix}_sm_sa_month.csv"
        d = pd.read_csv(path)
        d["county_fips"] = d.StateCodeFIPS.astype("Int64").astype("string").str.zfill(
            2
        ) + d.MunicipalCodeFIPS.astype("Int64").astype("string").str.zfill(3)
        d = d.loc[d.county_fips.isin(BAY_COUNTY_FIPS.values())]
        dates = [c for c in d if re.fullmatch("\\d{4}-\\d{2}-\\d{2}", c)]
        d = d[["county_fips", *dates]].melt(
            id_vars="county_fips", var_name="date", value_name="home_value_nominal"
        )
        d["date"] = pd.to_datetime(d.date)
        d["year"] = d.date.dt.year
        d["tier"] = tier
        d["source_file"] = path.name
        parts.append(d)
    return pd.concat(parts, ignore_index=True).sort_values(
        ["county_fips", "tier", "date"]
    )


def build_zhvi_annual():
    d = build_zhvi()
    d = d.loc[d.year.isin(YEARS)]
    d = d.groupby(["county_fips", "tier", "year"], as_index=False).agg(
        home_value_nominal=("home_value_nominal", "mean"),
        months_observed=("home_value_nominal", "count"),
    )
    d["home_value_nominal"] = d.home_value_nominal.where(d.months_observed.eq(12))
    d = d.merge(load_cpi(), on="year", validate="many_to_one")
    d["home_value_2024"] = d.home_value_nominal * d.inflation_to_2024
    return d


def build_loan_limits():
    parts = []
    for path in sorted(SOURCE_DIR.glob("fullcountyloanlimitlist*")):
        year = int(re.search("(20\\d{2})", path.name).group(1))
        if path.suffix == ".csv":
            d = pd.read_csv(path)
        else:
            raw = pd.read_excel(path, header=None)
            rows = raw.index[
                raw.apply(
                    lambda row: row.astype(str)
                    .str.contains("FIPS State Code", regex=False)
                    .any(),
                    axis=1,
                )
            ]
            header = rows[0]
            d = raw.iloc[header + 1 :].copy()
            d.columns = raw.iloc[header].tolist()
        d.columns = [normalize_name(c) for c in d.columns]
        d["county_fips"] = pd.to_numeric(d.fips_state_code, errors="coerce").astype(
            "Int64"
        ).astype("string").str.zfill(2) + pd.to_numeric(
            d.fips_county_code, errors="coerce"
        ).astype(
            "Int64"
        ).astype(
            "string"
        ).str.zfill(
            3
        )
        d = d.loc[d.county_fips.isin(BAY_COUNTY_FIPS.values())].copy()
        for units, col in enumerate(
            ["one_unit_limit", "two_unit_limit", "three_unit_limit", "four_unit_limit"],
            1,
        ):
            part = d[["county_fips", col]].rename(columns={col: "loan_limit_nominal"})
            part["loan_limit_nominal"] = pd.to_numeric(
                part.loan_limit_nominal.astype(str).str.replace("[$,]", "", regex=True),
                errors="raise",
            )
            part["year"] = year
            part["units"] = units
            part["source_file"] = path.name
            part["annual_comparable"] = year >= 2012
            part["schedule_note"] = (
                "full_year_modern_originations"
                if year >= 2012
                else "source_acquisition_and_origination_date_restrictions"
            )
            parts.append(part)
    d = pd.concat(parts, ignore_index=True)
    return d


def build_metro_context():
    variables = {
        "population": "B01003_001E",
        "median_household_income": "B19013_001E",
        "housing_units": "B25001_001E",
    }
    geo = "metropolitan statistical area/micropolitan statistical area"
    frames = []
    for year in YEARS:
        if year == 2020:
            continue
        frame = census_get(year, {"for": geo + ":*"}, variables)
        frame = frame.loc[frame.NAME.str.contains("Metro Area", regex=False)].copy()
        frame = frame.rename(columns={geo: "cbsa_code", "NAME": "msa"})
        frame["year"] = year
        for column in variables:
            frame[column] = frame[column].where(frame[column].ge(0))
        frames.append(frame)
    all_metros = pd.concat(frames, ignore_index=True)
    all_metros["published_cbsa_code"] = all_metros.cbsa_code
    all_metros["cbsa_code"] = all_metros.cbsa_code.replace({"31100": "31080"})
    bay_codes = {"41860", "41940", "42220", "46700", "34900"}
    baseline = all_metros.loc[all_metros.year.eq(min(YEARS))]
    largest = set(baseline.nlargest(25, "population").cbsa_code)
    selected = largest | bay_codes
    result = all_metros.loc[all_metros.cbsa_code.isin(selected)].copy()
    names = (
        result.sort_values("year")
        .drop_duplicates("cbsa_code", keep="last")
        .set_index("cbsa_code")
        .msa
    )
    index = pd.MultiIndex.from_product(
        [sorted(selected), list(YEARS)], names=["cbsa_code", "year"]
    )
    result = result.set_index(["cbsa_code", "year"]).reindex(index).reset_index()
    result["published_name"] = result.msa
    result["msa"] = result.cbsa_code.map(names)
    result["bay_msa"] = result.cbsa_code.isin(bay_codes)
    result["comparison_selection"] = "25_largest_2007_population_plus_five_Bay_MSAs"
    result["geography_type"] = "metropolitan_statistical_area"
    result["acs_product"] = np.where(result.year.eq(2020), "unavailable_2020", "ACS1")
    result = result.merge(
        load_cpi()[["year", "inflation_to_2024"]], on="year", validate="many_to_one"
    )
    result["median_household_income_2024"] = (
        result.median_household_income * result.inflation_to_2024
    )
    result["source"] = "Census_ACS1_published_annual_MSA_boundaries"
    return result.sort_values(["cbsa_code", "year"])


def main():
    for name, build in [
        ("macro_year", build_macro_year),
        ("metro_context_year", build_metro_context),
        ("zhvi_county_month", build_zhvi),
        ("zhvi_county_year", build_zhvi_annual),
        ("loan_limits_county_year", build_loan_limits),
        ("mortgage_rates_weekly", build_mortgage_rates),
        ("county_year", build_county_year),
        ("hpi_county_year", build_hpi_county),
        ("county_industry_year", build_qcew),
        ("boe_county_period", build_boe),
        ("rent_county_month", build_rent),
    ]:
        frame = build()
        if frame is None:
            if (OUTPUT_DIR / (name + ".parquet")).exists():
                raise FileNotFoundError(
                    "Restore ZORI_SOURCE before updating the existing rent output"
                )
            print("ZORI source absent; monthly rent is unavailable")
            continue
        write_parquet(frame, name + ".parquet")
        print(f"Wrote {name}: {len(frame):,} rows")


if __name__ == "__main__":
    main()
