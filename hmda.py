from contextlib import contextmanager
from pathlib import Path
from zipfile import ZipFile
import os
import re
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

SOURCE_DIR = Path(str(Path(__file__).resolve().parent / "data/source")).expanduser()
OUTPUT_DIR = Path(str(SOURCE_DIR.parent / "processed")).expanduser()
DATASET_DIR = OUTPUT_DIR / "hmda_loans"
YEARS = range(2007, 2025)
SESSION = requests.Session()
BAY_COUNTIES = {
    "06001",
    "06013",
    "06041",
    "06055",
    "06075",
    "06081",
    "06085",
    "06095",
    "06097",
}
CHUNK_SIZE = int(os.getenv("HMDA_CHUNK_SIZE", "250000"))
FORCE_PROCESS = os.getenv("HMDA_FORCE_PROCESS", "0") == "1"
FORCE_DOWNLOAD = os.getenv("HMDA_FORCE_DOWNLOAD", "0") == "1"
RAW_COLUMNS = {'action_taken',
 'action_type',
 'applicant_income_000s',
 'census_tract',
 'construction_method',
 'county_code',
 'derived_dwelling_category',
 'income',
 'lien_status',
 'loan_purpose',
 'open_end_line_of_credit',
 'property_type',
 'reverse_mortgage',
 'state_code',
 'total_units'}


def output_schema():
    return pa.schema([("year", pa.int64()), ("county_in_bay", pa.bool_()),
        ("eligible_purchase", pa.bool_()), ("is_origination", pa.bool_()),
        ("applicant_income_2024", pa.float64())])


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


@contextmanager
def csv_source(path):
    if path.suffix.lower() == ".zip":
        with ZipFile(path) as archive:
            members = [
                n
                for n in archive.namelist()
                if n.lower().endswith(".csv") and (not n.startswith("__MACOSX/"))
            ]
            if len(members) != 1:
                raise ValueError(f"Expected one CSV inside {path.name}")
            with archive.open(members[0]) as source:
                yield source
    else:
        with path.open("rb") as source:
            yield source


def normalize_name(value):
    return re.sub("[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")


def raw(frame, *names):
    for name in names:
        if name in frame:
            return frame[name].astype("string").str.strip()
    return pd.Series(pd.NA, index=frame.index, dtype="string")


def number(frame, *names):
    return pd.to_numeric(raw(frame, *names), errors="coerce")


def prepare_chunk(frame, year, inflation_factor):
    frame = frame.copy()
    frame.columns = [normalize_name(c) for c in frame]
    modern = year >= 2018
    county = raw(frame, "county_code").str.replace(r"\.0$", "", regex=True)
    if modern:
        county = county.str.zfill(5).where(county.str.fullmatch(r"\d{5}"))
        tract = raw(frame, "census_tract").str.replace(r"\.0$", "", regex=True)
        tract = tract.str.zfill(11).where(tract.str.fullmatch(r"\d{10,11}"))
        county = county.fillna(tract.str[:5])
    else:
        state = raw(frame, "state_code").str.zfill(2)
        county = (state + county.str.zfill(3)).where(county.str.fullmatch(r"\d{1,3}"))
    action = number(frame, "action_taken", "action_type").astype("Int64")
    first_lien = number(frame, "lien_status").astype("Int64").eq(1)
    site_built = (
        number(frame, "construction_method").astype("Int64").eq(1) & number(frame, "total_units").between(1, 4)
        | raw(frame, "derived_dwelling_category").eq("Single Family (1-4 Units):Site-Built")
        if modern else number(frame, "property_type").astype("Int64").eq(1)
    )
    comparable = county.isin(BAY_COUNTIES) & first_lien & site_built & action.isin([1, 2, 3, 4, 5])
    comparable &= ~number(frame, "reverse_mortgage").astype("Int64").eq(1).fillna(False) & ~number(frame, "open_end_line_of_credit").astype("Int64").eq(1).fillna(False)
    eligible = comparable & number(frame, "loan_purpose").astype("Int64").eq(1)
    originated = action.eq(1)
    out = pd.DataFrame({"year": year, "county_in_bay": county.isin(BAY_COUNTIES),
        "eligible_purchase": eligible, "is_origination": originated,
        "applicant_income_2024": number(frame, "income" if modern else "applicant_income_000s") * 1000 * inflation_factor})
    # Missing income remains in the financed-purchase denominator.
    return out.loc[eligible & originated].reset_index(drop=True)


def source_path(year):
    if year <= 2017:
        return SOURCE_DIR / "hmda_2007-2017" / f"hmda_{year}_ca_all-records_labels.zip"
    return SOURCE_DIR / "hmda_2018-2024" / f"hmda_{year}_ca_bay.csv"


def ensure_source(year):
    path = source_path(year)
    if year <= 2017:
        if not path.exists():
            raise FileNotFoundError(path)
        return path
    params = {"years": str(year), "counties": ",".join(sorted(BAY_COUNTIES))}
    return download(
        "https://ffiec.cfpb.gov/v2/data-browser-api/view/csv",
        path,
        params=params,
        force=FORCE_DOWNLOAD,
    )


def write_year(year, factor):
    path = source_path(year)
    destination = DATASET_DIR / f"year={year}" / "part-0000.parquet"
    if (not FORCE_PROCESS and not FORCE_DOWNLOAD and destination.exists()
            and pq.read_schema(destination).equals(output_schema(), check_metadata=False)):
        print(f"HMDA {year}: using local records (set HMDA_FORCE_PROCESS=1 to rebuild)")
        return
    schema = output_schema()
    offset = retained = 0
    with atomic_path(destination) as temporary:
        with csv_source(path) as source, pq.ParquetWriter(
            temporary, schema, compression="zstd"
        ) as writer:
            for chunk in pd.read_csv(
                source, dtype="string", keep_default_na=False, chunksize=CHUNK_SIZE,
                usecols=lambda c: normalize_name(c) in RAW_COLUMNS
            ):
                output = prepare_chunk(chunk, year, factor)
                offset += len(chunk)
                retained += len(output)
                writer.write_table(
                    pa.Table.from_pandas(output, schema=schema, preserve_index=False)
                )
    print(f"HMDA {year}: retained {retained:,} of {offset:,} source records")


def main():
    factors = (
        pd.read_parquet(OUTPUT_DIR / "macro_year.parquet")
        .set_index("year")
        .inflation_to_2024
    )
    for year in YEARS:
        ensure_source(year)
        write_year(year, float(factors.loc[year]))


if __name__ == "__main__":
    main()
