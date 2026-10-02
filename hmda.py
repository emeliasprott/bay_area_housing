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
CODE_FIELDS = {
    "action_code": ("action_taken", "action_type"),
    "loan_type_code": ("loan_type",),
    "loan_purpose_code": ("loan_purpose",),
    "lien_status_code": ("lien_status",),
    "reverse_mortgage_code": ("reverse_mortgage",),
    "open_end_line_of_credit_code": ("open_end_line_of_credit",),
    "business_or_commercial_purpose_code": ("business_or_commercial_purpose",),
    "preapproval_code": ("preapproval",),
    "purchaser_type_code": ("purchaser_type",),
    "construction_method_code": ("construction_method",),
    "property_type_code": ("property_type",),
    "hoepa_status_code": ("hoepa_status",),
    "negative_amortization_code": ("negative_amortization",),
    "interest_only_payment_code": ("interest_only_payment",),
    "balloon_payment_code": ("balloon_payment",),
    "other_nonamortizing_features_code": ("other_nonamortizing_features",),
    "submission_of_application_code": ("submission_of_application",),
    "initially_payable_to_institution_code": ("initially_payable_to_institution",),
}
NUMERIC_FIELDS = {
    "combined_loan_to_value_ratio": (
        "loan_to_value_ratio",
        "combined_loan_to_value_ratio",
    ),
    "interest_rate": ("interest_rate",),
    "loan_term_months": ("loan_term",),
    "rate_spread": ("rate_spread",),
    "intro_rate_period_months": ("intro_rate_period",),
    "prepayment_penalty_months": ("prepayment_penalty_term",),
    "total_loan_costs": ("total_loan_costs",),
    "total_points_and_fees": ("total_points_and_fees",),
    "origination_charges": ("origination_charges",),
    "discount_points": ("discount_points",),
    "lender_credits": ("lender_credits",),
}
BOOL_FIELDS = [
    "coapplicant_present",
    "county_in_bay",
    "is_origination",
    "is_first_lien",
    "site_built_1_4",
    "confirmed_closed_end_nonreverse",
    "product_status_unknown",
    "eligible_purchase",
    "eligible_refinance",
    "eligible_underwriting",
    "eligible_lockin_origination",
    "low_rate_cohort_2020_2021",
    "is_nonprincipal",
    "is_equity_credit",
    "eligible_preapproval",
    "is_purchased_loan",
    "tract_identified",
]
STRING_FIELDS = [
    "applicant_age_band",
    "coapplicant_age_reported",
    "applicant_race_reported",
    "applicant_ethnicity_reported",
    "county_fips",
    "tract_geoid",
    "lender_id",
    "lender_id_type",
    "purpose",
    "occupancy",
    "debt_to_income_reported",
    "conforming_loan_limit",
    "dwelling_category",
    "loan_product_type",
    "total_units_reported",
    "source_file",
    "source_url",
    "record_id",
    "lockin_measure",
]
FLOAT_FIELDS = [
    "property_less_first_loan",
    "scheduled_monthly_pi",
    "scheduled_first_year_principal",
    *NUMERIC_FIELDS,
    "applicant_income",
    "loan_amount",
    "property_value",
    "applicant_income_2024",
    "loan_amount_2024",
    "property_value_2024",
    "calculated_first_lien_ltv",
    "dti_lower",
    "dti_upper",
] + [
    c + "_2024"
    for c in [
        "total_loan_costs",
        "total_points_and_fees",
        "origination_charges",
        "discount_points",
        "lender_credits",
    ]
]
INT_FIELDS = ["year", "tract_vintage", "source_row", "occupancy_code", *CODE_FIELDS]


def output_schema(raw_columns):
    return pa.schema(
        [(c, pa.int64()) for c in INT_FIELDS]
        + [(c, pa.string()) for c in STRING_FIELDS]
        + [(c, pa.float64()) for c in FLOAT_FIELDS]
        + [(c, pa.bool_()) for c in BOOL_FIELDS]
        + [("raw_" + c, pa.string()) for c in raw_columns]
    )


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


def enrich_borrower_fields(out, frame):
    out["applicant_age_band"] = (
        raw(frame, "applicant_age")
        .where(
            raw(frame, "applicant_age").isin(
                ["<25", "25-34", "35-44", "45-54", "55-64", "65-74", ">74"]
            )
        )
        .fillna("unknown")
    )
    out["coapplicant_age_reported"] = raw(frame, "co_applicant_age")
    co = out.coapplicant_age_reported
    out["coapplicant_present"] = co.ne("9999").where(
        co.isin(["<25", "25-34", "35-44", "45-54", "55-64", "65-74", ">74", "9999"])
    )
    out["applicant_race_reported"] = raw(frame, "derived_race", "applicant_race_name_1")
    out["applicant_ethnicity_reported"] = raw(
        frame, "derived_ethnicity", "applicant_ethnicity_name"
    )
    out["property_less_first_loan"] = (out.property_value - out.loan_amount).where(
        out.is_first_lien & out.property_value.gt(0)
    )
    plain = (
        out.is_origination
        & out.is_first_lien
        & out.confirmed_closed_end_nonreverse
        & out.interest_only_payment_code.eq(2)
        & out.negative_amortization_code.eq(2)
        & out.balloon_payment_code.eq(2)
        & out.other_nonamortizing_features_code.eq(2)
        & out.interest_rate.gt(0)
        & out.loan_term_months.gt(0)
        & out.loan_amount.gt(0)
    )
    r = out.interest_rate / 1200
    n = out.loan_term_months
    pi = out.loan_amount * r / (1 - (1 + r) ** (-n))
    months = n.clip(upper=12)
    balance = out.loan_amount * (1 + r) ** months - pi * ((1 + r) ** months - 1) / r
    out["scheduled_monthly_pi"] = pi.where(plain)
    out["scheduled_first_year_principal"] = (out.loan_amount - balance).where(plain)
    return out


def prepare_chunk(frame, year, inflation_factor, offset=0, raw_columns=None):
    frame = frame.copy()
    frame.columns = [normalize_name(c) for c in frame]
    raw_columns = sorted(frame.columns) if raw_columns is None else raw_columns
    modern = year >= 2018
    county = raw(frame, "county_code").str.replace("\\.0$", "", regex=True)
    if modern:
        county = county.str.zfill(5).where(county.str.fullmatch("\\d{5}"))
        tract = raw(frame, "census_tract").str.replace("\\.0$", "", regex=True)
        tract = tract.str.zfill(11).where(tract.str.fullmatch("\\d{10,11}"))
        county = county.fillna(tract.str[:5])
        in_california = raw(frame, "state_code").eq("CA")
    else:
        state = raw(frame, "state_code").str.zfill(2)
        county = (state + county.str.zfill(3)).where(county.str.fullmatch("\\d{1,3}"))
        tract_number = number(frame, "census_tract_number", "census_tract")
        tract = county + (tract_number * 100).round().astype("Int64").astype(
            "string"
        ).str.zfill(6)
        tract = tract.where(tract_number.ge(0) & tract_number.lt(9999.99))
        in_california = state.eq("06")
    out = pd.DataFrame(index=frame.index)
    out["year"] = year
    out["tract_vintage"] = 2000 if year <= 2011 else 2010 if year <= 2021 else 2020
    out["source_row"] = np.arange(offset + 1, offset + len(frame) + 1)
    out["county_fips"] = county
    out["tract_geoid"] = tract.where(tract.str[:5].eq(county))
    out["tract_identified"] = out.tract_geoid.notna()
    out["county_in_bay"] = county.isin(BAY_COUNTIES)
    out["lender_id"] = (
        raw(frame, "lei")
        if modern
        else raw(frame, "agency_code") + ":" + raw(frame, "respondent_id")
    )
    out["lender_id_type"] = "lei" if modern else "agency_and_respondent"
    for column, aliases in CODE_FIELDS.items():
        out[column] = number(frame, *aliases).astype("Int64")
    out["occupancy_code"] = (
        number(frame, "occupancy_type")
        if modern
        else number(frame, "occupancy", "owner_occupancy")
    )
    out["occupancy"] = out.occupancy_code.map(
        {1: "principal", 2: "second_residence", 3: "investment"}
        if modern
        else {1: "principal", 2: "nonprincipal_unsplit"}
    ).fillna("unknown")
    out["purpose"] = out.loan_purpose_code.map(
        {
            1: "purchase",
            2: "home_improvement",
            3: "refinance",
            31: "refinance",
            32: "cash_out_refinance",
            4: "other",
            5: "not_applicable",
        }
    ).fillna("unknown")
    out["applicant_income"] = (
        number(frame, "income" if modern else "applicant_income_000s") * 1000
    )
    out["loan_amount"] = number(
        frame, "loan_amount" if modern else "loan_amount_000s"
    ) * (1 if modern else 1000)
    out["property_value"] = number(frame, "property_value")
    for column in ["applicant_income", "loan_amount", "property_value"]:
        out[column + "_2024"] = out[column] * inflation_factor
    for column, aliases in NUMERIC_FIELDS.items():
        out[column] = number(frame, *aliases)
    for column in [
        "total_loan_costs",
        "total_points_and_fees",
        "origination_charges",
        "discount_points",
        "lender_credits",
    ]:
        out[column + "_2024"] = out[column] * inflation_factor
    out["calculated_first_lien_ltv"] = (
        100 * out.loan_amount / out.property_value
    ).where(out.lien_status_code.eq(1) & out.property_value.gt(0))
    out["debt_to_income_reported"] = raw(frame, "debt_to_income_ratio")
    dti = pd.to_numeric(out.debt_to_income_reported, errors="coerce")
    out["dti_lower"] = dti
    out["dti_upper"] = dti
    for label, bounds in {
        "<20%": (0, 20),
        "20%-<30%": (20, 30),
        "30%-<36%": (30, 36),
        "50%-60%": (50, 60),
        ">60%": (60, np.nan),
    }.items():
        out.loc[out.debt_to_income_reported.eq(label), ["dti_lower", "dti_upper"]] = (
            bounds
        )
    out["conforming_loan_limit"] = raw(frame, "conforming_loan_limit")
    out["dwelling_category"] = raw(frame, "derived_dwelling_category")
    out["loan_product_type"] = raw(frame, "derived_loan_product_type")
    out["total_units_reported"] = raw(frame, "total_units")
    out["site_built_1_4"] = (
        out.construction_method_code.eq(1) & number(frame, "total_units").between(1, 4)
        | out.dwelling_category.eq("Single Family (1-4 Units):Site-Built")
        if modern
        else out.property_type_code.eq(1)
    )
    out["is_first_lien"] = out.lien_status_code.eq(1)
    out["is_origination"] = out.action_code.eq(1)
    out["is_purchased_loan"] = out.action_code.eq(6)
    out["is_nonprincipal"] = out.occupancy.isin(
        ["second_residence", "investment", "nonprincipal_unsplit"]
    )
    out["confirmed_closed_end_nonreverse"] = out.reverse_mortgage_code.eq(
        2
    ) & out.open_end_line_of_credit_code.eq(2)
    out["product_status_unknown"] = ~out.reverse_mortgage_code.isin(
        [1, 2]
    ) | ~out.open_end_line_of_credit_code.isin([1, 2])
    comparable = (
        out.county_in_bay
        & out.is_first_lien
        & out.site_built_1_4
        & out.action_code.isin([1, 2, 3, 4, 5])
    )
    comparable &= ~out.reverse_mortgage_code.eq(1).fillna(
        False
    ) & ~out.open_end_line_of_credit_code.eq(1).fillna(False)
    out["eligible_purchase"] = comparable & out.purpose.eq("purchase")
    out["eligible_refinance"] = comparable & out.purpose.isin(
        ["refinance", "cash_out_refinance"]
    )
    out["eligible_underwriting"] = (
        out.eligible_purchase | out.eligible_refinance
    ) & out.action_code.isin([1, 2, 3])
    out["eligible_preapproval"] = out.county_in_bay & out.action_code.isin([7, 8])
    out["is_equity_credit"] = out.county_in_bay & (
        out.purpose.eq("cash_out_refinance")
        | out.open_end_line_of_credit_code.eq(1)
        | out.lien_status_code.eq(2)
    )
    out["eligible_lockin_origination"] = (
        (out.eligible_purchase | out.eligible_refinance)
        & out.is_origination
        & out.occupancy.eq("principal")
        & out.confirmed_closed_end_nonreverse
        & out.interest_rate.notna()
    )
    out["low_rate_cohort_2020_2021"] = out.eligible_lockin_origination & out.year.isin(
        [2020, 2021]
    )
    out = enrich_borrower_fields(out, frame)
    out["lockin_measure"] = "origination_exposure_not_outstanding_households"
    out["source_file"] = source_path(year).name
    out["source_url"] = (
        "https://ffiec.cfpb.gov/v2/data-browser-api/view/csv"
        if modern
        else "local_legacy_HMDA_archive"
    )
    out["record_id"] = str(year) + ":" + out.source_row.astype(str)
    for column in INT_FIELDS:
        out[column] = out[column].astype("Int64")
    for column in BOOL_FIELDS:
        out[column] = out[column].astype("boolean")
    for column in FLOAT_FIELDS:
        out[column] = out[column].astype("float64")
    for column in STRING_FIELDS:
        out[column] = out[column].astype("string")
    raw_data = frame.reindex(columns=raw_columns).astype("string").add_prefix("raw_")
    out = pd.concat([out, raw_data], axis=1)
    return out.loc[out.county_in_bay | county.isna() & in_california].reset_index(
        drop=True
    )


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


def write_year(year, factor, raw_columns):
    path = source_path(year)
    destination = DATASET_DIR / f"year={year}" / "part-0000.parquet"
    if not FORCE_PROCESS and (not FORCE_DOWNLOAD) and destination.exists():
        print(f"HMDA {year}: using local records (set HMDA_FORCE_PROCESS=1 to rebuild)")
        return
    schema = output_schema(raw_columns)
    offset = retained = 0
    with atomic_path(destination) as temporary:
        with csv_source(path) as source, pq.ParquetWriter(
            temporary, schema, compression="zstd"
        ) as writer:
            for chunk in pd.read_csv(
                source, dtype="string", keep_default_na=False, chunksize=CHUNK_SIZE
            ):
                output = prepare_chunk(chunk, year, factor, offset, raw_columns)
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
    raw_columns = set()
    for year in YEARS:
        with csv_source(ensure_source(year)) as source:
            raw_columns.update(
                (normalize_name(c) for c in pd.read_csv(source, nrows=0).columns)
            )
    raw_columns = sorted(raw_columns)
    for year in YEARS:
        write_year(year, float(factors.loc[year]), raw_columns)


if __name__ == "__main__":
    main()
