from pathlib import Path
import os
import json
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import pandas as pd

SOURCE_DIR = Path(str(Path(__file__).resolve().parent / "data/source"))
OUTPUT_DIR = Path(str(SOURCE_DIR.parent / "processed"))
KEY = ["YEAR", "SAMPLE", "SERIAL", "PERNUM"]
HHKEY = KEY[:3]
RAW_COLUMNS = ['YEAR',
 'SAMPLE',
 'SERIAL',
 'PERNUM',
 'HHWT',
 'PERWT',
 'STATEFIP',
 'PUMA',
 'CPUMA0010',
 'MIGRATE1',
 'MIGPLAC1',
 'MIGPUMA1',
 'CPI99',
 'GQ',
 'RELATE',
 'RELATED',
 'EMPSTAT',
 'TRANWORK',
 'TRANTIME',
 'HHINCOME',
 'INCTOT',
 'OWNERSHP',
 'OWNERSHPD',
 'RENTGRS',
 'OWNCOST',
 'VALUEH',
 'BEDROOMS',
 'AGE',
 'MOVEDIN',
 'UNITSSTR',
 'NUMPREC']
OUTPUT_COLUMNS = {'acs_households': ['AGE',
                    'HHWT',
                    'MIGRATE1',
                    'MOVEDIN',
                    'OWNERSHP',
                    'PUMA',
                    'SAMPLE',
                    'SERIAL',
                    'YEAR',
                    'adult_child_count',
                    'adult_count',
                    'adult_roster_complete',
                    'age_group',
                    'bedrooms',
                    'commute_minutes',
                    'county_fips',
                    'cpuma0010',
                    'cpuma_allocation',
                    'employed',
                    'geography_allocation',
                    'gross_rent_2024',
                    'home_value_2024',
                    'household_income_2024',
                    'household_size',
                    'household_weight',
                    'householder_observed',
                    'housing_cost_monthly_2024',
                    'living_alone',
                    'minor_count',
                    'owner',
                    'post_housing_income_2024',
                    'roommates_present',
                    'sample_note',
                    'spouse_partner_present',
                    'structure',
                    'tenure',
                    'worked_from_home'],
 'acs_persons': ['YEAR',
                 'SAMPLE',
                 'SERIAL',
                 'PERNUM',
                 'AGE',
                 'PERWT',
                 'MIGRATE1',
                 'MIGPLAC1',
                 'STATEFIP',
                 'current_bay_population_share',
                 'prior_bay_population_share',
                 'household_population_member',
                 'personal_income_2024',
                 'owner',
                 'sample_note']}
EXTRACTS = ["usa_00083.csv", "usa_00084.csv"]
CHUNK_SIZE = int(os.getenv("ACS_CHUNK_SIZE", "250000"))
BAY_COUNTY_FIPS = {
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


def numeric(series):
    return pd.to_numeric(
        series.astype("string").str.replace(",", "", regex=False), errors="coerce"
    )


def cpuma_components():
    d = pd.read_csv(SOURCE_DIR / "CPUMA0010_PUMA2010_components.csv")
    d = d.rename(
        columns={"State_FIPS": "state_fips", "PUMA": "puma", "CPUMA0010": "cpuma0010"}
    )
    for col in ["state_fips", "puma", "cpuma0010"]:
        d[col] = numeric(d[col])
    return d.loc[
        d.state_fips.eq(6), ["state_fips", "puma", "cpuma0010"]
    ].drop_duplicates()


def cpuma_county_crosswalk(cpuma):
    d = pd.read_fwf(
        SOURCE_DIR / "PUMSEQ10_06.txt",
        encoding="latin-1",  # Equivalency-file names contain single-byte accented text.
        colspecs=[(0, 3), (3, 5), (13, 18), (18, 21), (61, 70), (70, 79)],
        names=["SL", "STATEFP", "PUMACE", "COUNTYFP", "POP10", "HU10"],
    )
    for col in ["SL", "STATEFP", "PUMACE", "COUNTYFP", "POP10", "HU10"]:
        d[col] = numeric(d[col])
    d = d.loc[d.SL.eq(796) & d.STATEFP.eq(6)].copy()
    d = d.merge(
        cpuma,
        left_on=["STATEFP", "PUMACE"],
        right_on=["state_fips", "puma"],
        validate="many_to_one",
    )
    d["county_fips"] = d.STATEFP.astype(int).astype(str).str.zfill(
        2
    ) + d.COUNTYFP.astype(int).astype(str).str.zfill(3)
    d = d.groupby(["cpuma0010", "county_fips"], as_index=False).agg(
        population_2010=("POP10", "sum"), housing_units_2010=("HU10", "sum")
    )
    d["county_population_share"] = d.population_2010 / d.groupby(
        "cpuma0010"
    ).population_2010.transform("sum")
    d["county_housing_share"] = d.housing_units_2010 / d.groupby(
        "cpuma0010"
    ).housing_units_2010.transform("sum")
    d["in_bay_county"] = d.county_fips.isin(BAY_COUNTY_FIPS)
    return d


def puma2020_cpuma_crosswalk(cpuma):
    d = pd.read_csv(SOURCE_DIR / "PUMA2010_PUMA2020_crosswalk.csv")
    for col in ["State10", "PUMA10", "State20", "PUMA20", "Part_Pop20"]:
        d[col] = numeric(d[col])
    d = d.loc[d.State10.eq(6) & d.State20.eq(6)].copy()
    d = d.merge(
        cpuma,
        left_on=["State10", "PUMA10"],
        right_on=["state_fips", "puma"],
        validate="many_to_one",
    )
    d = d.groupby(["State20", "PUMA20", "cpuma0010"], as_index=False).Part_Pop20.sum()
    d["cpuma_allocation"] = d.Part_Pop20 / d.groupby(
        ["State20", "PUMA20"]
    ).Part_Pop20.transform("sum")
    return d.rename(columns={"State20": "state_fips", "PUMA20": "puma"})[
        ["state_fips", "puma", "cpuma0010", "cpuma_allocation"]
    ]


def migpuma_components(vintage):
    if vintage == 2000:
        d = pd.read_excel(SOURCE_DIR / "ipums_usa_puma_migpuma_2000.xlsx")
        d = d.rename(
            columns={
                "State FIPS Code (STATEFIP)": "state_fips",
                "PUMA": "puma",
                "Migration PUMA (MIGPUMA1)": "migpuma",
            }
        )[["state_fips", "puma", "migpuma"]]
    else:
        d = (
            pd.read_excel(SOURCE_DIR / f"puma_migpuma1_pwpuma00_{vintage}.xls")
            .iloc[:, :4]
            .copy()
        )
        d.columns = ["state_fips", "puma", "mig_state_fips", "migpuma"]
        d = d[["state_fips", "puma", "migpuma"]]
    for col in ["state_fips", "puma", "migpuma"]:
        d[col] = numeric(d[col])
    return d.loc[d.state_fips.eq(6)].dropna().drop_duplicates()


def migpuma_cpuma_crosswalk(cpuma):
    x10_20 = pd.read_csv(SOURCE_DIR / "PUMA2010_PUMA2020_crosswalk.csv")
    for col in ["State10", "PUMA10", "State20", "PUMA20", "Part_Pop10", "Part_Pop20"]:
        x10_20[col] = numeric(x10_20[col])
    x10_20 = x10_20.loc[x10_20.State10.eq(6) & x10_20.State20.eq(6)].copy()

    x00_10 = pd.read_excel(SOURCE_DIR / "PUMA2000_PUMA2010_crosswalk.xls")
    for col in ["State00", "PUMA00", "State10", "PUMA10", "Part_Pop00"]:
        x00_10[col] = numeric(x00_10[col])
    x00_10 = x00_10.loc[x00_10.State00.eq(6) & x00_10.State10.eq(6)].copy()

    c00 = migpuma_components(2000)
    c10 = migpuma_components(2010)
    c20 = migpuma_components(2020)

    a00 = c00.merge(
        x00_10[["State00", "PUMA00", "State10", "PUMA10", "Part_Pop00"]],
        left_on=["state_fips", "puma"],
        right_on=["State00", "PUMA00"],
        validate="one_to_many",
    )
    a00 = a00.merge(
        cpuma,
        left_on=["State10", "PUMA10"],
        right_on=["state_fips", "puma"],
        suffixes=("", "_target"),
        validate="many_to_one",
    )
    a00 = (
        a00.groupby(["migpuma", "cpuma0010"], as_index=False)
        .Part_Pop00.sum()
        .rename(columns={"Part_Pop00": "population"})
    )
    a00["vintage"] = 2000

    p10 = x10_20.groupby(["State10", "PUMA10"], as_index=False).Part_Pop10.sum()
    a10 = c10.merge(
        p10,
        left_on=["state_fips", "puma"],
        right_on=["State10", "PUMA10"],
        validate="one_to_one",
    )
    a10 = a10.merge(cpuma, on=["state_fips", "puma"], validate="many_to_one")
    a10 = (
        a10.groupby(["migpuma", "cpuma0010"], as_index=False)
        .Part_Pop10.sum()
        .rename(columns={"Part_Pop10": "population"})
    )
    a10["vintage"] = 2010

    a20 = c20.merge(
        x10_20[["State10", "PUMA10", "State20", "PUMA20", "Part_Pop20"]],
        left_on=["state_fips", "puma"],
        right_on=["State20", "PUMA20"],
        validate="one_to_many",
    )
    a20 = a20.merge(
        cpuma,
        left_on=["State10", "PUMA10"],
        right_on=["state_fips", "puma"],
        suffixes=("", "_target"),
        validate="many_to_one",
    )
    a20 = (
        a20.groupby(["migpuma", "cpuma0010"], as_index=False)
        .Part_Pop20.sum()
        .rename(columns={"Part_Pop20": "population"})
    )
    a20["vintage"] = 2020

    d = pd.concat([a00, a10, a20], ignore_index=True)
    d["state_fips"] = 6
    d["cpuma_allocation"] = d.population / d.groupby(
        ["vintage", "state_fips", "migpuma"]
    ).population.transform("sum")
    return d[["vintage", "state_fips", "migpuma", "cpuma0010", "cpuma_allocation"]]


def bay_shares(cpuma_county, puma20_cpuma, migpuma_cpuma):
    b = (
        cpuma_county.loc[cpuma_county.in_bay_county]
        .groupby("cpuma0010", as_index=False)
        .agg(
            bay_population_share=("county_population_share", "sum"),
            bay_housing_share=("county_housing_share", "sum"),
        )
    )

    p = puma20_cpuma.merge(
        b, on="cpuma0010", how="left", validate="many_to_one"
    ).fillna({"bay_population_share": 0, "bay_housing_share": 0})
    p["population_piece"] = p.cpuma_allocation * p.bay_population_share
    p["housing_piece"] = p.cpuma_allocation * p.bay_housing_share
    p = p.groupby(["state_fips", "puma"], as_index=False).agg(
        current_bay_population_share=("population_piece", "sum"),
        current_bay_housing_share=("housing_piece", "sum"),
    )

    m = migpuma_cpuma.merge(
        b[["cpuma0010", "bay_population_share"]],
        on="cpuma0010",
        how="left",
        validate="many_to_one",
    ).fillna({"bay_population_share": 0})
    m["bay_piece"] = m.cpuma_allocation * m.bay_population_share
    m = (
        m.groupby(["vintage", "state_fips", "migpuma"], as_index=False)
        .bay_piece.sum()
        .rename(columns={"bay_piece": "prior_bay_population_share"})
    )
    return b, p, m


def record_chunks(cpuma_bay, puma20_bay, migpuma_bay):
    for name in EXTRACTS:
        pending = pd.DataFrame()
        previous = None
        for chunk in pd.read_csv(
            SOURCE_DIR / name,
            usecols=RAW_COLUMNS,
            chunksize=CHUNK_SIZE,
        ):
            d = (
                pd.concat([pending, chunk], ignore_index=True)
                if len(pending)
                else chunk
            )
            keys = pd.MultiIndex.from_frame(d[KEY])
            assert keys.is_monotonic_increasing and keys.is_unique
            if previous is not None:
                assert keys[0] > previous
            last = d[HHKEY].iloc[-1]
            tail = d[HHKEY].eq(last).all(axis=1)
            pending = d.loc[tail].copy()
            d = d.loc[~tail].copy()
            if len(d):
                previous = tuple(d[KEY].iloc[-1])
                yield map_records(d, name, cpuma_bay, puma20_bay, migpuma_bay)
        if len(pending):
            yield map_records(pending, name, cpuma_bay, puma20_bay, migpuma_bay)


def map_records(d, name, cpuma_bay, puma20_bay, migpuma_bay):
    d = d.copy()
    d["source_file"] = name
    d["adult_roster_complete"] = name == EXTRACTS[0]
    d["puma_vintage"] = np.select(
        [d.YEAR.le(2011), d.YEAR.le(2021)], [2000, 2010], 2020
    )
    d["current_cpuma0010"] = numeric(d.CPUMA0010).where(
        d.YEAR.le(2021) & d.STATEFIP.eq(6)
    )

    d = d.merge(
        cpuma_bay.rename(
            columns={
                "bay_population_share": "current_cpuma_bay_population_share",
                "bay_housing_share": "current_cpuma_bay_housing_share",
            }
        ),
        left_on="current_cpuma0010",
        right_on="cpuma0010",
        how="left",
        validate="many_to_one",
    ).drop(columns="cpuma0010")

    d[["current_cpuma_bay_population_share", "current_cpuma_bay_housing_share"]] = d[
        ["current_cpuma_bay_population_share", "current_cpuma_bay_housing_share"]
    ].fillna(0.0)

    d = d.merge(
        puma20_bay.rename(columns={"state_fips": "STATEFIP", "puma": "PUMA"}),
        on=["STATEFIP", "PUMA"],
        how="left",
        validate="many_to_one",
    )
    d[["current_bay_population_share", "current_bay_housing_share"]] = d[
        ["current_bay_population_share", "current_bay_housing_share"]
    ].fillna(0.0)

    d["current_bay_population_share"] = np.where(
        d.STATEFIP.ne(6),
        0.0,
        np.where(
            d.YEAR.le(2021),
            d.current_cpuma_bay_population_share,
            d.current_bay_population_share,
        ),
    )
    d["current_bay_housing_share"] = np.where(
        d.STATEFIP.ne(6),
        0.0,
        np.where(
            d.YEAR.le(2021),
            d.current_cpuma_bay_housing_share,
            d.current_bay_housing_share,
        ),
    )

    prior_lookup = migpuma_bay.rename(
        columns={
            "vintage": "puma_vintage",
            "state_fips": "MIGPLAC1",
            "migpuma": "MIGPUMA1",
        }
    )
    d = d.merge(
        prior_lookup,
        on=["puma_vintage", "MIGPLAC1", "MIGPUMA1"],
        how="left",
        validate="many_to_one",
    )
    same = d.MIGRATE1.eq(1)
    moved = d.MIGRATE1.isin([2, 3, 4])
    d["prior_bay_population_share"] = np.select(
        [same, moved & d.MIGPLAC1.eq(6), moved & d.MIGPLAC1.ne(6)],
        [d.current_bay_population_share, d.prior_bay_population_share, 0.0],
        default=np.nan,
    )
    d = d.drop(
        columns=[
            "current_cpuma_bay_population_share",
            "current_cpuma_bay_housing_share",
        ]
    )
    return d


def prepare_variables(d, cpi2024):
    d = d.copy()
    d["inflation_to_2024"] = d.CPI99 / cpi2024
    d["household_population_member"] = d.GQ.isin([1, 2])
    d["householder"] = d.RELATE.eq(1) & d.household_population_member
    d["employed"] = d.EMPSTAT.eq(1)
    worker = d.employed & d.TRANWORK.gt(0)
    d["worked_from_home"] = d.TRANWORK.eq(80).where(worker)
    d["commute_minutes"] = d.TRANTIME.where(
        d.employed & d.TRANWORK.ne(80) & d.TRANTIME.gt(0)
    )
    for raw, col, missing in [
        ("HHINCOME", "household_income_2024", [9999999]),
        ("INCTOT", "personal_income_2024", [9999998, 9999999]),
    ]:
        d[col] = d[raw].mask(d[raw].isin(missing)) * d.inflation_to_2024
    d["tenure"] = np.select(
        [d.OWNERSHP.eq(2), d.OWNERSHPD.eq(12), d.OWNERSHPD.eq(13)],
        ["renter", "free_clear", "mortgaged"],
        "unknown",
    )
    d["owner"] = d.OWNERSHP.eq(1)
    d["renter"] = d.OWNERSHP.eq(2)
    d["gross_rent_2024"] = (
        d.RENTGRS.where(d.renter & d.RENTGRS.gt(0) & d.RENTGRS.lt(99999))
        * d.inflation_to_2024
    )
    d["owner_cost_2024"] = (
        d.OWNCOST.where(d.owner & d.OWNCOST.lt(99999)) * d.inflation_to_2024
    )
    d["home_value_2024"] = (
        d.VALUEH.where(d.owner & d.VALUEH.gt(0) & d.VALUEH.lt(9999999))
        * d.inflation_to_2024
    )
    d["housing_cost_monthly_2024"] = d.owner_cost_2024.where(d.owner, d.gross_rent_2024)
    d["post_housing_income_2024"] = (
        d.household_income_2024 - 12 * d.housing_cost_monthly_2024
    )
    d["bedrooms"] = (d.BEDROOMS - 1).where(d.BEDROOMS.gt(0))
    d["age_group"] = pd.cut(
        d.AGE, [17, 34, 49, 64, 200], labels=["18-34", "35-49", "50-64", "65+"]
    ).astype("string")
    d["structure"] = np.select(
        [d.UNITSSTR.isin([3, 4]), d.UNITSSTR.isin([5, 6]), d.UNITSSTR.between(7, 10)],
        ["one_unit", "2-4_units", "5+_units"],
        "other_unknown",
    )
    d["sample_note"] = np.where(
        d.YEAR.eq(2020), "2020_nonstandard_ACS_use_separately", "annual_ACS"
    )
    ca = d.loc[d.adult_roster_complete & d.household_population_member].copy()
    ca["adult_child"] = ca.RELATE.eq(3)
    ca["roommate"] = ca.RELATED.eq(1115)
    ca["spouse_partner"] = ca.RELATE.eq(2) | ca.RELATED.eq(1114)
    hh = ca.groupby(HHKEY, as_index=False).agg(
        adult_count=("PERNUM", "size"), adult_child_count=("adult_child", "sum"),
        roommates_present=("roommate", "max"), spouse_partner_present=("spouse_partner", "max"),
    )
    d = d.merge(hh, on=HHKEY, how="left", validate="many_to_one")
    d["household_size"] = d.NUMPREC.where(
        d.household_population_member & d.NUMPREC.gt(0)
    )
    d["minor_count"] = (d.household_size - d.adult_count).where(
        d.adult_roster_complete & d.household_population_member
    )
    d["living_alone"] = d.household_size.eq(1).where(d.household_size.notna())
    return d


def household_records(d):
    a = d.loc[d.household_population_member].copy()
    h = a.sort_values(["householder", "PERNUM"], ascending=[False, True]).drop_duplicates(HHKEY).copy()
    h["householder_observed"] = h.householder
    for col in ["AGE", "MIGRATE1", "commute_minutes"]:
        h[col] = h[col].astype(float).where(h.householder_observed)
    h["age_group"] = h.age_group.astype("string").where(h.householder_observed, "unknown_householder")
    for col in ["employed", "worked_from_home"]:
        h[col] = h[col].where(h.householder_observed)
    return h


def allocate_bay_households(h, puma20_cpuma, cpuma_county):
    pre = h.loc[h.YEAR.le(2021) & h.STATEFIP.eq(6) & h.current_cpuma0010.notna()].copy()
    pre["cpuma0010"] = pre.current_cpuma0010
    pre["cpuma_allocation"] = 1.0

    post = h.loc[h.YEAR.ge(2022) & h.STATEFIP.eq(6)].merge(
        puma20_cpuma.rename(columns={"state_fips": "STATEFIP", "puma": "PUMA"}),
        on=["STATEFIP", "PUMA"],
        how="inner",
        validate="many_to_many",
    )

    d = pd.concat([pre, post], ignore_index=True)
    d = d.merge(cpuma_county, on="cpuma0010", how="inner", validate="many_to_many")
    d = d.loc[d.in_bay_county].copy()
    d["county_allocation"] = d.county_housing_share
    d["geography_allocation"] = d.cpuma_allocation * d.county_allocation
    d["household_weight"] = d.HHWT * d.geography_allocation
    return d


def main():
    factors = pd.concat(
        [
            x.drop_duplicates()
            for x in pd.read_csv(
                SOURCE_DIR / EXTRACTS[0],
                usecols=["YEAR", "CPI99"],
                chunksize=CHUNK_SIZE,
            )
        ]
    ).drop_duplicates()
    base = factors.loc[factors.YEAR.eq(2024), "CPI99"].item()

    cpuma = cpuma_components()
    cpuma_county = cpuma_county_crosswalk(cpuma)
    puma20_cpuma = puma2020_cpuma_crosswalk(cpuma)
    migpuma_cpuma = migpuma_cpuma_crosswalk(cpuma)
    cpuma_bay, puma20_bay, migpuma_bay = bay_shares(
        cpuma_county, puma20_cpuma, migpuma_cpuma
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    names = list(OUTPUT_COLUMNS)
    writers = {}
    schemas = {}
    counts = {n: 0 for n in names}

    def append(frame, name):
        if frame.empty:
            return
        frame = frame[OUTPUT_COLUMNS[name]].copy()
        for col in ["worked_from_home", "roommates_present", "spouse_partner_present", "living_alone", "employed"]:
            if col in frame:
                frame[col] = frame[col].astype("boolean")
        for col in frame.select_dtypes(["object", "string"]).columns:
            frame[col] = frame[col].astype("string")
        table = pa.Table.from_pandas(frame, preserve_index=False)
        if name not in writers:
            schemas[name] = table.schema
            writers[name] = pq.ParquetWriter(
                OUTPUT_DIR / (name + ".parquet.part"), table.schema, compression="zstd"
            )
        table = table.cast(schemas[name])
        writers[name].write_table(table)
        counts[name] += len(frame)

    try:
        for d in record_chunks(cpuma_bay, puma20_bay, migpuma_bay):
            d = prepare_variables(d, base)
            h = household_records(d)
            bay = allocate_bay_households(h, puma20_cpuma, cpuma_county)
            append(d, "acs_persons")
            append(bay, "acs_households")
            print("Processed", counts["acs_persons"], "persons", flush=True)
    finally:
        for writer in writers.values():
            writer.close()
    for name in writers:
        (OUTPUT_DIR / (name + ".parquet.part")).replace(
            OUTPUT_DIR / (name + ".parquet")
        )
    print(json.dumps(counts, indent=2))


if __name__ == "__main__":
    main()
