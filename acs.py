from pathlib import Path
from contextlib import contextmanager
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
EXTRACTS = ["usa_00083.csv", "usa_00084.csv"]
CHUNK_SIZE = int(os.getenv("ACS_CHUNK_SIZE", "250000"))


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
    with atomic_path(OUTPUT_DIR / name) as tmp:
        frame.to_parquet(tmp, index=False, compression="zstd")


def support_membership(frame, geography):
    frame = frame.copy()
    frame["puma_vintage"] = np.select(
        [frame.YEAR.le(2011), frame.YEAR.le(2021)], [2000, 2010], 2020
    )
    for role, types, state, code in [
        ("current", ["standard"], "STATEFIP", "PUMA"),
        ("prior", ["migration", "migration_workplace"], "MIGPLAC1", "MIGPUMA1"),
        ("workplace", ["workplace", "migration_workplace"], "PWSTATE2", "PWPUMA00"),
    ]:
        lookup = geography.loc[
            geography.geography_type.isin(types),
            ["vintage", "state_fips", "puma", "bay_membership", "county_fips_exact"],
        ].drop_duplicates()
        lookup = lookup.rename(
            columns={
                "vintage": "puma_vintage",
                "state_fips": state,
                "puma": code,
                "bay_membership": role + "_bay_membership",
                "county_fips_exact": role + "_county_fips",
            }
        )
        frame = frame.merge(
            lookup, on=["puma_vintage", state, code], how="left", validate="many_to_one"
        )
        col = role + "_bay_membership"
        frame[col] = frame[col].fillna("unknown")
        frame.loc[frame[state].gt(0) & frame[state].ne(6), col] = "outside"
        frame[role + "_support_intersects_bay"] = frame[col].isin(["inside", "mixed"])
    frame["bay_analysis_relevant"] = frame[
        [r + "_support_intersects_bay" for r in ["current", "prior", "workplace"]]
    ].any(axis=1)
    frame["county_fips"] = frame.current_county_fips
    return frame


def record_chunks(geography, components, cpuma):
    for name in EXTRACTS:
        pending = pd.DataFrame()
        previous = None
        for chunk in pd.read_csv(
            SOURCE_DIR / name,
            dtype={"INDNAICS": "string", "CBSERIAL": "string"},
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
                yield map_records(d, name, geography, components, cpuma)
        if len(pending):
            yield map_records(pending, name, geography, components, cpuma)


def map_records(d, name, geography, components, cpuma):
    d = support_membership(d, geography)
    d["source_file"] = name
    d["adult_roster_complete"] = name == EXTRACTS[0]
    d["household_analysis_relevant"] = d.groupby(HHKEY).bay_analysis_relevant.transform(
        "any"
    )
    cm = components.loc[
        components.geography_type.isin(["migration", "migration_workplace"]),
        ["vintage", "state_fips", "puma", "coarse_puma"],
    ].drop_duplicates()
    d = d.merge(
        cm,
        left_on=["puma_vintage", "STATEFIP", "PUMA"],
        right_on=["vintage", "state_fips", "puma"],
        how="left",
        validate="many_to_one",
    ).drop(columns=["vintage", "state_fips", "puma"])
    d["current_migration_puma_check"] = d.coarse_puma.eq(d.MIGPUMANOW).where(
        d.coarse_puma.notna()
    )
    d = d.rename(columns={"coarse_puma": "current_migration_puma_from_components"})
    d = d.merge(
        cpuma[["vintage", "state_fips", "puma", "cpuma0010"]],
        left_on=["puma_vintage", "STATEFIP", "PUMA"],
        right_on=["vintage", "state_fips", "puma"],
        how="left",
        validate="many_to_one",
    ).drop(columns=["vintage", "state_fips", "puma"])
    d = d.rename(columns={"cpuma0010": "cpuma0010_exact"})
    return d


def prepare_variables(d, cpi2024, industry):
    d = d.copy()
    d["inflation_to_2024"] = d.CPI99 / cpi2024
    d["year"] = d.YEAR
    d["household_population_member"] = d.GQ.isin([1, 2])
    d["householder"] = d.RELATE.eq(1) & d.household_population_member
    d["person_weight"] = d.PERWT.where(d.PERWT.gt(0))
    d["household_weight"] = d.HHWT.where(d.householder & d.HHWT.gt(0))
    d["employed"] = d.EMPSTAT.eq(1)
    d["worked_from_home"] = d.TRANWORK.eq(80).where(d.TRANWORK.gt(0))
    d["commute_minutes"] = d.TRANTIME.where(d.TRANTIME.gt(0) & d.TRANWORK.ne(80))
    for raw, col, missing in [
        ("HHINCOME", "household_income_2024", [9999999]),
        ("INCTOT", "personal_income_2024", [9999998, 9999999]),
        ("INCWAGE", "wage_income_2024", [999998, 999999]),
    ]:
        d[col] = d[raw].mask(d[raw].isin(missing)) * d.inflation_to_2024
    d["tenure"] = np.select(
        [d.OWNERSHP.eq(2), d.OWNERSHPD.eq(12), d.OWNERSHPD.eq(13)],
        ["renter", "free_clear", "mortgaged"],
        "unknown",
    )
    d["mortgage_status_disagrees_with_tenure"] = d.OWNERSHPD.eq(13) & d.MORTGAGE.eq(1)
    d["owner"] = d.OWNERSHP.eq(1)
    d["renter"] = d.OWNERSHP.eq(2)
    d["no_cash_rent"] = d.OWNERSHPD.eq(21)
    d["contract_rent_2024"] = (
        d.RENT.where(d.renter & d.RENT.lt(99999)) * d.inflation_to_2024
    )
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
    for raw in ["COSTELEC", "COSTGAS", "COSTWATR"]:
        valid = d[raw].where(d[raw].gt(0) & d[raw].lt(99990))
        valid = valid.mask(
            d[raw].isin([99993, 99997] + ([99992] if raw == "COSTGAS" else [])), 0
        )
        d[raw.lower() + "_monthly_2024"] = valid / 12 * d.inflation_to_2024
    utility_cols = [
        c.lower() + "_monthly_2024" for c in ["COSTELEC", "COSTGAS", "COSTWATR"]
    ]
    d["reported_utilities_monthly_2024"] = d[utility_cols].sum(axis=1, min_count=3)
    d["housing_cost_monthly_2024"] = d.owner_cost_2024.where(d.owner, d.gross_rent_2024)
    d["post_housing_income_2024"] = (
        d.household_income_2024 - 12 * d.housing_cost_monthly_2024
    )
    d["housing_burden"] = (
        12 * d.housing_cost_monthly_2024 / d.household_income_2024
    ).where(d.household_income_2024.gt(0))
    d["nonpositive_income"] = d.household_income_2024.le(0).where(
        d.household_income_2024.notna()
    )
    d["rooms"] = d.ROOMS.where(d.ROOMS.gt(0))
    d["bedrooms"] = (d.BEDROOMS - 1).where(d.BEDROOMS.gt(0))
    d["family_size"] = d.FAMSIZE.where(d.FAMSIZE.gt(0))
    d["age_group"] = pd.cut(
        d.AGE, [17, 34, 49, 64, 200], labels=["18-34", "35-49", "50-64", "65+"]
    ).astype("string")
    d["income_band"] = (
        pd.cut(
            d.household_income_2024,
            [-np.inf, 50000, 100000, 150000, 250000, np.inf],
            labels=["<50k", "50-100k", "100-150k", "150-250k", "250k+"],
            right=False,
        )
        .astype("string")
        .fillna("unknown")
    )
    d["race_ethnicity"] = np.select(
        [d.HISPAN.between(1, 4), d.RACE.eq(1), d.RACE.eq(2), d.RACE.isin([4, 5, 6])],
        ["Hispanic_any_race", "NH_White", "NH_Black", "NH_Asian_Pacific"],
        "NH_other_multiple",
    )
    d["duration"] = d.MOVEDIN.map(
        {
            1: "0-1_year",
            2: "1-2_years",
            3: "2-4_years",
            4: "5-9_years",
            5: "10-19_years",
            6: "20-29_years",
            7: "30+_years",
        }
    ).fillna("unknown")
    d["structure"] = np.select(
        [d.UNITSSTR.isin([3, 4]), d.UNITSSTR.isin([5, 6]), d.UNITSSTR.between(7, 10)],
        ["one_unit", "2-4_units", "5+_units"],
        "other_unknown",
    )
    d["moved_last_year"] = d.MIGRATE1.isin([2, 3, 4]).where(
        d.MIGRATE1.isin([1, 2, 3, 4])
    )
    current = d.current_bay_membership.eq("inside")
    prior = d.prior_bay_membership.eq("inside")
    moved = d.moved_last_year.eq(True)
    d["migration_flow"] = np.select(
        [
            current & d.MIGRATE1.eq(1),
            current & moved & prior,
            current & moved & d.MIGPLAC1.eq(6) & d.prior_bay_membership.eq("outside"),
            current & moved & d.MIGPLAC1.between(1, 56) & d.MIGPLAC1.ne(6),
            current & moved & d.MIGPLAC1.between(100, 899),
            prior & moved & d.STATEFIP.eq(6) & d.current_bay_membership.eq("outside"),
            prior & moved & d.STATEFIP.ne(6),
        ],
        [
            "stayer",
            "within_Bay",
            "in_other_CA",
            "in_other_state",
            "in_abroad",
            "out_other_CA",
            "out_other_state",
        ],
        "unknown_or_other",
    )
    d["bay_outmover"] = d.migration_flow.isin(["out_other_CA", "out_other_state"])
    d["sample_note"] = np.where(
        d.YEAR.eq(2020), "2020_nonstandard_ACS_use_separately", "annual_ACS"
    )
    ca = d.loc[d.adult_roster_complete & d.household_population_member].copy()
    ca["adult_child"] = ca.RELATE.eq(3)
    ca["wage_recipient"] = ca.wage_income_2024.gt(0)
    ca["roommate"] = ca.RELATED.eq(1115)
    ca["spouse_partner"] = ca.RELATE.eq(2) | ca.RELATED.eq(1114)
    ca["youngest_own_child"] = ca.YNGCH.where(ca.NCHILD.gt(0) & ca.YNGCH.lt(99))
    ca["eldest_own_child"] = ca.ELDCH.where(ca.NCHILD.gt(0) & ca.ELDCH.lt(99))
    ca["own_minor_child_present"] = ca.NCHILD.gt(0) & ca.YNGCH.between(0, 17)
    ca["nonrelative_adult"] = ca.RELATE.isin([11, 12])
    hh = ca.groupby(HHKEY, as_index=False).agg(
        adult_count=("PERNUM", "size"),
        employed_adult_count=("employed", "sum"),
        adult_child_count=("adult_child", "sum"),
        nonrelative_adult_count=("nonrelative_adult", "sum"),
        own_minor_child_present=("own_minor_child_present", "max"),
        wage_recipient_count=("wage_recipient", "sum"),
        adult_wage_total_2024=("wage_income_2024", lambda x: x.sum(min_count=len(x))),
        adult_wage_observed_count=("wage_income_2024", "count"),
        roommates_present=("roommate", "max"),
        spouse_partner_present=("spouse_partner", "max"),
        youngest_own_child=("youngest_own_child", "min"),
        eldest_own_child=("eldest_own_child", "max"),
    )
    d = d.merge(hh, on=HHKEY, how="left", validate="many_to_one")
    d["adult_arrangement"] = np.select(
        [
            d.adult_count.eq(1),
            d.adult_child_count.gt(0),
            d.nonrelative_adult_count.gt(0),
            d.adult_count.ge(2),
        ],
        [
            "one_adult",
            "with_adult_children",
            "with_nonrelative_or_partner",
            "other_multiple_adults",
        ],
        "incomplete_roster",
    )
    d["income_per_adult_2024"] = d.household_income_2024 / d.adult_count
    d["household_size"] = d.NUMPREC.where(
        d.household_population_member & d.NUMPREC.gt(0)
    )
    d["minor_count"] = (d.household_size - d.adult_count).where(
        d.adult_roster_complete & d.household_population_member
    )
    d["minor_children_present"] = d.minor_count.gt(0).where(d.minor_count.notna())
    d["living_alone"] = d.household_size.eq(1).where(d.household_size.notna())
    d["equivalized_income_2024"] = d.household_income_2024 / np.sqrt(d.household_size)
    d["equivalized_post_housing_2024"] = d.post_housing_income_2024 / np.sqrt(
        d.household_size
    )
    d["income_other_than_observed_adult_wages_2024"] = (
        d.household_income_2024 - d.adult_wage_total_2024
    )
    d["industry_code"] = d.INDNAICS.str.strip()
    d["industry_vintage"] = np.select(
        [d.YEAR.le(2007), d.YEAR.le(2012), d.YEAR.le(2017), d.YEAR.le(2022)],
        [2002, 2007, 2012, 2017],
        2022,
    )
    d = d.merge(
        industry,
        on=["industry_vintage", "industry_code"],
        how="left",
        validate="many_to_one",
    )
    d["industry_code_matched"] = d.industry_sector.notna()
    d["industry_sector"] = d.industry_sector.fillna("unknown_or_unclassified")
    return d


def household_records(d):
    a = d.loc[d.household_population_member].copy()
    h = (
        a.sort_values(["householder", "PERNUM"], ascending=[False, True])
        .drop_duplicates(HHKEY)
        .copy()
    )
    h["householder_observed"] = h.householder
    for col in [
        "AGE",
        "RELATE",
        "RELATED",
        "MARST",
        "RACE",
        "RACED",
        "HISPAN",
        "HISPAND",
        "NCHILD",
        "YNGCH",
        "ELDCH",
        "MIGRATE1",
        "MIGRATE1D",
        "MIGPLAC1",
        "MIGPUMA1",
        "MIGMETRO1",
        "PWSTATE2",
        "PWPUMA00",
        "TRANWORK",
        "TRANTIME",
        "EMPSTAT",
        "EMPSTATD",
        "INCTOT",
        "INCWAGE",
        "PERWT",
        "PERNUM",
        "personal_income_2024",
        "wage_income_2024",
        "commute_minutes",
    ]:
        h[col] = h[col].astype(float).where(h.householder_observed)
    for col in ["age_group", "race_ethnicity", "migration_flow"]:
        h[col] = (
            h[col].astype("string").where(h.householder_observed, "unknown_householder")
        )
    for col in [
        "prior_county_fips",
        "workplace_county_fips",
        "prior_bay_membership",
        "workplace_bay_membership",
        "INDNAICS",
        "industry_code",
        "industry_sector",
    ]:
        h[col] = h[col].astype("string").where(h.householder_observed)
    for col in ["moved_last_year", "bay_outmover", "employed", "worked_from_home"]:
        h[col] = h[col].where(h.householder_observed)
    h["household_weight"] = h.HHWT
    return h


def industry_concordance():
    x = pd.read_excel(SOURCE_DIR / "indnaics_crosswalk_2023.xlsx", dtype="string")
    sector = x.iloc[:, 0].ffill().str.strip().str.rstrip(":")
    rows = []
    for vintage, period in [
        (2002, "2003-2007"),
        (2007, "2008-2012"),
        (2012, "2013-2017"),
        (2017, "2018-2022"),
        (2022, "2023-2027"),
    ]:
        col = next((c for c in x if c.strip().startswith(period)))
        d = pd.DataFrame(
            {
                "industry_code": x[col]
                .str.strip()
                .str.replace("\\.0$", "", regex=True),
                "industry_sector": sector,
            }
        )
        d = d.loc[d.industry_code.notna()].copy()
        d.loc[d.industry_code.eq("0"), "industry_sector"] = "not_in_industry_universe"
        d["industry_vintage"] = vintage
        rows.append(d.drop_duplicates(["industry_code", "industry_vintage"]))
    out = pd.concat(rows, ignore_index=True)
    write_parquet(out, "acs_industry_concordance.parquet")
    return out


def main():
    paths = [SOURCE_DIR / n for n in EXTRACTS] + [
        OUTPUT_DIR / n
        for n in [
            "acs_county_membership.parquet",
            "puma_components.parquet",
            "standard_puma_to_cpuma.parquet",
        ]
    ]
    factors = pd.concat(
        [
            x.drop_duplicates()
            for x in pd.read_csv(
                paths[0], usecols=["YEAR", "CPI99"], chunksize=CHUNK_SIZE
            )
        ]
    ).drop_duplicates()
    base = factors.loc[factors.YEAR.eq(2024), "CPI99"].item()
    geo, components, cpuma = [pd.read_parquet(p) for p in paths[2:5]]
    names = [
        "acs_persons",
        "acs_households_all",
        "acs_households",
        "acs_household_migration",
        "acs_household_outmovers",
        "acs_destination_households_with_migrants",
    ]
    industry = industry_concordance()
    writers = {}
    schemas = {}
    counts = {n: 0 for n in names}

    def append(frame, name):
        if frame.empty:
            return
        frame = frame.copy()
        for col in [
            "worked_from_home",
            "moved_last_year",
            "nonpositive_income",
            "own_minor_child_present",
            "roommates_present",
            "spouse_partner_present",
            "minor_children_present",
            "living_alone",
            "current_migration_puma_check",
            "bay_outmover",
            "employed",
        ]:
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
        for d in record_chunks(geo, components, cpuma):
            d = prepare_variables(d, base, industry)
            h = household_records(d)
            selected = d.groupby(HHKEY, as_index=False).agg(
                any_bay_outmover=("bay_outmover", "max")
            )
            h = h.merge(selected, on=HHKEY, validate="one_to_one")
            bay = h.loc[h.current_bay_membership.eq("inside")]
            for frame, name in [
                (d, "acs_persons"),
                (h, "acs_households_all"),
                (bay, "acs_households"),
                (
                    h.loc[
                        h.current_support_intersects_bay
                        | h.prior_support_intersects_bay
                    ],
                    "acs_household_migration",
                ),
                (
                    h.loc[h.householder_observed & h.bay_outmover.eq(True)],
                    "acs_household_outmovers",
                ),
                (h.loc[h.any_bay_outmover], "acs_destination_households_with_migrants"),
            ]:
                append(frame, name)
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
