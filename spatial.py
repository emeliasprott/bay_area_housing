from contextlib import contextmanager
import zipfile
import os
import pyogrio
import json
import hashlib
import requests
from pathlib import Path
from dotenv import load_dotenv
import geopandas as gpd
import numpy as np
import pandas as pd
from shapely import make_valid

SOURCE_DIR = Path(str(Path(__file__).resolve().parent / "data/source")).expanduser()
OUTPUT_DIR = Path(str(SOURCE_DIR.parent / "processed")).expanduser()
CACHE_DIR = Path(str(SOURCE_DIR / ".cache")).expanduser()
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
SESSION = requests.Session()
load_dotenv(SOURCE_DIR.parent.parent / ".env")
SPATIAL_SOURCE_DIR = SOURCE_DIR / "spatial"
RELATION_DIR = CACHE_DIR / "spatial_relationships"
FORCE_DOWNLOAD = os.getenv("SPATIAL_FORCE_DOWNLOAD", "0") == "1"
FORCE_PROCESS = os.getenv("SPATIAL_FORCE_PROCESS", "0") == "1"
CA_FIPS = 6
AREA_CRS = "EPSG:3310"
WGS84 = "EPSG:4326"
SQ_METERS_PER_SQ_MILE = 2589988.110336
LEGACY_PUMA_SOURCE = {
    "path": SOURCE_DIR / "ipums_puma_2000_tl10.zip",
    "vintage": 2000,
    "geography_type": "standard",
    "state_fields": ["STATEFIP", "STATEFP", "STATE"],
    "puma_fields": ["PUMA", "PUMA00", "PUMACE00", "PUMA5CE00"],
}
TRACT_URLS = {
    2000: "https://www2.census.gov/geo/tiger/TIGER2010/TRACT/2000/tl_2010_06_tract00.zip",
    2010: "https://www2.census.gov/geo/pvs/tiger2010st/06_California/06/tl_2010_06_tract10.zip",
    2020: "https://www2.census.gov/geo/tiger/TIGER2020/TRACT/tl_2020_06_tract.zip",
}


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


def read_tracts(vintage):
    url = TRACT_URLS[vintage]
    path = download(url, SPATIAL_SOURCE_DIR / Path(url).name, force=FORCE_DOWNLOAD)
    frame = gpd.read_file(vector_path_in_zip(path))
    frame["tract_geoid"] = (
        frame[tract_geoid_field(frame, vintage)]
        .astype("string")
        .str.replace("\\.0$", "", regex=True)
        .str.zfill(11)
    )
    frame["county_fips"] = frame["tract_geoid"].str[:5]
    frame["tract_vintage"] = vintage
    frame = add_area_fields(repair_geometry(frame)).to_crs(AREA_CRS)
    return frame[
        [
            "tract_vintage",
            "tract_geoid",
            "county_fips",
            "land_area_sq_miles",
            "water_area_sq_miles",
            "geometry",
        ]
    ]


def relationship_file(name, url=None):
    url = url or "https://usa.ipums.org/usa/resources/volii/" + name
    return download(url, RELATION_DIR / name, force=FORCE_DOWNLOAD)


def components():
    county_rows = []
    path = relationship_file(
        "2000PUMAsASCII.txt", "https://usa.ipums.org/usa/volii/2000PUMAsASCII.txt"
    )
    for line in path.read_text(encoding="latin-1").splitlines():
        if line.startswith(" 781"):
            fields = line.split()
            if fields[1] == "06":
                county_rows.append((2000, 6, int(fields[3]), fields[1] + fields[4]))
    county = pd.DataFrame(
        county_rows, columns=["vintage", "state_fips", "puma", "county_fips"]
    ).drop_duplicates()
    tracts = []
    for vintage in [2010, 2020]:
        name = f"{vintage}_Census_Tract_to_{vintage}_PUMA." + (
            "txt" if vintage == 2010 else "csv"
        )
        url = (
            "https://www2.census.gov/geo/docs/maps-data/data/rel/" + name
            if vintage == 2010
            else None
        )
        frame = pd.read_csv(relationship_file(name, url), dtype="string")
        frame = frame.loc[frame.STATEFP.eq("06")].copy()
        frame["county_fips"] = frame.STATEFP + frame.COUNTYFP
        frame["tract_geoid"] = frame.county_fips + frame.TRACTCE
        frame["puma"] = frame.PUMA5CE.astype(int)
        frame["vintage"], frame["state_fips"] = (vintage, 6)
        tracts.append(
            frame[["vintage", "state_fips", "puma", "county_fips", "tract_geoid"]]
        )
    tract_components = pd.concat(tracts, ignore_index=True)
    county = pd.concat(
        [county, tract_components[county.columns]], ignore_index=True
    ).drop_duplicates()
    coarse = []
    for kind, name, code in [
        ("migration", "ipums_usa_puma_migpuma_2000.xlsx", "Migration PUMA (MIGPUMA1)"),
        (
            "workplace",
            "ipums_usa_puma_pwpuma_2000.xlsx",
            "Place-of-Work PUMA (PWPUMA00)",
        ),
    ]:
        frame = pd.read_excel(relationship_file(name))
        frame = frame.rename(
            columns={
                "State FIPS Code (STATEFIP)": "state_fips",
                "PUMA": "puma",
                code: "coarse_puma",
            }
        )
        frame = frame.loc[frame.state_fips.eq(6), ["state_fips", "puma", "coarse_puma"]]
        frame["vintage"], frame["geography_type"] = (2000, kind)
        coarse.append(frame)
    for vintage in [2010, 2020]:
        frame = pd.read_excel(
            relationship_file(f"puma_migpuma1_pwpuma00_{vintage}.xls"),
            header=2 if vintage == 2010 else 0,
        )
        frame.columns = ["state_fips", "puma", "coarse_state_fips", "coarse_puma"]
        frame = frame.loc[frame.state_fips.eq(6)].copy()
        frame["vintage"], frame["geography_type"] = (vintage, "migration_workplace")
        coarse.append(frame.drop(columns="coarse_state_fips"))
    cpuma = []
    for vintage, suffix in [(2000, "assignments"), (2010, "components")]:
        frame = pd.read_excel(
            relationship_file(f"CPUMA0010_PUMA{vintage}_{suffix}.xls")
        )
        frame = frame.rename(
            columns={
                "State_FIPS": "state_fips",
                "PUMA": "puma",
                "CPUMA0010": "cpuma0010",
            }
        )
        frame = frame.loc[frame.state_fips.eq(6), ["state_fips", "puma", "cpuma0010"]]
        frame["vintage"] = vintage
        frame["mapping_method"] = "IPUMS_CPUMA0010_component_assignment"
        cpuma.append(frame)
    return (
        county,
        tract_components,
        pd.concat(coarse, ignore_index=True),
        pd.concat(cpuma, ignore_index=True),
    )


def support_counties(county, coarse, cpuma):
    standard = county.assign(geography_type="standard")
    combined = county.merge(
        coarse, on=["vintage", "state_fips", "puma"], validate="many_to_many"
    )
    combined = combined.drop(columns="puma").rename(columns={"coarse_puma": "puma"})
    consistent = county.loc[county.vintage.eq(2010)].merge(
        cpuma.loc[cpuma.vintage.eq(2010)],
        on=["vintage", "state_fips", "puma"],
        validate="many_to_one",
    )
    consistent = (
        consistent.drop(columns="puma")
        .rename(columns={"cpuma0010": "puma"})
        .assign(geography_type="cpuma0010")
    )
    keys = ["geography_type", "vintage", "state_fips", "puma", "county_fips"]
    result = pd.concat(
        [standard[keys], combined[keys], consistent[keys]], ignore_index=True
    ).drop_duplicates()
    result["county_in_bay"] = result.county_fips.isin(BAY_COUNTY_FIPS.values())
    groups = result.groupby(keys[:-1]).county_in_bay
    result["bay_membership"] = np.select(
        [groups.transform("all"), ~groups.transform("any")],
        ["inside", "outside"],
        default="mixed",
    )
    result["membership_basis"] = "published_county_and_PUMA_components"
    return result


def read_pumas(tracts, tract_components, coarse, cpuma, membership):
    spec = LEGACY_PUMA_SOURCE
    legacy, _ = read_california_zip(spec["path"], spec["state_fields"])
    legacy["puma"] = pd.to_numeric(
        legacy[first_existing(legacy.columns, spec["puma_fields"])]
    )
    legacy["state_fips"], legacy["vintage"] = (6, 2000)
    keys = ["vintage", "state_fips", "puma"]
    standard = [repair_geometry(legacy).to_crs(AREA_CRS)[keys + ["geometry"]]]
    for vintage in [2010, 2020]:
        native = tracts.loc[
            tracts.tract_vintage.eq(vintage), ["tract_geoid", "geometry"]
        ]
        native = native.merge(
            tract_components.loc[tract_components.vintage.eq(vintage)],
            on="tract_geoid",
            validate="one_to_one",
        )
        standard.append(native.dissolve(by=keys, as_index=False)[keys + ["geometry"]])
    standard = gpd.GeoDataFrame(
        pd.concat(standard, ignore_index=True), crs=AREA_CRS
    ).assign(geography_type="standard")
    combined = standard.drop(columns="geography_type").merge(
        coarse, on=keys, validate="one_to_many"
    )
    combined = combined.drop(columns="puma").rename(columns={"coarse_puma": "puma"})
    combined = combined.dissolve(by=["geography_type", *keys], as_index=False)
    consistent = standard.loc[standard.vintage.eq(2010)].merge(
        cpuma.loc[cpuma.vintage.eq(2010)], on=keys, validate="one_to_one"
    )
    consistent = (
        consistent.drop(columns="puma")
        .rename(columns={"cpuma0010": "puma"})
        .assign(geography_type="cpuma0010")
    )
    consistent = consistent.dissolve(by=["geography_type", *keys], as_index=False)
    columns = ["geography_type", *keys, "geometry"]
    result = gpd.GeoDataFrame(
        pd.concat(
            [standard[columns], combined[columns], consistent[columns]],
            ignore_index=True,
        ),
        crs=AREA_CRS,
    )
    result = result.merge(
        membership[columns[:-1] + ["bay_membership"]].drop_duplicates(),
        on=columns[:-1],
        validate="one_to_one",
    )
    result["support_id"] = "06" + result.puma.astype(int).astype(str).str.zfill(5)
    return result


def observation_supports(pumas, counties, tracts):
    p = pumas.rename(columns={"geography_type": "support_type"}).copy()
    c = counties.assign(
        support_type="county", vintage=2010, support_id=counties.county_fips
    )
    t = tracts.rename(
        columns={"tract_vintage": "vintage", "tract_geoid": "support_id"}
    ).assign(support_type="tract")
    cols = ["support_type", "vintage", "support_id", "geometry"]
    return gpd.GeoDataFrame(
        pd.concat([p[cols], c[cols], t[cols]], ignore_index=True), crs=AREA_CRS
    )


def build_support_overlap(supports, cells, membership):
    source = supports.copy()
    source["source_area_m2"] = source.geometry.area
    grid = (
        cells[["tract_geoid", "county_fips", "geometry"]]
        .rename(columns={"tract_geoid": "cell_id"})
        .copy()
    )
    grid["cell_area_m2"] = grid.geometry.area
    out = gpd.overlay(source, grid, how="intersection", keep_geom_type=False)
    out["intersection_area_m2"] = out.geometry.area
    out = out.loc[
        (out.intersection_area_m2 > 1)
        & (out.intersection_area_m2 / out.cell_area_m2 > 1e-06)
    ].copy()
    member = membership.rename(columns={"geography_type": "support_type"}).copy()
    member["support_id"] = "06" + member.puma.astype(int).astype(str).str.zfill(5)
    allowed = pd.MultiIndex.from_frame(
        member[["support_type", "vintage", "support_id", "county_fips"]]
    )
    correct_county = pd.MultiIndex.from_frame(
        out[["support_type", "vintage", "support_id", "county_fips"]]
    ).isin(allowed)
    correct_county |= out.support_type.eq("county") & out.support_id.eq(out.county_fips)
    correct_county |= out.support_type.eq("tract") & out.support_id.str[:5].eq(
        out.county_fips
    )
    out = out.loc[correct_county].copy()
    keys = ["support_type", "vintage", "support_id", "cell_id"]
    out = out.groupby(keys, as_index=False).agg(
        intersection_area_m2=("intersection_area_m2", "sum"),
        source_area_m2=("source_area_m2", "first"),
        cell_area_m2=("cell_area_m2", "first"),
    )
    out["share_of_source_area"] = out.intersection_area_m2 / out.source_area_m2
    out["share_of_cell_area"] = out.intersection_area_m2 / out.cell_area_m2
    total = out.groupby(keys[:3]).share_of_source_area.transform("sum")
    out["uncovered_source_area_share"] = (1 - total).clip(lower=0)
    out["allocation_basis"] = "geometry_including_water_not_population_or_housing"
    missing = source[keys[:3]].merge(
        out[keys[:3]].drop_duplicates(), on=keys[:3], how="left", indicator=True
    )
    missing = missing.loc[missing._merge.eq("left_only"), keys[:3]].assign(
        cell_id=pd.NA,
        share_of_source_area=0.0,
        share_of_cell_area=0.0,
        uncovered_source_area_share=1.0,
        allocation_basis="unmatched",
    )
    if missing.empty:
        return out.sort_values(keys)
    missing = missing.reindex(columns=out.columns)
    for col in out.select_dtypes("number").columns:
        missing[col] = pd.to_numeric(missing[col]).astype(out[col].dtype)
    return pd.concat([out, missing], ignore_index=True).sort_values(keys)


def census_2010_anchors(cells):
    variables = {
        "housing_units": "H003001",
        "occupied_housing_units": "H003002",
        "vacant_housing_units": "H003003",
        "mortgaged_owner_households": "H004002",
        "free_clear_owner_households": "H004003",
        "renter_households": "H004004",
        "household_population": "P016001",
        "population": "P001001",
    }
    frames = []
    for county in sorted(cells["county_fips"].unique()):
        query = {
            "get": ",".join(variables.values()),
            "for": "tract:*",
            "in": f"state:{county[:2]} county:{county[2:]}",
        }
        digest = hashlib.sha256(json.dumps(query, sort_keys=True).encode()).hexdigest()[
            :12
        ]
        cache = CACHE_DIR / "census/anchors" / f"sf1_2010_{county}_{digest}.json"
        payload = census_json(
            "https://api.census.gov/data/2010/dec/sf1",
            query,
            cache,
            force=FORCE_DOWNLOAD,
        )
        frame = pd.DataFrame(payload[1:], columns=payload[0])
        frame["cell_id"] = frame["state"] + frame["county"] + frame["tract"]
        frame = frame.rename(columns={v: k for k, v in variables.items()})
        for column in variables:
            frame[column] = pd.to_numeric(frame[column], errors="coerce").where(
                lambda x: x.ge(0)
            )
        frames.append(frame[["cell_id", *variables]])
    result = pd.concat(frames, ignore_index=True)
    result = (
        cells[["tract_geoid"]]
        .rename(columns={"tract_geoid": "cell_id"})
        .merge(result, on="cell_id", how="left", validate="one_to_one")
    )
    if result[list(variables)].isna().any().any():
        raise ValueError(
            "Census 2010 anchors are missing for retained integration cells"
        )
    result["owner_households"] = (
        result["mortgaged_owner_households"] + result["free_clear_owner_households"]
    )
    result["reference_date"] = "2010-04-01"
    result["source"] = "2010_decennial_sf1"
    return result


def allocation_crosswalks(overlap, anchors, tract_components, cpuma):
    keys = ["support_type", "vintage", "support_id"]
    frame = overlap.merge(anchors, on="cell_id", how="left", validate="many_to_one")
    outputs = []
    for basis, count in [
        ("population", "population"),
        ("households", "occupied_housing_units"),
        ("housing_units", "housing_units"),
    ]:
        weights = frame[keys + ["cell_id", "uncovered_source_area_share"]].copy()
        mass = frame.share_of_cell_area * frame[count]
        denominator = mass.groupby([frame[k] for k in keys]).transform("sum")
        covered = 1 - frame.uncovered_source_area_share
        weights["allocation"] = mass / denominator.where(denominator.gt(0)) * covered
        weights["basis"] = basis
        weights["reference_year"] = 2010
        weights["method"] = "fixed_2010_tract_anchor_uniform_within_tract"
        weights["county_fips"] = weights.cell_id.str[:5]
        weights["target_in_bay"] = weights.county_fips.isin(BAY_COUNTY_FIPS.values())
        outputs.append(weights)
    weights = pd.concat(outputs, ignore_index=True)
    target = tract_components.loc[tract_components.vintage.eq(2010)].merge(
        cpuma.loc[cpuma.vintage.eq(2010)],
        on=["vintage", "state_fips", "puma"],
        validate="many_to_one",
    )
    target = target[["tract_geoid", "cpuma0010"]].rename(
        columns={"tract_geoid": "cell_id"}
    )
    to_cpuma = weights.merge(target, on="cell_id", how="left", validate="many_to_one")
    to_cpuma = to_cpuma.groupby(
        keys + ["basis", "cpuma0010"], dropna=False, as_index=False
    ).allocation.sum(min_count=1)
    to_cpuma["mapping_method"] = "allocated_to_2010_CPUMA_not_observed_membership"
    workplace = to_cpuma.loc[
        to_cpuma.support_type.isin(["workplace", "migration_workplace"])
        & to_cpuma.basis.eq("population")
    ].copy()
    targets = workplace.groupby(keys).cpuma0010.transform("nunique")
    workplace["allocation"] = np.where(targets.eq(1), 1.0, np.nan)
    workplace["basis"] = "workplace_jobs"
    workplace["mapping_method"] = np.where(
        targets.eq(1),
        "single_CPUMA_support",
        "job_split_unobserved_keep_workplace_node",
    )
    to_cpuma = pd.concat([to_cpuma, workplace], ignore_index=True)
    return (weights, to_cpuma)


def puma2020_cpuma_projection():
    path = SOURCE_DIR / "PUMA2010_PUMA2020_crosswalk.csv"
    if path.exists():
        frame = pd.read_csv(path, thousands=",")
    else:
        path = relationship_file("PUMA2010_PUMA2020_crosswalk.xls")
        frame = pd.read_excel(path)
    frame = frame.loc[frame.State20.eq(6)].rename(
        columns={"State10": "state_fips", "PUMA10": "puma"}
    )
    national = pd.read_csv(SOURCE_DIR / "CPUMA0010_PUMA2010_components.csv")
    national = national.rename(
        columns={"State_FIPS": "state_fips", "PUMA": "puma", "CPUMA0010": "cpuma0010"}
    )
    frame = frame.merge(
        national[["state_fips", "puma", "cpuma0010"]],
        on=["state_fips", "puma"],
        how="left",
        validate="many_to_one",
    )
    assert frame.cpuma0010.notna().all(), "Unmapped intersection must not disappear"
    frame = frame.rename(columns={"state_fips": "target_state_fips"})
    rows = []
    for reference in [2010, 2020]:
        column = "Part_Pop" + str(reference)[2:]
        frame[column] = pd.to_numeric(
            frame[column].astype(str).str.replace(",", "", regex=False)
        )
        grouped = frame.groupby(
            ["State20", "PUMA20", "target_state_fips", "cpuma0010"], as_index=False
        )[column].sum()
        source_pop = "PUMA20_Pop" + str(reference)[2:]
        check = frame.groupby(["State20", "PUMA20"]).agg(
            supplied=(source_pop, "first"),
            intersections=(column, "sum"),
            distinct=(source_pop, "nunique"),
        )
        grouped = grouped.merge(
            check[["supplied", "intersections"]],
            on=["State20", "PUMA20"],
            validate="many_to_one",
        )
        grouped["published_population_difference"] = (
            grouped.supplied - grouped.intersections
        )
        grouped["allocation"] = grouped[column] / grouped.intersections
        grouped = grouped.rename(
            columns={
                "supplied": "published_source_population",
                "intersections": "summed_intersection_population",
            }
        )
        grouped["reference_year"] = reference
        rows.append(
            grouped.rename(
                columns={
                    "State20": "state_fips",
                    "PUMA20": "puma",
                    column: "intersection_population",
                }
            )
        )
    result = pd.concat(rows, ignore_index=True)
    result["vintage"] = 2020
    result["mapping_method"] = "IPUMS_population_projection_not_CPUMA0010_observation"
    return result


def county_membership_lookup(membership):
    keys = ["geography_type", "vintage", "state_fips", "puma"]
    result = membership.groupby(keys, as_index=False).agg(
        county_count=("county_fips", "nunique"),
        county_fips_exact=("county_fips", "first"),
        bay_membership=("bay_membership", "first"),
    )
    result["county_fips_exact"] = result.county_fips_exact.where(
        result.county_count.eq(1)
    )
    result["mapping_basis"] = "published_components_no_population_allocation"
    return result


def harmonized_cpuma_links(cpuma, projection, anchored):
    exact = cpuma[["vintage", "state_fips", "puma", "cpuma0010"]].copy()
    exact["target_state_fips"] = exact.state_fips
    exact["allocation"] = 1.0
    exact["basis"] = "published_assignment"
    exact["reference_year"] = 2010
    exact["mapping_method"] = "observed_consistent_area_assignment"
    estimated = projection[
        [
            "vintage",
            "state_fips",
            "puma",
            "target_state_fips",
            "cpuma0010",
            "allocation",
            "reference_year",
            "mapping_method",
        ]
    ].copy()
    estimated["basis"] = "population_" + estimated.reference_year.astype(str)
    hh = anchored.loc[
        anchored.support_type.eq("standard")
        & anchored.vintage.eq(2020)
        & anchored.basis.eq("households")
    ].copy()
    hh["state_fips"] = hh.support_id.str[:2].astype(int)
    hh["target_state_fips"] = hh.state_fips
    hh["puma"] = hh.support_id.str[2:].astype(int)
    hh["basis"] = "households_2010"
    hh["reference_year"] = 2010
    hh["mapping_method"] = "fixed_2010_occupied_household_anchor_sensitivity"
    links = pd.concat([exact, estimated, hh[estimated.columns]], ignore_index=True)
    links["source_covered_share"] = links.groupby(
        ["vintage", "state_fips", "puma", "basis"]
    ).allocation.transform("sum")
    links["unallocated_share"] = 1 - links.source_covered_share
    return links


def polygon_county_audit(pumas, membership, tracts):
    a = (
        pumas[["geography_type", "vintage", "state_fips", "puma", "geometry"]]
        .to_crs(AREA_CRS)
        .copy()
    )
    a["source_area"] = a.geometry.area
    pieces = []
    for vintage, g in a.groupby("vintage"):
        b = (
            tracts.loc[tracts.tract_vintage.eq(vintage), ["county_fips", "geometry"]]
            .to_crs(AREA_CRS)
            .dissolve(by="county_fips", as_index=False)
        )
        pieces.append(gpd.overlay(g, b, how="intersection", keep_geom_type=True))
    x = pd.concat(pieces, ignore_index=True)
    x["intersection_area"] = x.geometry.area
    x["source_area_share"] = x.intersection_area / x.source_area
    keys = ["geography_type", "vintage", "state_fips", "puma", "county_fips"]
    x = x.merge(
        membership[keys].drop_duplicates().assign(published_component=True),
        on=keys,
        how="left",
        validate="many_to_one",
    )
    x["published_component"] = x.published_component.eq(True)
    return pd.DataFrame(x.drop(columns="geometry"))


def national_acs_counties():
    rows = []
    for line in (
        relationship_file(
            "2000PUMAsASCII.txt", "https://usa.ipums.org/usa/volii/2000PUMAsASCII.txt"
        )
        .read_text(encoding="latin-1")
        .splitlines()
    ):
        if line.startswith(" 781"):
            f = line.split()
            if f[1].isdigit():
                rows.append((2000, int(f[1]), int(f[3]), f[1] + f[4]))
    county = pd.DataFrame(
        rows, columns=["vintage", "state_fips", "puma", "county_fips"]
    ).drop_duplicates()
    for vintage in [2010, 2020]:
        name = f"{vintage}_Census_Tract_to_{vintage}_PUMA." + (
            "txt" if vintage == 2010 else "csv"
        )
        d = pd.read_csv(RELATION_DIR / name, dtype="string")
        a = pd.DataFrame(
            {
                "vintage": vintage,
                "state_fips": d.STATEFP.astype(int),
                "puma": d.PUMA5CE.astype(int),
                "county_fips": d.STATEFP + d.COUNTYFP,
            }
        )
        county = pd.concat([county, a], ignore_index=True).drop_duplicates()
    coarse = []
    for kind, name, code in [
        ("migration", "ipums_usa_puma_migpuma_2000.xlsx", "Migration PUMA (MIGPUMA1)"),
        (
            "workplace",
            "ipums_usa_puma_pwpuma_2000.xlsx",
            "Place-of-Work PUMA (PWPUMA00)",
        ),
    ]:
        d = pd.read_excel(RELATION_DIR / name).rename(
            columns={
                "State FIPS Code (STATEFIP)": "state_fips",
                "PUMA": "puma",
                code: "coarse_puma",
            }
        )
        d = d[["state_fips", "puma", "coarse_puma"]].copy()
        d["vintage"] = 2000
        d["geography_type"] = kind
        coarse.append(d)
    for vintage in [2010, 2020]:
        d = pd.read_excel(
            RELATION_DIR / f"puma_migpuma1_pwpuma00_{vintage}.xls",
            header=2 if vintage == 2010 else 0,
        )
        d.columns = ["state_fips", "puma", "coarse_state_fips", "coarse_puma"]
        d = d.drop(columns="coarse_state_fips")
        d["vintage"] = vintage
        d["geography_type"] = "migration_workplace"
        coarse.append(d)
    c = pd.concat(coarse, ignore_index=True)
    combined = (
        county.merge(c, on=["vintage", "state_fips", "puma"], validate="many_to_many")
        .drop(columns="puma")
        .rename(columns={"coarse_puma": "puma"})
    )
    out = pd.concat(
        [county.assign(geography_type="standard"), combined], ignore_index=True
    ).drop_duplicates()
    keys = ["geography_type", "vintage", "state_fips", "puma"]
    out["in_bay"] = out.county_fips.isin(BAY_COUNTY_FIPS.values())
    g = out.groupby(keys).in_bay
    out["bay_membership"] = np.select(
        [g.transform("all"), ~g.transform("any")], ["inside", "outside"], "mixed"
    )
    write_parquet(out.drop(columns="in_bay"), "acs_county_components.parquet")
    return county_membership_lookup(out)


def main():
    names = [
        "puma_county_membership",
        "puma_geography",
        "tract_geography",
        "county_geography",
        "cpuma_geography",
        "support_counties",
        "standard_puma_to_cpuma",
        "puma_components",
        "tract_puma_components",
        "support_overlap",
        "spatial_anchors_2010",
        "support_to_cells",
        "support_to_cpuma",
        "puma2020_to_cpuma",
        "acs_cpuma_links",
        "polygon_county_audit",
        "acs_county_membership",
        "acs_county_components",
    ]
    if (
        not FORCE_PROCESS
        and (not FORCE_DOWNLOAD)
        and all(((OUTPUT_DIR / (n + ".parquet")).exists() for n in names))
    ):
        print(
            "Using local geography and crosswalks (set SPATIAL_FORCE_PROCESS=1 to rebuild)"
        )
        return
    county_components, tract_components, coarse, cpuma = components()
    write_parquet(national_acs_counties(), "acs_county_membership.parquet")
    membership = support_counties(county_components, coarse, cpuma)
    write_parquet(
        county_membership_lookup(membership), "puma_county_membership.parquet"
    )
    tracts = gpd.GeoDataFrame(
        pd.concat([read_tracts(v) for v in [2000, 2010, 2020]], ignore_index=True),
        crs=AREA_CRS,
    )
    tracts["in_bay_county"] = tracts.county_fips.isin(BAY_COUNTY_FIPS.values())
    cells = tracts.loc[tracts.tract_vintage.eq(2010)].copy()
    counties = cells[["county_fips", "geometry"]].dissolve(
        by="county_fips", as_index=False
    )
    counties["in_bay_county"] = counties.county_fips.isin(BAY_COUNTY_FIPS.values())
    pumas = read_pumas(tracts, tract_components, coarse, cpuma, membership)
    supports = observation_supports(pumas, counties, tracts)
    overlap = build_support_overlap(supports, cells, membership)
    anchors = census_2010_anchors(cells)
    weights, to_cpuma = allocation_crosswalks(overlap, anchors, tract_components, cpuma)
    projection = puma2020_cpuma_projection()
    write_parquet(
        harmonized_cpuma_links(cpuma, projection, to_cpuma), "acs_cpuma_links.parquet"
    )
    write_parquet(
        polygon_county_audit(pumas, membership, tracts), "polygon_county_audit.parquet"
    )
    for frame, name in [
        (pumas, "puma_geography"),
        (tracts, "tract_geography"),
        (counties, "county_geography"),
        (pumas.loc[pumas.geography_type.eq("cpuma0010")], "cpuma_geography"),
    ]:
        write_parquet(frame.to_crs(WGS84), name + ".parquet")
    for frame, name in [
        (membership, "support_counties"),
        (cpuma, "standard_puma_to_cpuma"),
        (coarse, "puma_components"),
        (tract_components, "tract_puma_components"),
        (overlap, "support_overlap"),
        (anchors, "spatial_anchors_2010"),
        (weights, "support_to_cells"),
        (to_cpuma, "support_to_cpuma"),
        (projection, "puma2020_to_cpuma"),
    ]:
        write_parquet(frame, name + ".parquet")
    print(
        f"Wrote statewide supports, official memberships and {len(weights):,} allocation rows"
    )


def repair_geometry(frame):
    frame = frame.loc[frame.geometry.notna() & ~frame.geometry.is_empty].copy()
    invalid = ~frame.geometry.is_valid
    if invalid.any():
        frame.loc[invalid, "geometry"] = frame.loc[invalid, "geometry"].map(make_valid)
    frame = frame.loc[frame.geometry.notna() & ~frame.geometry.is_empty].copy()
    return frame


def first_existing(columns, candidates):
    lookup = {str(column).upper(): column for column in columns}
    for candidate in candidates:
        if candidate.upper() in lookup:
            return lookup[candidate.upper()]
    return None


def vector_path_in_zip(path):
    path = Path(path).resolve()
    with zipfile.ZipFile(path) as archive:
        members = [
            name
            for name in archive.namelist()
            if name.lower().endswith(".shp")
            and (not name.startswith("__MACOSX/"))
            and (not Path(name).name.startswith("._"))
        ]
    if len(members) != 1:
        matches = [
            name for name in members if Path(name).stem.lower() == path.stem.lower()
        ]
        if len(matches) != 1:
            matches = [name for name in members if "puma" in Path(name).stem.lower()]
        if len(matches) != 1:
            raise ValueError(
                f"Cannot select one shapefile inside {path.name}: {members}"
            )
        members = matches
    return f"/vsizip/{path.as_posix()}/{members[0]}"


def read_california_zip(path, state_candidates):
    uri = vector_path_in_zip(path)
    info = pyogrio.read_info(uri)
    state_field = first_existing(info["fields"], state_candidates)
    if state_field is None:
        raise ValueError(f"No state identifier in {path.name}")
    state_dtype = str(dict(zip(info["fields"], info["dtypes"]))[state_field]).lower()
    where = (
        f'"{state_field}" = {CA_FIPS}'
        if any((t in state_dtype for t in ["int", "float"]))
        else f""""{state_field}" IN ('6', '06', '006')"""
    )
    frame = gpd.read_file(uri, where=where, engine="pyogrio")
    if frame.empty:
        raise ValueError(f"No California polygons in {path.name}")
    return (frame, state_field)


def source_area_fields(frame):
    land = first_existing(
        frame.columns,
        ["ALAND", "ALAND20", "ALAND10", "ALAND00", "AREALAND", "LAND_AREA"],
    )
    water = first_existing(
        frame.columns,
        ["AWATER", "AWATER20", "AWATER10", "AWATER00", "AREAWATER", "WATER_AREA"],
    )
    return (land, water)


def add_area_fields(frame):
    frame = frame.copy()
    for field, output in zip(
        source_area_fields(frame), ["land_area_sq_miles", "water_area_sq_miles"]
    ):
        frame[output] = (
            pd.to_numeric(frame[field], errors="coerce") / SQ_METERS_PER_SQ_MILE
            if field
            else np.nan
        )
    return frame


def tract_geoid_field(frame, vintage):
    candidates = {
        2000: ["GEOID00", "CTIDFP00", "GEOID"],
        2010: ["GEOID10", "GEOID"],
        2020: ["GEOID", "GEOID20"],
    }[vintage]
    for field in candidates:
        if field in frame.columns:
            return field
    raise ValueError(
        f"Could not identify {vintage} tract GEOID field. Fields: {list(frame.columns)}"
    )


if __name__ == "__main__":
    main()
