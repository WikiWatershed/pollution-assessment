"""
Created by Sara Geleskie Damiano
"""
#%%
from pathlib import Path
import sys
import logging

# import time
import json
import copy
from typing import Dict
import pytz
from datetime import datetime

import pandas as pd

import geopandas as gpd
import requests
import shapely
from shapely.geometry.multipolygon import MultiPolygon

from modelmw_client import *
from soupsieve import closest

#%%
# Set up the API client
from mmw_secrets import (
    wiki_srat_url,
    wiki_srat_key,
    restoration_csv_path,
    geojson_path,
    csv_extension,
    gwlfe_json_dump_path,restoration_save_path,
    restoration_json_dump_path,
)
land_use_layer = "2019_2019"
# ^^ NOTE:  This is the default, but I also specified it in the code below
stream_layer = "nhdhr"
# ^^ NOTE:  This is the default.  I did not specify a stream override.
weather_layer = "NASA_NLDAS_2000_2019"
used_attenuation = True
used_restoration_sources = [
    "Delaware River Restoration Fund",
]

#%%
# Read location data - shapes from national map
# These are all of the HUC-12's in the HUC-6's 020401, 020402, and 020403
# I got the list from https://hydro.nationalmap.gov/arcgis/rest/services/wbd/MapServer/6/query?f=json&where=((UPPER(huc12)%20LIKE%20%27020401%25%27)%20OR%20(UPPER(huc12)%20LIKE%20%27020402%25%27)%20OR%20(UPPER(huc12)%20LIKE%20%27020403%25%27))&spatialRel=esriSpatialRelIntersects&outFields=OBJECTID%2Csourcefeatureid%2Cloaddate%2Careaacres%2Careasqkm%2Cstates%2Chuc12%2Cname%2Chutype%2Chumod%2Ctohuc%2Cnoncontributingareaacres%2Cnoncontributingareasqkm&orderByFields=OBJECTID%20ASC&outSR=102100

huc12_shapes = gpd.read_file(
    geojson_path + "WBD_HUC12s.json",
)
huc12_shapes = huc12_shapes.set_crs("EPSG:3857").to_crs("EPSG:4326")
huc12_shapes["huc_level"] = 12
huc12_shapes = (
    huc12_shapes.sort_values(by=["huc12"])
    .reset_index(drop=True)
    .rename(columns={"huc12": "huc"})
)
# Fix huc name strings
huc12_shapes["huc"] = huc12_shapes["huc"].astype(str)
huc12_shapes.loc[~huc12_shapes["huc"].str.startswith("0"), "huc"] = (
    "0" + huc12_shapes.loc[~huc12_shapes["huc"].str.startswith("0")]["huc"]
)
hucs_to_run = huc12_shapes
hucs_to_run["huc6"] = huc12_shapes["huc"].str.slice(0, 6)
hucs_to_run["huc8"] = huc12_shapes["huc"].str.slice(0, 8)
hucs_to_run["huc10"] = huc12_shapes["huc"].str.slice(0, 10)

#%% Set up logging
log_file = restoration_save_path + "run_srat_with_bmps.log"

logging.basicConfig(filename=log_file, encoding="utf-8", level=logging.INFO)

root = logging.getLogger()
root.setLevel(logging.INFO)
logging.getLogger("modelmw_client").setLevel(logging.WARN)

# add a handler for when running interactively
if len(root.handlers) < 2:
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    root.addHandler(handler)

this_run_start = (
    pytz.utc.localize(datetime.utcnow())
    .astimezone(pytz.timezone("US/Eastern"))
    .strftime("%Y-%m-%d %H:%M:%S %z")
)


logging.info("Starting script at {}".format(this_run_start))

#%%
# helper functions

# taken from https://github.com/WikiWatershed/model-my-watershed/blob/f9591f390c4f54751bf34019f3cc126f45892ca6/src/mmw/mmw/settings/gwlfe_settings.py#L623-L639
SRAT_KEYS = {
    "Hay/Pasture": "hp",
    "Cropland": "crop",
    "Wooded Areas": "wooded",
    "Open Land": "open",
    "Barren Areas": "barren",
    "Low-Density Mixed": "ldm",
    "Medium-Density Mixed": "mdm",
    "High-Density Mixed": "hdm",
    "Low-Density Open Space": "tiledrain",
    "Farm Animals": "farman",
    "Stream Bank Erosion": "streambank",
    "Subsurface Flow": "subsurface",
    "Wetlands": "wetland",
    "Point Sources": "pointsource",
    "Septic Systems": "septics",
    "TotalLoadingRates": "total",
    "LoadingRateConcentrations": "conc",
}

# taken from https://github.com/WikiWatershed/model-my-watershed/blob/31566fefbb91055c96a32a6279dac5598ba7fc10/src/mmw/apps/modeling/tasks.py#L72-L96
def format_for_srat(huc12_id, model_output, with_attenuation, restoration_sources):
    formatted = {
        "huc12": huc12_id,
        # Tile Drain may be calculated by future versions of
        # Mapshed. The SRAT API requires a placeholder
        "tpload_tiledrain": 0,
        "tnload_tiledrain": 0,
        "tssload_tiledrain": 0,
        "restoration_sources": restoration_sources,
        "with_attenuation": with_attenuation,
    }

    for load in model_output["Loads"]:
        source_key = SRAT_KEYS.get(load["Source"], None)

        if source_key is None:
            continue

        formatted["tpload_" + source_key] = load["TotalP"]
        formatted["tnload_" + source_key] = load["TotalN"]

        if source_key not in ["farman", "subsurface", "septics", "pointsource"]:
            formatted["tssload_" + source_key] = load["Sediment"]

    return formatted


TASK_REQUEST_TIMEOUT = 600

# from https://github.com/WikiWatershed/model-my-watershed/blob/31566fefbb91055c96a32a6279dac5598ba7fc10/src/mmw/apps/modeling/tasks.py#L375-L409
def run_srat(gwlfe_watereshed_result, with_attenuation, restoration_sources):
    try:
        data = [
            format_for_srat(id, w, with_attenuation, restoration_sources)
            for id, w in gwlfe_watereshed_result.items()
        ]
    except Exception as e:
        raise Exception("Formatting sub-basin GWLF-E results failed: %s" % e)

    headers = {"x-api-key": wiki_srat_key}

    try:
        r = requests.post(
            wiki_srat_url,
            headers=headers,
            data=json.dumps(data),
            timeout=600,
        )
    except requests.Timeout:
        raise Exception("Request to SRAT Catchment API timed out")
    except ConnectionError:
        raise Exception("Failed to connect to SRAT Catchment API")

    if r.status_code != 200:
        raise Exception(
            "SRAT Catchment API request failed: %s %s" % (r.status_code, r.text)
        )

    try:
        srat_catchment_result = r.json()
    except ValueError:
        raise Exception("SRAT Catchment API did not return JSON")

    return srat_catchment_result


def format_wikisrat_return(
    in_dict: Dict, id_key: str = "comid", huc: str = ""
) -> pd.DataFrame:
    source_dict = copy.deepcopy(in_dict)
    id_value = source_dict.pop(id_key)
    loads = pd.DataFrame.from_dict(
        source_dict, orient="index", columns=["Value"]
    ).reset_index()
    loads[["Nutrient", "Source"]] = loads["index"].str.split("_", expand=True)
    loads["Nutrient"] = loads["Nutrient"].replace(
        {
            "tpload": "TotalP",
            "tnload": "TotalN",
            "tssload": "Sediment",
            "tploadrate": "TotalP",
            "tnloadrate": "TotalN",
            "tssloadrate": "Sediment",
        }
    )
    loads["Source"] = loads["Source"].replace(SRAT_KEYS.values(), SRAT_KEYS.keys())
    total_keys = ["TotalLoadingRates", "LoadingRateConcentrations"]
    totals = loads.loc[loads["Source"].isin(total_keys)]
    # NOTE:  Individual land use sources are NOT valid when applying restorations, only the totals
    sources = loads.loc[~loads["Source"].isin(total_keys)]
    return_dict = {}
    for key, frame in zip(["totals", "sources"], [totals, sources]):
        frame_w = frame.pivot(columns="Nutrient", index="Source", values="Value")
        if id_key == "huc12":
            frame_w["huc"] = id_value
        if id_key == "comid":
            frame_w["comid"] = id_value
            if huc != "":
                frame_w["huc"] = huc
        frame_w["gwlfe_endpoint"] = "wikisrat"
        frame_w["huc_level"] = 12
        return_dict[key] = frame_w
    return return_dict


#%%
# create empty lists to hold results
cum_results = {
    "wikisrat_catchment_loading_rates": [],
    "wikisrat_reach_concentrations": [],
}


#%%
for huc8_id, huc8 in hucs_to_run.groupby(by=["huc8"]):
    logging.info(huc8_id)
    logging.info("  Loading GWLF-E Results")
    huc8_dict = {}
    for _, huc_row in huc8.iterrows():
        gwlfe_result_file_name = gwlfe_json_dump_path+"{}_{}_{{}}_subbasin_run.json".format(
            huc_row["huc"], land_use_layer, weather_layer
        )
        gwlfe_result_file=None
        for weather_source in[weather_layer,"USEPA_1960_1990"]:
            if Path(gwlfe_result_file_name.format(weather_source)).is_file():
                gwlfe_result_file=gwlfe_result_file_name.format(weather_source)

        gwlfe_sb_result = None
        if gwlfe_result_file is not None:
            f = (
                open(gwlfe_result_file)
            )
            req_dump = json.load(f)
            f.close()
            result_raw = req_dump["result_response"]
            gwlfe_sb_result = copy.deepcopy(result_raw)["result"]
        if gwlfe_sb_result is not None:
            huc8_dict[huc_row["huc"]] = copy.deepcopy(
                gwlfe_sb_result["HUC12s"][huc_row["huc"]]["Raw"]
            )
        else:
            logging.warning(
                "No GWLF-E data from {} ({})".format(huc_row["huc"], huc_row["name"])
            )

    # break
    logging.info("  Running SRAT")
    wikisrat_result = run_srat(huc8_dict, used_attenuation, used_restoration_sources)

    if wikisrat_result is not None:
        with open(
            restoration_json_dump_path
            + "HUC8_{}_wikiSRAT_withBMPs".format(huc8_id)
            + ".json",
            "w",
        ) as fp:
            json.dump(wikisrat_result, fp, indent=2)

    if wikisrat_result is not None:
        logging.info("  Framing data")
        for huc12, huc12_wikisrat in wikisrat_result["huc12s"].items():
            huc12_result = dict.fromkeys(cum_results.keys(), None)
            h12_cpy = copy.deepcopy(huc12_wikisrat)
            h12_catches = h12_cpy.pop("catchments")

            catch_results = {
                "wikisrat_catchment_loading_rates": [],
                "wikisrat_reach_concentrations": [],
            }
            for catchment, catch_sources in h12_catches.items():
                catch_loads = format_wikisrat_return(catch_sources, huc=huc12)
                catch_results["wikisrat_catchment_loading_rates"].append(
                    catch_loads["totals"]
                    .loc[catch_loads["totals"].index == "TotalLoadingRates"]
                    .copy()
                    .reset_index()
                )
                catch_results["wikisrat_reach_concentrations"].append(
                    catch_loads["totals"]
                    .loc[catch_loads["totals"].index == "LoadingRateConcentrations"]
                    .copy()
                    .reset_index()
                )
            for catch_res_key, catch_list in catch_results.items():
                if len(catch_list) > 0:
                    all_catch_frame = pd.concat(catch_list, ignore_index=True)
                    all_catch_frame["gwlfe_endpoint"] = "wikiSRAT"
                    all_catch_frame["huc"] = huc12
                    huc12_result[catch_res_key] = all_catch_frame.copy()

            for result_key, result_frame in huc12_result.items():
                if result_frame is not None:
                    cum_results[result_key].append(result_frame.copy())


#%%
# join various results
wikisrat_catchment_loading_rates = pd.concat(
    cum_results["wikisrat_catchment_loading_rates"], ignore_index=True
)
wikisrat_catchment_loading_rates[
    "restoration_sources"
] = used_restoration_sources * len(wikisrat_catchment_loading_rates.index)
wikisrat_catchment_loading_rates["with_attenuation"] = used_attenuation

wikisrat_reach_concentrations = pd.concat(
    cum_results["wikisrat_reach_concentrations"], ignore_index=True
)
wikisrat_reach_concentrations["restoration_sources"] = used_restoration_sources * len(
    wikisrat_catchment_loading_rates.index
)
wikisrat_reach_concentrations["with_attenuation"] = used_attenuation

#%%
# save csv's
wikisrat_catchment_loading_rates.sort_values(by=["huc", "comid"]).reset_index(
    drop=True
).to_csv(
    restoration_csv_path + "wikisrat_catchment_loading_rates_with_bmps" + csv_extension
)
wikisrat_reach_concentrations.sort_values(by=["huc", "comid"]).reset_index(
    drop=True
).to_csv(
    restoration_csv_path + "wikisrat_reach_concentrations_with_bmps" + csv_extension
)


#%%
logging.info("DONE!")

# %%
