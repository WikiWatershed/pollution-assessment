"""
Created by Sara Geleskie Damiano
"""
#%%
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
    srgd_staging_api_key,
    srgd_mmw_user,
    srgd_mmw_pass,
    save_path,
    csv_path,
    geojson_path,
    json_dump_path,
    csv_extension,
)

# Create an API user
mmw_run = ModelMyWatershedAPI(srgd_staging_api_key, save_path, True)
# Authenticate with MMW
mmw_run.login(mmw_user=srgd_mmw_user, mmw_pass=srgd_mmw_pass)

land_use_layer = "2019_2019"
# ^^ NOTE:  This is the default, but I also specified it in the code below
stream_layer = "nhdhr"
# ^^ NOTE:  This is the default.  I did not specify a stream override.
weather_layer = "NASA_NLDAS_2000_2019"

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
log_file = save_path + "run_whole_basin_srat.log"

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
def format_for_srat(huc12_id, model_output):
    formatted = {
        "huc12": huc12_id,
        # Tile Drain may be calculated by future versions of
        # Mapshed. The SRAT API requires a placeholder
        "tpload_tiledrain": 0,
        "tnload_tiledrain": 0,
        "tssload_tiledrain": 0,
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
def run_srat(gwlfe_watereshed_result):
    try:
        data = [format_for_srat(id, w) for id, w in gwlfe_watereshed_result.items()]
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
    "wikisrat_huc_sources": [],
    "wikisrat_catchment_loading_rates": [],
    "wikisrat_reach_concentrations": [],
    "wikisrat_catchment_sources": [],
}


#%%
for huc8_id, huc8 in hucs_to_run.groupby(by=["huc8"]):
    logging.info(huc8_id)

    wikisrat_result = None
    _, wikisrat_result = mmw_run.read_dumped_result(
        "wikiSRAT", "HUC8_{}_SRAT".format(huc8_id)
    )

    if wikisrat_result is None:
        logging.info("  Loading GWLF-E Results")
        huc8_dict = {}
        for _, huc_row in huc8.iterrows():
            gwlfe_sb_result = None
            gwlfe_sb_job_dict, gwlfe_sb_result = mmw_run.read_dumped_result(
                mmw_run.subbasin_run_endpoint,
                "{}_{}_{}".format(huc_row["huc"], land_use_layer, weather_layer),
                json_dump_path
                + "{}_{}_{}_{}.json".format(
                    huc_row["huc"],
                    land_use_layer,
                    "USEPA_1960_1990",
                    mmw_run._pprint_endpoint(mmw_run.subbasin_run_endpoint),
                ),
                "SummaryLoads",
            )
            if gwlfe_sb_result is not None:
                huc8_dict[huc_row["huc"]] = copy.deepcopy(
                    gwlfe_sb_result["HUC12s"][huc_row["huc"]]["Raw"]
                )
            else:
                logging.warning(
                    "No GWLF-E data from {} ({})".format(
                        huc_row["huc"], huc_row["name"]
                    )
                )

        logging.info("  Running SRAT")
        wikisrat_result = run_srat(huc8_dict)

        if wikisrat_result is not None:
            mock_job_dict = {
                "job_label": "HUC8_{}_SRAT".format(huc8_id),
                "request_host": "wikiSRAT",
                "request_endpoint": "wikiSRAT",
                "start_job_status": "complete",
                "job_result_status": "complete",
                "result_response": {"result": wikisrat_result},
            }
            mmw_run.dump_job_json(mock_job_dict)

    if wikisrat_result is not None:
        logging.info("  Framing data")
        for huc12, huc12_wikisrat in wikisrat_result["huc12s"].items():
            huc12_result = dict.fromkeys(cum_results.keys(), None)
            h12_cpy = copy.deepcopy(huc12_wikisrat)
            h12_catches = h12_cpy.pop("catchments")
            huc12_result["wikisrat_huc_sources"] = (
                format_wikisrat_return(h12_cpy, "huc12")["sources"].copy().reset_index()
            )

            catch_results = {
                "wikisrat_catchment_loading_rates": [],
                "wikisrat_reach_concentrations": [],
                "wikisrat_catchment_sources": [],
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
                catch_results["wikisrat_catchment_sources"].append(
                    catch_loads["sources"].copy().reset_index()
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
wikisrat_huc_sources = pd.concat(cum_results["wikisrat_huc_sources"], ignore_index=True)
wikisrat_catchment_loading_rates = pd.concat(
    cum_results["wikisrat_catchment_loading_rates"], ignore_index=True
)
wikisrat_reach_concentrations = pd.concat(
    cum_results["wikisrat_reach_concentrations"], ignore_index=True
)
wikisrat_catchment_sources = pd.concat(
    cum_results["wikisrat_catchment_sources"], ignore_index=True
)

#%%
# save csv's
wikisrat_huc_sources.sort_values(by=["huc"]).reset_index(drop=True).to_csv(
    csv_path + "basin_wikisrat_huc_sources" + csv_extension
)
wikisrat_catchment_loading_rates.sort_values(by=["huc", "comid"]).reset_index(
    drop=True
).to_csv(csv_path + "basin_wikisrat_catchment_loading_rates" + csv_extension)
wikisrat_reach_concentrations.sort_values(by=["huc", "comid"]).reset_index(
    drop=True
).to_csv(csv_path + "basin_wikisrat_reach_concentrations" + csv_extension)
wikisrat_catchment_sources.sort_values(by=["huc", "comid"]).reset_index(
    drop=True
).to_csv(csv_path + "basin_wikisrat_catchment_sources" + csv_extension)


#%%
logging.info("DONE!")

# %%
