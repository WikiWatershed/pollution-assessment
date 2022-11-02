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
from requests import Request, Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
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
    gwlfe_json_dump_path,
    restoration_save_path,
    restoration_json_dump_path,
)

land_use_layer = "2019_2019"
# ^^ NOTE:  This is the default, but I also specified it in the code below
stream_layer = "nhdhr"
# ^^ NOTE:  This is the default.  I did not specify a stream override.
weather_layer = "NASA_NLDAS_2000_2019"
used_attenuation = True
funding_source_groups = {
    "No restoration or protection": [],
    "Direct WPF Restoration": [
        "Delaware River Restoration Fund",
    ],
    "Direct and Indirect WPF Restoration": [
        "Delaware River Restoration Fund",
        "Delaware River Operational Fund",
        "Delaware Watershed Conservation Fund",
    ],
    "All Restoration": [
        "Delaware River Restoration Fund",
        "Delaware River Operational Fund",
        "Delaware Watershed Conservation Fund",
        "PADEP",
        "NJDEP",
    ],
    "Direct WPF Protection": [
        "Delaware River Watershed Protection Fund - Forestland Capital Grants"
    ],
}

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
    "Total Local Load": "total",
    "Reach Concentration": "conc",
    "Point Source Derived Concentration": "conc_ptsource",
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
    }
    if restoration_sources != []:
        formatted["restoration_sources"] = restoration_sources
        formatted["with_attenuation"] = with_attenuation

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


# create a TimeoutHTTPAdapter to enforce a default timeout on the session
# from https://findwork.dev/blog/advanced-usage-python-requests-timeouts-retries-hooks/
class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self.timeout = TASK_REQUEST_TIMEOUT
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)


retry_strategy = Retry(
    total=10,
    backoff_factor=1,
    status_forcelist=[413, 429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE"],
    raise_on_status=True,
)
adapter = TimeoutHTTPAdapter(max_retries=retry_strategy)
# create a request session
srat_session = Session()
srat_session.verify = True
# mount the session for all requests, attaching the timeout/retry adapter
srat_session.mount("https://", adapter)
srat_session.mount("http://", adapter)
srat_session.headers.update({"x-api-key": wiki_srat_key})

# from https://github.com/WikiWatershed/model-my-watershed/blob/31566fefbb91055c96a32a6279dac5598ba7fc10/src/mmw/apps/modeling/tasks.py#L375-L409
def run_srat(gwlfe_watereshed_result, with_attenuation, restoration_sources):
    try:
        data = [
            format_for_srat(id, w, with_attenuation, restoration_sources)
            for id, w in gwlfe_watereshed_result.items()
        ]
    except Exception as e:
        raise Exception("Formatting sub-basin GWLF-E results failed: %s" % e)

    # headers = {"x-api-key": wiki_srat_key}

    try:
        r = srat_session.post(
            wiki_srat_url,
            # headers=headers,
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
    # break intod sub-groups
    maflowv = loads.loc[loads["index"] == "maflowv"].copy()
    conc_ptsource = loads.loc[loads["index"].str.contains("conc_ptsource")].copy()
    reach_conc = loads.loc[loads["index"].str.contains("loadrate_conc")].copy()
    total_local_load = loads.loc[loads["index"].str.contains("loadrate_total")].copy()
    local_loads_by_source = loads.loc[
        ~(loads["index"] == "maflowv")
        & ~(
            loads["index"].str.contains("conc_ptsource")
            & ~(loads["index"].str.contains("loadrate_conc"))
            & ~(loads["index"].str.contains("loadrate_total"))
        )
    ].copy()

    return_dict = {"maflowv": maflowv.drop(columns="index")}
    for key, frame in zip(
        ["conc_ptsource", "reach_conc", "total_local_load", "local_loads_by_source"],
        [conc_ptsource, reach_conc, total_local_load, local_loads_by_source],
    ):
        frame[["Nutrient", "Source"]] = frame["index"].str.split("_", expand=True, n=1)
        frame["Nutrient"] = frame["Nutrient"].replace(
            {
                "tpload": "TotalP",
                "tnload": "TotalN",
                "tssload": "Sediment",
                "tploadrate": "TotalP",
                "tnloadrate": "TotalN",
                "tssloadrate": "Sediment",
                "tp": "TotalP",
                "tn": "TotalN",
                "tss": "Sediment",
            }
        )
        frame["Source"] = frame["Source"].replace(SRAT_KEYS.values(), SRAT_KEYS.keys())

        frame_w = frame.pivot(columns="Nutrient", index="Source", values="Value")
        return_dict[key] = frame_w.reset_index()

    for key, frame in return_dict.items():
        if id_key == "huc12":
            return_dict[key]["huc"] = id_value
        if id_key == "comid":
            return_dict[key]["comid"] = id_value
            if huc != "":
                return_dict[key]["huc"] = huc
        return_dict[key]["gwlfe_endpoint"] = "wikisrat"
        return_dict[key]["huc_level"] = 12

    return copy.deepcopy(return_dict)


#%%
# create empty lists to hold results
cum_results = {
    "catchment_total_local_load": [],
    "reach_concentrations": [],
    "reach_average_flow": [],
    "reach_pt_source_conc": [],
    # NOTE:  Individual land use sources are NOT valid when applying restorations, only the totals
    "catchment_sources_local_load": [],
}


#%%
for huc8_id, huc8 in hucs_to_run.groupby(by=["huc8"]):
    logging.info(huc8_id)
    logging.info("  Loading GWLF-E Results")
    huc8_dict = {}
    for _, huc_row in huc8.iterrows():
        gwlfe_result_file_name = (
            gwlfe_json_dump_path
            + "{}_{}_{{}}_subbasin_run.json".format(
                huc_row["huc"], land_use_layer, weather_layer
            )
        )
        gwlfe_result_file = None
        for weather_source in [weather_layer, "USEPA_1960_1990"]:
            if Path(gwlfe_result_file_name.format(weather_source)).is_file():
                gwlfe_result_file = gwlfe_result_file_name.format(weather_source)

        gwlfe_sb_result = None
        if gwlfe_result_file is not None:
            f = open(gwlfe_result_file)
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
    for run_group, funding_source_group in funding_source_groups.items():
        logging.info("    Running for {}".format(run_group))
        wikisrat_result = run_srat(huc8_dict, used_attenuation, funding_source_group)

        if wikisrat_result is not None:
            with open(
                restoration_json_dump_path
                + "HUC8_{}_{}".format(huc8_id, run_group.lower().replace(" ", "_"))
                + ".json",
                "w",
            ) as fp:
                json.dump(wikisrat_result, fp, indent=2)

        if wikisrat_result is not None:
            logging.info("      Framing data")
            for huc12, huc12_wikisrat in wikisrat_result["huc12s"].items():
                huc12_result = {key: None for key in cum_results.keys()}
                h12_cpy = copy.deepcopy(huc12_wikisrat)
                h12_catches = h12_cpy.pop("catchments")

                catch_results = {key: [] for key in cum_results.keys()}
                for catchment, catch_sources in h12_catches.items():
                    catch_loads = format_wikisrat_return(catch_sources, huc=huc12)
                    catch_results["catchment_total_local_load"].append(
                        catch_loads["total_local_load"].copy()
                    )
                    catch_results["reach_concentrations"].append(
                        catch_loads["reach_conc"].copy()
                    )
                    catch_results["reach_average_flow"].append(
                        catch_loads["maflowv"].copy()
                    )
                    catch_results["reach_pt_source_conc"].append(
                        catch_loads["conc_ptsource"].copy()
                    )
                    if run_group == "No restoration or protection":
                        # NOTE:  Individual land use sources are NOT valid when applying restorations, only the totals
                        catch_results["catchment_sources_local_load"].append(
                            catch_loads["local_loads_by_source"].copy()
                        )

                for catch_res_key, catch_list in catch_results.items():
                    if len(catch_list) > 0:
                        all_catch_frame = pd.concat(catch_list, ignore_index=True)
                        all_catch_frame["gwlfe_endpoint"] = "wikiSRAT"
                        all_catch_frame["huc"] = huc12
                        all_catch_frame["run_group"] = run_group
                        all_catch_frame["funding_sources"] = ", ".join(
                            funding_source_group
                        )
                        huc12_result[catch_res_key] = all_catch_frame.copy()

                for result_key, result_frame in huc12_result.items():
                    if result_frame is not None:
                        cum_results[result_key].append(result_frame.copy())
    #     break
    # break

#%%
# join various results and save csv's
for all_res_key, all_list in cum_results.items():
    if len(all_list) > 0:
        all_catch_frame = pd.concat(all_list, ignore_index=True)
        all_catch_frame["with_attenuation"] = used_attenuation
        all_catch_frame = (
            all_catch_frame.sort_values(by=["huc", "comid"])
            .reset_index(drop=True)
            .copy()
        )
        all_catch_frame.to_csv(restoration_csv_path + all_res_key + csv_extension)


#%%
logging.info("DONE!")

# %%
