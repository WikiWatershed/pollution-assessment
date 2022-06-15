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

#%% Set up logging
log_file = save_path + "run_gwlfe_srat_drb_v3.log"

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
def read_or_run_mapshed(endpoint, label, payload):
    job_dict = None
    job_dict, result = mmw_run.read_dumped_result(
        endpoint,
        label,
    )
    if job_dict is None:
        logging.info("  Running Mapshed ({})".format(endpoint))
        job_dict = mmw_run.run_mmw_job(
            request_endpoint=endpoint,
            job_label=label,
            payload=payload,
        )
    if job_dict is not None and "result_response" in job_dict.keys():
        # if it's a whole basin mapshed, find the weather station key
        if (
            endpoint == mmw_run.gwlfe_prepare_endpoint
            and "WeatherStations" in job_dict["result_response"]["result"].keys()
        ):
            station_list = [
                sta["station"]
                for sta in job_dict["result_response"]["result"]["WeatherStations"]
            ]
            station_list.sort()
            weather_stations = ",".join(map(str, station_list))
        # if it's a sub-basin mapshed, find stations for each HUC
        elif endpoint == mmw_run.subbasin_prepare_endpoint:
            weather_stations = {}
            for huc12, huc12_prep in job_dict["result_response"]["result"].items():
                if "WeatherStations" in huc12_prep.keys():
                    station_list = [
                        sta["station"] for sta in huc12_prep["WeatherStations"]
                    ]
                    station_list.sort()
                    weather_station_val = ",".join(map(str, station_list))
                    weather_stations[huc12] = weather_station_val

        else:
            weather_stations = None
        return (
            job_dict["result_response"]["job_uuid"],
            weather_stations,
        )

    return None, None


def run_gwlfe(endpoint, label, mapshed_job_id, modifications):
    logging.info("  Running GWLF-E ({})".format(endpoint))
    gwlfe_payload: Dict = {
        # NOTE:  The value of the inputmod_hash doesn't really matter here
        # Internally, the ModelMW site uses the inputmod_hash in scenerios to
        # determine whether it can use cached results or if it needs to
        # re-run the job so the value is meaningless here
        "inputmod_hash": mmw_run.inputmod_hash,
        "modifications": modifications,
        "job_uuid": mapshed_job_id,
    }
    gwlfe_job_dict: ModelMyWatershedJob = mmw_run.run_mmw_job(
        request_endpoint=endpoint,
        job_label=label,
        payload=gwlfe_payload,
    )
    if "result_response" in gwlfe_job_dict.keys():
        gwlfe_result_raw = gwlfe_job_dict["result_response"]
        gwlfe_result = copy.deepcopy(gwlfe_result_raw)["result"]
        logging.info("  --Got GWLF-E ({}) results".format(endpoint))
        return gwlfe_job_dict, gwlfe_result
    return gwlfe_job_dict, None


def get_weather_modifications(huc_row, mapshed_job_id, layer_overrides):
    gwlfe_mods = [{}]
    used_weather_layer = "USEPA_1960_1990"

    # With a mapshed job and a HUC shape, we create a project so we can get the weather data for it
    logging.info("  Creating a new project")
    project_dict: Dict = mmw_run.create_project(
        model_package="gwlfe",
        area_of_interest=shapely.geometry.mapping(MultiPolygon([huc_row["geometry"]])),
        name=huc_row["huc"],
        mapshed_job_uuid=mapshed_job_id,
        layer_overrides=layer_overrides,
    )
    project_id: str = project_dict["id"] if "id" in project_dict.keys() else None
    if project_id is None:
        logging.info(
            "*** Couldn't create a project for {} ({})".format(
                huc_row["huc"], huc_row["name"]
            )
        )
        return used_weather_layer, gwlfe_mods
    logging.info("  --Project {} created".format(project_id))

    # with a project ID in hand, we can get the 2000-2019 weather data,
    # which is not otherwise available

    logging.info("  Getting weather data")
    weather_2019: Dict = mmw_run.get_project_weather(project_id, weather_layer)
    if weather_2019 is None or weather_2019 == {}:
        logging.info(
            "*** Couldn't get 2019 weather for {} ({})!! Will use older weather data.".format(
                huc_row["huc"], huc_row["name"]
            )
        )
    else:
        logging.info("  --Got weather data")
        used_weather_layer = weather_layer
        gwlfe_mods = [weather_2019["output"]]

    # clean up by deleting project
    logging.info("  Deleting project {}".format(project_id))
    mmw_run.delete_project(project_id)

    return used_weather_layer, gwlfe_mods


def append_raw_gwlfe_results(
    huc_result: Dict, gwlfe_raw_result: Dict, huc_id: str, huc_level: int
):
    gwlfe_monthly = pd.DataFrame(gwlfe_raw_result["monthly"])
    gwlfe_monthly["month"] = gwlfe_monthly.index + 1
    gwlfe_meta = pd.DataFrame(gwlfe_raw_result["meta"], index=[1])
    gwlfe_summary = pd.DataFrame(
        {
            key: gwlfe_raw_result[key]
            for key in ["AreaTotal", "MeanFlow", "MeanFlowPerSecond"]
        },
        index=[1],
    )
    gwlfe_whole_load_summary = pd.DataFrame(gwlfe_raw_result["SummaryLoads"])
    gwlfe_whole_sources = pd.DataFrame(gwlfe_raw_result["Loads"])

    for frame in [
        gwlfe_monthly,
        gwlfe_meta,
        gwlfe_summary,
        gwlfe_whole_load_summary,
        gwlfe_whole_sources,
    ]:
        frame["gwlfe_endpoint"] = "gwlfe"
        frame["huc"] = huc_id
        frame["huc_level"] = huc_level
    huc_result["gwlfe_monthly_q"] = gwlfe_monthly
    huc_result["gwlfe_metadata"] = gwlfe_meta
    huc_result["gwlfe_summ_q"] = gwlfe_summary
    huc_result["raw_load_summaries"] = gwlfe_whole_load_summary
    huc_result["raw_source_summaries"] = gwlfe_whole_sources


def append_attenuated_gwlfe_results(
    huc_result: Dict, gwlfe_sb_result: Dict, huc_id: str, huc_level: int
):
    attenuated_load_summary = pd.DataFrame(gwlfe_sb_result["SummaryLoads"], index=[1])
    attenuated_source_summary = pd.DataFrame(gwlfe_sb_result["Loads"])

    for frame in [
        attenuated_load_summary,
        attenuated_source_summary,
    ]:
        frame["gwlfe_endpoint"] = "subbasin"
        frame["huc"] = huc_id
        frame["huc_level"] = huc_level

    huc_result["attenuated_load_summaries"] = attenuated_load_summary
    huc_result["attenuated_source_summaries"] = attenuated_source_summary


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


TASK_REQUEST_TIMEOUT = 60

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
            timeout=60,
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
    "gwlfe_monthly_q": [],
    "gwlfe_metadata": [],
    "gwlfe_summ_q": [],
    "raw_load_summaries": [],
    "raw_source_summaries": [],
    "attenuated_load_summaries": [],
    "attenuated_source_summaries": [],
    "catchment_loading_rates": [],
    "reach_concentrations": [],
    "catchment_sources": [],
    "wikisrat_huc_sources": [],
    "wikisrat_catchment_loading_rates": [],
    "wikisrat_reach_concentrations": [],
    "wikisrat_catchment_sources": [],
}

#%%
for idx, huc_row in hucs_to_run.iterrows():
    logging.info("=====================")
    logging.info(
        "{} ({}) -- {} of {}".format(
            huc_row["huc"], huc_row["name"], idx, len(hucs_to_run.index)
        )
    )
    huc_result = dict.fromkeys(cum_results.keys(), None)

    mapshed_job_label = "{}_{}".format(huc_row["huc"], land_use_layer)
    mapshed_payload = {
        "huc": huc_row["huc"],
        "layer_overrides": {
            "__LAND__": mmw_run.land_use_layers[land_use_layer],
            "__STREAMS__": stream_layer,
        },
    }

    mapshed_sb_job_id, closest_stations = read_or_run_mapshed(
        mmw_run.subbasin_prepare_endpoint, mapshed_job_label, mapshed_payload
    )

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
    if gwlfe_sb_result is None and closest_stations is not None:
        used_weather_layer, gwlfe_mods = get_weather_modifications(
            huc_row, mapshed_sb_job_id, mapshed_payload["layer_overrides"]
        )
    elif gwlfe_sb_job_dict is not None and gwlfe_sb_job_dict["payload"][
        "modifications"
    ] == [{}]:
        used_weather_layer = "USEPA_1960_1990"
    else:
        used_weather_layer = weather_layer

    gwlfe_job_label: str = "{}_{}_{}".format(
        huc_row["huc"], land_use_layer, used_weather_layer
    )

    if gwlfe_sb_result is None and mapshed_sb_job_id is not None:
        gwlfe_sb_job_dict, gwlfe_sb_result = run_gwlfe(
            mmw_run.subbasin_run_endpoint,
            gwlfe_job_label,
            mapshed_sb_job_id,
            gwlfe_mods,
        )

    wikisrat_job_label = gwlfe_job_label
    wikisrat_result = None
    _, wikisrat_result = mmw_run.read_dumped_result("wikiSRAT", gwlfe_job_label)

    if (
        gwlfe_sb_result is not None
        and wikisrat_result is None
        and huc_row["huc_level"] == 12
    ):
        logging.info("  Running SRAT")
        wikisrat_result = run_srat(
            {huc_row["huc"]: copy.deepcopy(gwlfe_sb_result["HUC12s"][huc_row["huc"]]["Raw"])}
        )

        if wikisrat_result is not None:
            mock_job_dict = {
                "job_label": wikisrat_job_label,
                "request_host": "wikiSRAT",
                "request_endpoint": "wikiSRAT",
                "start_job_status": "complete",
                "job_result_status": "complete",
                "result_response": {"result": wikisrat_result},
            }
            mmw_run.dump_job_json(mock_job_dict)

    logging.info("  Framing data")
    if gwlfe_sb_result is not None:
        for huc12 in gwlfe_sb_result["HUC12s"].keys():
            gwlfe_raw_result = gwlfe_sb_result["HUC12s"][huc12]["Raw"]
            append_raw_gwlfe_results(huc_result, gwlfe_raw_result, huc12, 12)
            append_attenuated_gwlfe_results(
                huc_result,
                gwlfe_sb_result["HUC12s"][huc12],
                huc12,
                12,
            )
            catch_results = {
                "catchment_loading_rates": [],
                "reach_concentrations": [],
                "catchment_sources": [],
            }
            for catchment in gwlfe_sb_result["HUC12s"][huc12]["Catchments"].keys():
                catch_sources = pd.DataFrame(
                    gwlfe_sb_result["HUC12s"][huc12]["Catchments"][catchment]["Loads"]
                )
                catch_sources["comid"] = catchment
                catch_results["catchment_sources"].append(catch_sources.copy())

                reach_concentrations = pd.DataFrame(
                    gwlfe_sb_result["HUC12s"][huc12]["Catchments"][catchment][
                        "LoadingRateConcentrations"
                    ],
                    index=pd.Index([catchment]),
                )
                reach_concentrations["comid"] = catchment
                catch_results["reach_concentrations"].append(reach_concentrations.copy())

                catch_loading_rates = pd.DataFrame(
                    gwlfe_sb_result["HUC12s"][huc12]["Catchments"][catchment][
                        "TotalLoadingRates"
                    ],
                    index=pd.Index([catchment]),
                )
                catch_loading_rates["comid"] = catchment
                catch_results["catchment_loading_rates"].append(
                    catch_loading_rates.copy()
                )

            for catch_res_key, catch_list in catch_results.items():
                if len(catch_list) > 0:
                    all_catch_frame = pd.concat(catch_list)
                    all_catch_frame["gwlfe_endpoint"] = "subbasin"
                    all_catch_frame["huc"] = huc12
                    all_catch_frame["huc_level"] = 12
                    huc_result[catch_res_key] = all_catch_frame.copy()

    if wikisrat_result is not None:
        for huc12, huc12_wikisrat in wikisrat_result["huc12s"].items():
            h12_cpy = copy.deepcopy(huc12_wikisrat)
            h12_catches = h12_cpy.pop("catchments")
            huc_result["wikisrat_huc_sources"] = (
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
                    all_catch_frame =  pd.concat(catch_list, ignore_index=True)
                    all_catch_frame["gwlfe_endpoint"] = "wikiSRAT"
                    all_catch_frame["huc"] = huc12
                    all_catch_frame["huc_level"] = 12
                    huc_result[catch_res_key] = all_catch_frame.copy()

    for result_key, result_frame in huc_result.items():
        if result_frame is not None:
            result_frame["huc_run"] = huc_row["huc"]
            result_frame["huc_run_level"] = huc_row["huc_level"]
            result_frame["huc_run_name"] = huc_row["name"]
            result_frame["huc_run_states"] = huc_row["states"]
            result_frame["huc_run_areaacres"] = huc_row["areaacres"]
            result_frame["land_use_source"] = land_use_layer
            result_frame["closest_weather_stations"] = closest_stations[huc_row["huc"]]
            result_frame["stream_layer"] = stream_layer
            result_frame["weather_source"] = used_weather_layer
            cum_results[result_key].append(result_frame.copy())


#%%
# join various results
gwlfe_monthly_q = pd.concat(cum_results["gwlfe_monthly_q"], ignore_index=True)
gwlfe_metadata = pd.concat(cum_results["gwlfe_metadata"], ignore_index=True)
gwlfe_summ_q = pd.concat(cum_results["gwlfe_summ_q"], ignore_index=True)
raw_load_summaries = pd.concat(cum_results["raw_load_summaries"], ignore_index=True)
raw_source_summaries = pd.concat(
    cum_results["raw_source_summaries"], ignore_index=True
)

attenuated_load_summaries = pd.concat(
    cum_results["attenuated_load_summaries"], ignore_index=True
)
attenuated_source_summaries = pd.concat(
    cum_results["attenuated_source_summaries"], ignore_index=True
)

catchment_loading_rates = pd.concat(
    cum_results["catchment_loading_rates"], ignore_index=True
)
reach_concentrations = pd.concat(cum_results["reach_concentrations"], ignore_index=True)
catchment_sources = pd.concat(cum_results["catchment_sources"], ignore_index=True)

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
gwlfe_monthly_q.sort_values(by=["huc"] + ["month"]).reset_index(drop=True).to_csv(
    csv_path + "gwlfe_monthly_q" + csv_extension
)
gwlfe_metadata.sort_values(by=["huc"]).reset_index(drop=True).to_csv(
    csv_path + "gwlfe_metadata" + csv_extension
)
gwlfe_summ_q.sort_values(by=["huc"]).reset_index(drop=True).to_csv(
    csv_path + "gwlfe_summ_q" + csv_extension
)
raw_load_summaries.sort_values(by=["huc"] + ["Source"]).reset_index(drop=True).to_csv(
    csv_path + "raw_load_summaries" + csv_extension
)
raw_source_summaries.sort_values(by=["huc"] + ["Source"]).reset_index(
    drop=True
).to_csv(csv_path + "raw_source_summaries" + csv_extension)

attenuated_load_summaries.sort_values(by=["huc"] + ["Source"]).reset_index(
    drop=True
).to_csv(csv_path + "attenuated_load_summaries" + csv_extension)
attenuated_source_summaries.sort_values(by=["huc"] + ["Source"]).reset_index(
    drop=True
).to_csv(csv_path + "attenuated_source_summaries" + csv_extension)

catchment_loading_rates.sort_values(by=["huc_run", "comid"]).reset_index(
    drop=True
).to_csv(csv_path + "catchment_loading_rates" + csv_extension)
reach_concentrations.sort_values(by=["huc_run", "comid"]).reset_index(drop=True).to_csv(
    csv_path + "reach_concentrations" + csv_extension
)
catchment_sources.sort_values(by=["huc_run", "comid"]).reset_index(drop=True).to_csv(
    csv_path + "catchment_sources" + csv_extension
)

wikisrat_huc_sources.sort_values(by=["huc"]).reset_index(drop=True).to_csv(
    csv_path + "wikisrat_huc_sources" + csv_extension
)
wikisrat_catchment_loading_rates.sort_values(by=["huc", "comid"]).reset_index(
    drop=True
).to_csv(csv_path + "wikisrat_catchment_loading_rates" + csv_extension)
wikisrat_reach_concentrations.sort_values(by=["huc", "comid"]).reset_index(drop=True).to_csv(
    csv_path + "wikisrat_reach_concentrations" + csv_extension
)
wikisrat_catchment_sources.sort_values(by=["huc", "comid"]).reset_index(
    drop=True
).to_csv(csv_path + "wikisrat_catchment_sources" + csv_extension)


#%%
logging.info("DONE!")

# %%
