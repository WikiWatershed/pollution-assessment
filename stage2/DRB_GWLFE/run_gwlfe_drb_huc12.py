"""
Created by Sara Geleskie Damiano
"""
#%%
import sys
import logging

# import time
# import json
import copy
from typing import Dict
import pytz
from datetime import datetime

import pandas as pd

# import geopandas as gpd

from modelmw_client import *

#%%
# Set up the API client
from mmw_secrets import (
    srgd_staging_api_key,
    srgd_mmw_user,
    srgd_mmw_pass,
    save_path,
    csv_path,
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
# I then used geopandas to read that and dumped a csv with some of the attribures.

# huc_shapes = gpd.read_file(
#     "HUC12s in 020401, 020402, 020403 v2.json",
# )
# huc_shapes = huc_shapes.set_crs("EPSG:3857").to_crs("EPSG:4326")
# huc_shapes["huc12"] = huc_shapes["huc12"].astype(str)
# huc_shapes.loc[~huc_shapes["huc12"].str.startswith("0"), "huc12"] = (
#     "0" + huc_shapes.loc[~huc_shapes["huc12"].str.startswith("0")]["huc12"]
# )

#%%
# Read location data - list from Michael Campagna
hucs_from_Mike = pd.read_csv(save_path + "huc12_list_drwipolassess.csv")
hucs_from_Mike["huc12"] = hucs_from_Mike["huc12"].astype(str)
hucs_from_Mike.loc[~hucs_from_Mike["huc12"].str.startswith("0"), "huc12"] = (
    "0" + hucs_from_Mike.loc[~hucs_from_Mike["huc12"].str.startswith("0")]["huc12"]
)
huc_list = hucs_from_Mike.sort_values(by=["huc12"])["huc12"].to_list()

#%% Read the last time this was run
log_file = save_path + "run_gwlfe_drb_huc12.log"

logging.basicConfig(filename=log_file, encoding="utf-8", level=logging.INFO)

root = logging.getLogger()
root.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
root.addHandler(handler)
logging.getLogger("modelmw_client").setLevel(logging.WARN)

this_run_start = (
    pytz.utc.localize(datetime.utcnow())
    .astimezone(pytz.timezone("US/Eastern"))
    .strftime("%Y-%m-%d %H:%M:%S %z")
)


logging.info("Starting script at {}".format(this_run_start))

#%%
# set file names and create empty lists to hold results
result_filenames = [
    "gwlfe_monthly_hucs",
    "gwlfe_load_summaries_hucs",
    "gwlfe_lu_loads_hucs",
    "gwlfe_metadata_hucs",
    "gwlfe_summaries_hucs",
]

mapshed_z_files = []
finished_sites = []

gwlfe_monthlies = []
gwlfe_load_summaries = []
gwlfe_lu_loads = []
gwlfe_metas = []
gwlfe_summaries = []

#%%
# for idx, huc_aoi in huc_shapes.iterrows():
for idx, huc_aoi in enumerate(huc_list):
    logging.info("=====================")
    logging.info("{} -- {} of {}".format(huc_aoi, idx, len(huc_list)))

    mapshed_job_label = "{}_{}".format(huc_aoi, land_use_layer)
    gwlfe_job_label = "{}_{}_{}".format(huc_aoi, land_use_layer, weather_layer)

    gwlfe_result = None
    _, gwlfe_result = mmw_run.read_dumped_result(
        mmw_run.gwlfe_run_endpoint,
        gwlfe_job_label,
        json_dump_path + "{}_gwlfe.json".format(gwlfe_job_label),
        "SummaryLoads",
    )

    if gwlfe_result is None:
        # if we couldn't find the GWLF-E file, we need to rerun both MapShed and
        # GWLF-E becaues the cache of the MapShed job will probably have expired

        ### NOTE:  We're running this twice, once as a normal job and once as a
        # sub-basin job, even though we only have a single HUC-12 and only one
        # "sub-basin".  Running it as a sub-basin will allow us to get the
        # sub-basin area details - ie, the shape of the HUC-12.
        mapshed_job_id = None
        mapshed_result = None
        subbasin_mapshed_id = None

        mapshed_payload = {
            "huc": huc_aoi,
            "layer_overrides": {"__LAND__": mmw_run.land_use_layers[land_use_layer]},
        }

        logging.info("\tRunning Mapshed on whole shape")
        mapshed_job_dict: ModelMyWatershedJob = mmw_run.run_mmw_job(
            request_endpoint=mmw_run.mapshed_endpoint,
            job_label=mapshed_job_label,
            payload=mapshed_payload,
        )
        if "result_response" in mapshed_job_dict.keys():
            mapshed_job_id = mapshed_job_dict["start_job_response"]["job"]
            mapshed_result = mapshed_job_dict["result_response"]["result"]
            logging.info("\t--Got the whole shape MapShed job")

            mapshed_result["huc_aoi"] = huc_aoi
            mapshed_z_files.append(mapshed_result)

        logging.info("\tRunning Mapshed as a sub-basin job")
        sub_mapshed_job_dict: ModelMyWatershedJob = mmw_run.run_mmw_job(
            request_endpoint=mmw_run.subbasin_prepare_endpoint,
            job_label=mapshed_job_label,
            payload=mapshed_payload,
        )
        if "result_response" in sub_mapshed_job_dict.keys():
            subbasin_mapshed_id = sub_mapshed_job_dict["start_job_response"]["job"]
            logging.info("\t--Got the sub-basin MapShed job")

        ## NOTE:  Don't keep processing if we don't get MapShed results,
        # continue to the next site
        if (
            mapshed_job_id is None
            or subbasin_mapshed_id is None
            or mapshed_result is None
        ):
            logging.info("*** Couldn't run MapShed for {}".format(huc_aoi))
            continue

        # get the geojson corresponding to our HUC -
        # we need this to create a project
        logging.info("\tgetting details about the HUC-12s")
        subbasin_huc12s: list = mmw_run.get_subbasin_details(subbasin_mapshed_id)
        # If we don't get a subbasin shape, continue to next HUC
        if len(subbasin_huc12s) != 1:
            logging.info("*** Didn't get a HUC shape for {}".format(huc_aoi))
            continue
        logging.info("\t--Got the shape of the HUC-12s")

        # Now that we have a mapshed job and a HUC shape,
        # we're going to create a project so we can get the weather data for it
        logging.info("\tCreating a new project")
        project_dict: Dict = mmw_run.create_project(
            model_package="gwlfe",
            area_of_interest=copy.deepcopy(subbasin_huc12s[0]["shape"]),
            name=huc_aoi,
            mapshed_job_uuid=mapshed_job_id,
            layer_overrides={"__LAND__": mmw_run.land_use_layers[land_use_layer]},
        )
        project_id: str = project_dict["id"] if "id" in project_dict.keys() else None
        if project_id is None:
            logging.info("*** Couldn't create a project for {}".format(huc_aoi))
            continue
        logging.info("\t--Project {} created".format(project_id))

        # with a project ID in hand, we can get the 2000-2019 weather data,
        # which is not otherwise available
        logging.info("\tGetting weather data")
        weather_2019: Dict = mmw_run.get_project_weather(project_id, weather_layer)
        if weather_2019 == {}:
            logging.info("*** Couldn't create a 2019 weather for {}".format(huc_aoi))
            continue
        logging.info("\t--Got weather data")

        # NOW, we can run GWLF-E!
        logging.info("\tRunning GWLF-E on whole shape")
        gwlfe_payload: Dict = {
            # NOTE:  The value of the inputmod_hash doesn't really matter here
            # Internally, the ModelMW site uses the inputmod_hash in scenerios to
            # determine whether it can use cached results or if it needs to
            # re-run the job
            "inputmod_hash": mmw_run.inputmod_hash,
            "modifications": [weather_2019["output"]],
            "job_uuid": mapshed_job_id,
            "layer_overrides": {"__LAND__": mmw_run.land_use_layers[land_use_layer]},
        }
        gwlfe_job_dict: ModelMyWatershedJob = mmw_run.run_mmw_job(
            request_endpoint=mmw_run.gwlfe_run_endpoint,
            job_label=gwlfe_job_label,
            payload=gwlfe_payload,
        )
        if "result_response" in gwlfe_job_dict.keys():
            gwlfe_result_raw = gwlfe_job_dict["result_response"]
            gwlfe_result = copy.deepcopy(gwlfe_result_raw)["result"]
        logging.info("\t--Got GWLF-E result for whole shape")

        # clean up by deleting project
        logging.info("\tDeleting project {}".format(project_id))
        mmw_run.delete_project(project_id)

    logging.info("\tAdding data to frame lists")
    if gwlfe_result is not None:
        gwlfe_monthly = pd.DataFrame(gwlfe_result.pop("monthly"))
        gwlfe_monthly["month"] = gwlfe_monthly.index + 1
        gwlfe_load_summary = pd.DataFrame(gwlfe_result.pop("SummaryLoads"))
        gwlfe_lu_load = pd.DataFrame(gwlfe_result.pop("Loads"))
        gwlfe_meta = pd.DataFrame(gwlfe_result.pop("meta"), index=[1])
        gwlfe_summary = pd.DataFrame(gwlfe_result, index=[1])

        for frame in [
            gwlfe_monthly,
            gwlfe_load_summary,
            gwlfe_lu_load,
            gwlfe_meta,
            gwlfe_summary,
        ]:
            frame["huc"] = huc_aoi
            frame["land_use_source"] = land_use_layer
            frame["stream_layer"] = stream_layer
            frame["weather_source"] = weather_layer
        gwlfe_monthlies.append(gwlfe_monthly)
        gwlfe_load_summaries.append(gwlfe_load_summary)
        gwlfe_lu_loads.append(gwlfe_lu_load)
        gwlfe_metas.append(gwlfe_meta)
        gwlfe_summaries.append(gwlfe_summary)

#%% join various results
gwlfe_monthly_results = pd.concat(gwlfe_monthlies, ignore_index=True)
gwlfe_load_sum_results = pd.concat(gwlfe_load_summaries, ignore_index=True)
gwlfe_lu_load_results = pd.concat(gwlfe_lu_loads, ignore_index=True)
gwlfe_metas_results = pd.concat(gwlfe_metas, ignore_index=True)
gwlfe_sum_results = pd.concat(gwlfe_summaries, ignore_index=True)

#%% Save csv's
logging.info("Saving the GWLF-E data ...")
gwlfe_monthly_results.sort_values(by=["huc"] + ["month"]).reset_index(drop=True).to_csv(
    csv_path + "gwlfe_monthly_hucs" + csv_extension
)

gwlfe_load_sum_results.sort_values(by=["huc"] + ["Source"]).reset_index(
    drop=True
).to_csv(csv_path + "gwlfe_load_summaries_hucs" + csv_extension)

gwlfe_lu_load_results.sort_values(by=["huc"] + ["Source"]).reset_index(
    drop=True
).to_csv(csv_path + "gwlfe_lu_loads_hucs" + csv_extension)

gwlfe_metas_results.sort_values(by=["huc"]).reset_index(drop=True).to_csv(
    csv_path + "gwlfe_metadata_hucs" + csv_extension
)

gwlfe_sum_results.sort_values(by=["huc"]).reset_index(drop=True).to_csv(
    csv_path + "gwlfe_summaries_hucs" + csv_extension
)
#%%
logging.info("DONE!")

# %%
