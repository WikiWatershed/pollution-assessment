#%%
import os
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
from shapely.geometry import mapping,Polygon
from shapely.validation import make_valid
from shapely.geometry.multipolygon import  MultiPolygon
from shapely.ops import polygonize

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

script_dir = os.path.dirname(os.path.realpath(__file__))

#%%
# Read protected land shapes from private directory
# These shapes are already chunked into ComID's via SQL scripting within the
# Academy's database and then repulled to a parquet
protected_shape_file_fromDB = os.path.realpath(
    os.path.join(script_dir, "../private/protection_df.parquet")
)
protected_shapes_fromDB = (
    gpd.read_parquet(protected_shape_file_fromDB)
    .sort_values(by="practice_id")
    .reset_index()
)

# the shapes are chunked into small bits based on the catchment ComIDs
# that causes problems with ModelMW because the shapes end up too small
# we're going to join the shapes of each practice by "dissolving" them into one
dissoved_shapes_fromDB = protected_shapes_fromDB.dissolve(
    by="practice_id"
).reset_index()

# Read protected land shapes as they are in Field Docs from private directory
protected_shape_file_fromFD = os.path.realpath(
    os.path.join(script_dir, "../private/protection_bmps_from_FieldDoc.parquet")
)
protected_shapes_fromFD = (
    gpd.read_parquet(protected_shape_file_fromFD)
    .sort_values(by="practice_id")
    .reset_index()
)

#%% Set up logging
log_file = save_path + "get_future_landuse.log"

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


# from https://github.com/WikiWatershed/model-my-watershed/blob/31566fefbb91055c96a32a6279dac5598ba7fc10/src/mmw/apps/modeling/tasks.py#L375-L409
def run_fastzonal_2100(in_shape):
    try:
        r = requests.post(
            "http://watersheds.cci.drexel.edu/api/fzs_buildout/",
            data=json.dumps({"geom": in_shape, "rasters": ["corridors"]}),
            timeout=60,
        )
    except requests.Timeout:
        raise Exception("Request to Fast Zonal Buildout timed out")
    except ConnectionError:
        raise Exception("Failed to connect to Fast Zonal Buildout")

    if r.status_code != 200:
        raise Exception(
            "Fast Zonal Buildout request failed: %s %s" % (r.status_code, r.text)
        )

    try:
        fz_result = r.json()
    except ValueError:
        raise Exception("Fast Zonal Buildout did not return JSON")

    return fz_result

#from https://stackoverflow.com/questions/2964751/how-to-convert-a-geos-multilinestring-to-polygon
def close_geometry(self, geometry):
   if geometry.empty or geometry[0].empty:
       return geometry # empty

   if(geometry[-1][-1] == geometry[0][0]):
       return geometry  # already closed

   result = None
   for linestring in geom:
      if result is None:
          resultstring = linestring.clone()
      else:
          resultstring.extend(linestring.coords)

   geom = Polygon(resultstring)

   return geom

#%%
# get future land use data
lu_frames = []
run_num = 0
for idx, row in protected_shapes_fromFD.iterrows():
    logging.info("=====================")
    logging.info(
        "{} ({}) [{}] -- {} of {}".format(
            row["practice_id"],
            row["practice_name"].split(" - ")[0],
            row["geometry"].type,
            idx + 1,
            len(protected_shapes_fromFD.index),
        )
    )

    feature = mapping(row["geometry"])

    # this one shape is a mess
    # it's complicated and has a bunch of chunks and holes,
    # some of which self-intersect or aren't closed
    # the fast zonal api times-out tring to run as-is so I'm running the holey polygons and summing
    if row["practice_id"]==51769:
        sub_results=[]
        for geom in row["geometry"].geoms:
            sub_results.append(pd.DataFrame.from_dict(run_fastzonal_2100(mapping(geom)),orient='index').reset_index())
        sub_r=pd.concat(sub_results)
        lu_frame=sub_r.groupby('index').sum()

    else:
        fz_result = run_fastzonal_2100(feature)
        lu_frame = pd.DataFrame.from_dict(fz_result,orient='index')
    lu_frame["practice_id"] = row["practice_id"]
    lu_frame["Land_Use_Source"] = lu_frame.index
    lu_frames.append(lu_frame.reset_index(drop=True).copy())


#%%
# join land use frames
logging.info("Merging all sites...")
lu_results = pd.concat(lu_frames, ignore_index=True)
lu_results=lu_results.fillna(0)
merge_lu = pd.merge(
    protected_shapes_fromFD,
    lu_results,
    on=["practice_id"],
).drop(columns='index')

#%%
# save the results
logging.info("Saving the Land Use Summary...")
merge_lu.to_parquet(
    os.path.realpath(
        os.path.join(script_dir, "../private/protected_future_landuse.parquet")
    )
)

# %%
