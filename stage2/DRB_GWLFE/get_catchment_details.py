#%%
import numpy as np
import pandas as pd
import requests

import json

import geopandas as gpd
from shapely.geometry import shape

#%%
# Set up the API client
from mmw_secrets import (
    save_path,
    csv_path,
    csv_extension,
)

#%%
# get catchment list from sub-basin results
catchments = (
    pd.read_csv(csv_path + "srat_catchment_concs" + csv_extension)[["huc", "comid"]]
    .drop_duplicates()
    .reset_index(drop=True)
)
catchments["HUC10"] = catchments["huc"].apply(lambda x: f"{x:012d}").str.slice(0, 10)
catchments["HUC12"] = catchments["huc"]
catchments["grouper"] = np.floor(catchments.index / 100)

#%%
# get details about each catchment
all_catchment_details = []
for name, chunk in catchments.groupby(by=["grouper"]):
    print(name)
    comid_list = chunk["comid"].unique().tolist()
    comid_details = requests.get(
        "https://staging.modelmywatershed.org/mmw/modeling/subbasins/catchments",
        params={"catchment_comids": json.dumps(comid_list)},
    )
    print(comid_details)
    # all_catchment_details.extend(comid_details.json())
    for catchment in comid_details.json():
        all_catchment_details.append(catchment)

# %%
# save the catchment details
with open(
    csv_path + "nhd_catchment_details.json",
    "w",
) as fp:
    json.dump(all_catchment_details, fp, indent=2)

#%%
# turn catchment details into a dataframe
catchment_detail_df = pd.DataFrame(all_catchment_details)
catchment_detail_df["nhd_catchment_geometry"] = catchment_detail_df.apply(
    lambda row: shape(row["shape"]), axis=1
)
catchment_detail_df["stream_geometry"] = catchment_detail_df.apply(
    lambda row: shape(row["stream"]), axis=1
)
catchments2 = catchments.merge(catchment_detail_df, left_on="comid", right_on="id")[
    [
        "comid",
        "HUC10",
        "HUC12",
        "area",
        "nhd_catchment_geometry",
        "stream_geometry",
    ]
]


gdf_shapes = gpd.GeoDataFrame(
    catchments2.drop(columns=["stream_geometry"]),
    geometry="nhd_catchment_geometry",
).set_crs("EPSG:4326")
gdf_shapes.to_file(
    csv_path + "nhd_catchment_shapes.geojson",
    driver="GeoJSON",
)

gdf_lines = gpd.GeoDataFrame(
    catchments2.drop(columns=["area", "nhd_catchment_geometry"]),
    geometry="stream_geometry",
).set_crs("EPSG:4326")
gdf_lines.to_file(
    csv_path + "nhd_catchment_blue_lines.geojson",
    driver="GeoJSON",
)

# %%
