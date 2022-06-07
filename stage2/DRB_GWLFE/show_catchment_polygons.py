#%%
from mailbox import linesep
import numpy as np
import pandas as pd
import geopandas as gpd
import spatialpandas as spd
import spatialpandas.geometry
from spatialpandas import GeoSeries, GeoDataFrame

from shapely import ops
from shapely.geometry import Polygon

import colorcet as cc
import datashader as ds
import datashader.transfer_functions as tf

import holoviews as hv
from holoviews import dim, opts, Options
from holoviews.operation.datashader import rasterize, datashade, inspect_polygons

import geoviews as gv
import geoviews.tile_sources as gvts

import panel as pn
from bokeh.resources import INLINE
from bokeh.models import HoverTool

pd.options.plotting.backend = "holoviews"
pn.extension(comms="vscode")
hv.output(backend="bokeh")
gv.extension("bokeh")

from mmw_secrets import (
    save_path,
    csv_path,
)

save_plots = True

import xyzservices.providers as xyz
from bokeh.tile_providers import get_provider

tile_provider = get_provider(xyz.OpenStreetMap.Mapnik)

# set up some plotting dimensions
nutrients = {"TotalN": "Nitrogen", "TotalP": "Phosphorous", "Sediment": "Sediment"}
nutrient_dim = hv.Dimension(
    "nutrient",
    label="Nutrient",
    default="Nitrogen",
    type=str,
    values=nutrients.values(),
)
source_dim = hv.Dimension(
    "load_source",
    label="Load Source",
    default="Entire area",
    type=str,
    values=[
        "Entire area",
        "Barren Areas",
        "Cropland",
        "Farm Animals",
        "Hay/Pasture",
        "High-Density Mixed",
        "Low-Density Mixed",
        "Low-Density Open Space",
        "Medium-Density Mixed",
        "Open Land",
        "Point Sources",
        "Septic Systems",
        "Stream Bank Erosion",
        "Subsurface Flow",
        "Wetlands",
        "Wooded Areas",
    ],
)


# set some default plotting options
font_sizes = {
    "title": 16,
    "labels": 14,
    "xticks": 8,
    "yticks": 8,
    "legend": 8,
    "legend_title": 8,
}
opts.defaults(
    opts.Scatter(
        xlabel="Year",
        # width=1024,
        size=8,
        legend_position="top_left",
        cmap="glasbey",
    ),
    opts.Slope(line_width=2),
    opts.Polygons(
        tools=["hover"],
        fixed_bounds=True,
        active_tools=["pan", "wheel_zoom"],
        fill_alpha=0.75,
        nonselection_fill_alpha=0.75,
        selection_alpha=0.75,
        hover_alpha=0.95,
        muted_fill_alpha=0.5,
    ),
    opts.GridSpace(plot_size=(250, 300), merge_tools=True, yrotation=90),
    opts.Table(
        editable=False,
        fit_columns=True,
        sortable=False,
        # width=1024,
    ),
    opts.Tiles(
        # height=300,
        # width=250,
        xaxis=None,
        yaxis=None,
        active_tools=["pan", "wheel_zoom"],
    ),
    fontsize=font_sizes,
)


#%%
# get HUC shapes

# These are all of the HUC-12's in the HUC-6's 020401, 020402, and 020403
# I got the list from https://hydro.nationalmap.gov/arcgis/rest/services/wbd/MapServer/6/query?f=json&where=((UPPER(huc12)%20LIKE%20%27020401%25%27)%20OR%20(UPPER(huc12)%20LIKE%20%27020402%25%27)%20OR%20(UPPER(huc12)%20LIKE%20%27020403%25%27))&spatialRel=esriSpatialRelIntersects&outFields=OBJECTID%2Csourcefeatureid%2Cloaddate%2Careaacres%2Careasqkm%2Cstates%2Chuc12%2Cname%2Chutype%2Chumod%2Ctohuc%2Cnoncontributingareaacres%2Cnoncontributingareasqkm&orderByFields=OBJECTID%20ASC&outSR=102100
print("Reading the HUC-12 names and shapes")
huc12_shapes = gpd.read_file(
    save_path + "HUC12s in 020401, 020402, 020403 v2.json",
)
huc12_shapes = huc12_shapes.set_crs("EPSG:3857")
huc12_shapes2 = (
    huc12_shapes.sort_values(by=["huc12"])
    .reset_index(drop=True)
    .rename(columns={"huc12": "huc"})
)
huc12_shapes2["huc_level"] = "HUC12"

# These are all of the HUC-10's in the HUC-6's 020401, 020402, and 020403
# I got the list from https://hydro.nationalmap.gov/arcgis/rest/services/wbd/MapServer/5/query?f=json&where=((UPPER(huc10) LIKE '020401%') OR (UPPER(huc10) LIKE '020402%') OR (UPPER(huc10) LIKE '020403%'))&spatialRel=esriSpatialRelIntersects&outFields=OBJECTID,Shape,sourcedatadesc,sourceoriginator,sourcefeatureid,loaddate,areaacres,areasqkm,states,huc10,name,hutype,humod,referencegnis_ids&orderByFields=OBJECTID ASC&outSR=102100
print("Reading the HUC-10 names and shapes")
huc10_shapes = gpd.read_file(
    save_path + "WBD_HUC10s.json",
)
huc10_shapes = huc10_shapes.set_crs("EPSG:3857")
huc10_shapes["huc_level"] = 10
huc10_shapes2 = (
    huc10_shapes.sort_values(by=["huc10"])
    .reset_index(drop=True)
    .rename(columns={"huc10": "huc"})
)
huc10_shapes2["huc_level"] = "HUC10"

#%%
# get NHD Catchment shapes
print("Reading the NHD Catchment shapes")
comid_geom_r = gpd.read_file(
    csv_path + "nhd_catchment_shapes.geojson", driver="GeoJSON"
).drop(columns="HUC10")
comid_geom_r["huc"] = comid_geom_r["HUC12"].apply(lambda x: f"{x:012d}")
comid_geom_r = comid_geom_r.to_crs("EPSG:3857")


#%%
# combine shapes
geo_df = GeoDataFrame(
    pd.concat([huc12_shapes2, huc10_shapes2, comid_geom_r])[
        [
            "comid",
            "huc",
            "name",
            "states",
            "tohuc",
            "huc_level",
            "areaacres",
            "areasqkm",
            "geometry",
        ]
    ]
)
geo_df["geom_type"] = geo_df["huc_level"].fillna("catchment")
geo_df["geom_id"] = geo_df["comid"].fillna(geo_df["huc"])

#%%
# cross check for whether HUC's are really in the DRB interest region
hucs_from_Mike = pd.read_csv(save_path + "huc12_list_drwipolassess.csv")
hucs_from_Mike["huc12"] = hucs_from_Mike["huc12"].apply(lambda x: f"{x:012d}")
drb_huc10s = hucs_from_Mike["huc12"].str.slice(0, 10).unique()
drb_huc12s = hucs_from_Mike["huc12"].unique()
# geo_df["in_drb"] = geo_df["huc"].isin(np.concatenate((drb_huc10s, drb_huc12s), axis=0))
geo_df["in_drb"] = geo_df["huc"].str.slice(0, 10).isin(drb_huc10s)

#%%
# get model results

# Attenuated subbasins results - already summed for the larger basin
sb_load_sum_results = pd.read_csv(csv_path + "gwlfe_sb_load_summaries.csv")
sb_load_sum_results2 = sb_load_sum_results.loc[
    sb_load_sum_results["huc"].apply(lambda x: f"{x:012d}")
    == sb_load_sum_results["huc_run"].apply(lambda x: f"{x:012d}")
].copy()
sb_source_loads_results = pd.read_csv(csv_path + "gwlfe_sb_source_summaries.csv")
sb_source_loads_results2 = sb_source_loads_results.loc[
    sb_source_loads_results["huc"].apply(lambda x: f"{x:012d}")
    == sb_source_loads_results["huc_run"].apply(lambda x: f"{x:012d}")
].copy()


# Attenuated subbasins, started using the whole results from ModelMW
# and then run directly on WikiSRAT microservice
wiki_srat_rates = pd.read_csv(csv_path + "wikisrat_catchment_load_rates.csv")
wiki_srat_rates["Source"] = "Entire area"
wiki_source_loads_results = pd.read_csv(csv_path + "wikisrat_catchment_sources.csv")

#%%
#  combine results
combined_results = pd.concat(
    [
        sb_load_sum_results2,
        sb_source_loads_results2,
        wiki_srat_rates,
        wiki_source_loads_results,
    ]
)[
    ["Source", "Sediment", "TotalN", "TotalP", "comid", "huc_run", "huc_run_level"]
].copy()
combined_results["huc_run"] = np.where(
    combined_results["huc_run_level"] == 10,
    combined_results["huc_run"].apply(lambda x: f"{x:010d}"),
    combined_results["huc_run"].apply(lambda x: f"{x:012d}"),
)
combined_results["geom_id"] = combined_results["comid"].fillna(
    combined_results["huc_run"]
)


combined_w = combined_results.pivot(
    index=["geom_id"],
    columns="Source",
    values=["Sediment", "TotalN", "TotalP"],
)

for nutrient in nutrients.keys():
    combined_w[(nutrient, "Developed Land Uses")] = (
        combined_w[(nutrient, "Barren Areas")]
        + combined_w[(nutrient, "High-Density Mixed")]
        + combined_w[(nutrient, "Medium-Density Mixed")]
        + combined_w[(nutrient, "Low-Density Mixed")]
        + combined_w[(nutrient, "Low-Density Open Space")]
        + combined_w[(nutrient, "Septic Systems")]
    )
    combined_w[(nutrient, "Ag Uses")] = (
        combined_w[(nutrient, "Cropland")]
        + combined_w[(nutrient, "Hay/Pasture")]
        + combined_w[(nutrient, "Farm Animals")]
    )
combined_w.columns = [" ".join(col).strip() for col in combined_w.columns.values]

# combine all of the shapes with the model data
model_and_shapes = geo_df.merge(combined_w, on=["geom_id"])
drb = model_and_shapes.loc[model_and_shapes["in_drb"]].copy()


#%%
# test plot
# from https://github.com/holoviz/spatialpandas/blob/master/examples/Overview.ipynb
# Visualizing geometry arrays interactively with Datashader and HoloViews
# def callback(x_range, y_range):
#     cvs = ds.Canvas(plot_width=650, plot_height=400, x_range=x_range, y_range=y_range)
#     agg = cvs.polygons(
#         drb.loc[drb["geom_type"] == "catchment"],
#         geometry="geometry",
#         agg=ds.mean("TotalP Developed Land Uses"),
#     )
#     return hv.Image(agg).opts(cmap="viridis").opts(width=650, height=400)


# hv.DynamicMap(callback, streams=[hv.streams.RangeXY()])


# %%
tooltips = [("ID", "@geom_id"), ("Name", "@name")]

hover_tool = HoverTool(tooltips=tooltips)
tiles = gv.tile_sources.OSM().opts(
    min_height=500, responsive=True, xaxis=None, yaxis=None
)
polys = hv.Polygons(
    drb.loc[drb["geom_type"] == "catchment"],
    vdims="TotalP Developed Land Uses",
).opts(show_frame=True)
# borders = hv.Polygons(
#     drb.loc[drb["geom_type"] == "HUC10"],
# ).opts(fill_alpha=0)
huc12_borders = hv.Path(
    drb.loc[drb["geom_type"] == "HUC12"],
).opts(line_color='black',line_width=1)
huc10_borders = hv.Path(
    drb.loc[drb["geom_type"] == "HUC10"],
).opts(line_color='black',line_width=2)

shaded_polys = datashade(polys, aggregator=ds.mean("TotalP Developed Land Uses"),cmap='RdYlGn_r',cnorm='log')

hover = inspect_polygons(shaded_polys).opts(tools=[hover_tool])

tiles * shaded_polys * hover * huc12_borders*huc10_borders

# %%
