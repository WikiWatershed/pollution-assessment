import pandas as pd
import geopandas as gpd
import holoviews as hv
from holoviews.operation.datashader import datashade, rasterize
import geoviews as gv
hv.extension("bokeh")

def plot(gdf: geopandas.geodataframe.GeoDataFrame):
	'''
	Main plotting function for all DRWI geometries, 
	including reaches (MultiLineString), NHD catchments (MultiPolygon),
	and HUCs (MultiPolygon)

	Parameters:
		gdf:	Pandas geodataframe. Geometry can be either MultiLineString or MultiPolygon.
	'''
	# Define geometry type 
	geometry_type = gdf.geom_type.unique()[0]
	# Determine which plotting function to use
	if geometry_type == "MultiLineString":
		plot_lines()
	elif geometry_type == "MultiPolygon":
		plot_polys()
	else:
		print(f"Error! Not equipped to handle {geometry_type}")


