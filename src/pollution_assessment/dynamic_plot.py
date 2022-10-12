import pandas as pd
import numpy as np
import warnings
import geopandas as gpd
import holoviews as hv
from holoviews.operation.datashader import datashade, rasterize
import geoviews as gv
import hvplot
hv.extension("bokeh")

warnings.filterwarnings('ignore', message='.*Iteration over multi-part geometries is deprecated and will be removed in Shapely 2.0. Use the `geoms` property to access the constituent parts of a multi-part geometry*')

def project_gdf(gdf: geopandas.geodataframe.GeoDataFrame): -> geopandas.geodataframe.GeoDataFrame
	'''
	Geoviews requires certain projections for plotting

	Parameters:
		gdf:	Pandas geodataframe. Geometry can be either MultiLineString or MultiPolygon.

	Returns:
		gdf_proj: 	Projected pandas geodataframe (EPSG:4326)
	'''

	gdf_proj = gdf.to_crs('EPSG:4326')
	return gdf_proj

def rename_geometry_column(gdf: geopandas.geodataframe.GeoDataFrame): -> geopandas.geodataframe.GeoDataFrame
	'''
	Geoviews requires the geometry column is named 'geometry'

	Parameters:
		gdf:	Pandas geodataframe. Geometry can be either MultiLineString or MultiPolygon.

	Returns:
		gdf_renamed: 	Pandas geodataframe with geometry column named 'geometry'
	'''
	geom_name = gdf.geometry.name
	gdf_renamed = gdf.rename(columns={geom_name:"geometry"})
	return gdf_renamed

def remove_invalid_geometry(gdf: geopandas.geodataframe.GeoDataFrame): -> geopandas.geodataframe.GeoDataFrame
	'''
	Geoviews cannot include null geometries. This function removes any null geometries. 

	Parameters:
		gdf:	Pandas geodataframe. Geometry can be either MultiLineString or MultiPolygon.

	Returns:
		gdf_renamed: 	Pandas geodataframe with all non-null geometries 
	'''
	gdf_valid = gdf_proj[~gdf.geom_type.isna()]
	return gdf_valid

def prep_gdf(gdf: geopandas.geodataframe.GeoDataFrame): -> geopandas.geodataframe.GeoDataFrame
	'''
	Pull together all pre-processing steps for plotting the results 

	Parameters:
		gdf:	Pandas geodataframe. Geometry can be either MultiLineString or MultiPolygon.

	Returns:
		gdf_final: 	Pandas geodataframe with all necessary requirements for Geoviews plotting
	'''
	gdf_proj = project_gdf(gdf)
	gdf_renamed = rename_geometry_column(gdf_proj)
	gdf_final = remove_invalid_geometry(gdf_renamed)
	return gdf_final


def plot(
			gdf: geopandas.geodataframe.GeoDataFrame, var: str, 
			cmap = 'OrRd': str, line_width = 0.1: float, colorbar = True: bool,
			height = 750: int, width = 500: int, tools = ['hover']: list, 
			basemap = gv.tile_sources.CartoLight(): geoviews.element.geo.WMTS):
	'''
	Main plotting function for all DRWI geometries, 
	including reaches (MultiLineString), NHD catchments (MultiPolygon),
	and HUCs (MultiPolygon)

	Parameters:
		gdf:			Pandas geodataframe. Geometry can be either MultiLineString or MultiPolygon.
		var:			Variable to plot. 
		camp: 			Color map for plotting. Default is the orange to red colormap. 
		line_width:		The width of lines to plot (currently used for Polygon boundaries only). Default is 0.1.
		colorbar:		Boolean indicating whether or not to plot colorbar. 
		height:			Height of plot
		width:			Width of plot 
		tools:			List indicating which bokeh tools to plot. Default is to just have hover. 
		basemap:		Which geoviews tile source to plot over (https://geoviews.org/user_guide/Working_with_Bokeh.html)

	Returns:
		TBD
	''' 
	# Ensure variable is valid 
	if var not in gdf.columns:
		print("Invalid Variable. var must be in the list below:")
		print(gdf.columns)
		return 
	# Prepare GDF for plotting functions 
	gdf = prep_gdf(gdf)

	# Define geometry type to determine which plotting function to use 
	geometry_type = gdf.geom_type.unique()[0]
	if geometry_type == "MultiLineString":
		plot_lines(gdf, var, cmap, colorbar, height, width, tools, basemap)
	elif geometry_type == "MultiPolygon":
		plot_polys(gdf, cmap, line_width, colorbar, height, width, tools, basemap)
	else:
		print(f"Error! Not equipped to handle {geometry_type}.")
		print("Please ensure your geometries are MultiLineString or MultiPolygon")
		return 


def plot_poly(self, gdf: geopandas.geodataframe.GeoDataFrame, var: str, **kwargs): -> geoviews.element.geo.Polygons
	'''
	Plot polygons in the DRWI (e.g., NHD+ catchments or HUCs)

	Parameters:
		gdf:	Pandas geodataframe with MultiPolygon geometry
		var:	Variable to plot. 

	Returns:
		poly_map: 	Polygon map colored by variable of choice plotted on basemap.  
	'''
	poly_map = gv.Polygons(gdf, vdims=[var]).opts(
													height = height,
													width = width,
													colorbar = colorbar,
													cmap = cmap,
													line_width = line_width,
													title = var,
													tools = tools
												)
	return poly_map * basemap 


def plot_lines(gdf: geopandas.geodataframe.GeoDataFrame, var: str, **kwargs): -> geoviews.element.geo.Path
	'''
	Plot lines in the DRWI (e.g., stream reaches)

	Parameters:
		gdf:	Pandas geodataframe with MultiLineStringGeometry
		var:	Variable to plot. 


	Returns:
		line_map: Polyline map colored by variable of choice plotted on basemap.  
	'''
	line_map = gv.Path(gdf, vdims=[var]).opts(
												height = height,
												width = width, 
												color = var, 
												colorbar = colorbar, 
												cmap = cmap, 
												line_width = line_width,
												title = var, 
												tools = tools)

	return



