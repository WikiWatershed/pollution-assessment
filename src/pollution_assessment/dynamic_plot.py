import pandas as pd
import numpy as np
import warnings
import geopandas as gpd
import holoviews as hv
from holoviews.operation.datashader import datashade, rasterize
import geoviews as gv
import hvplot
import matplotlib
import matplotlib.pyplot as plt
from mpl_toolkits.axes_grid1 import AxesGrid
hv.extension("bokeh")
from bokeh.models import HoverTool

warnings.filterwarnings('ignore', message='.*Iteration over multi-part geometries is deprecated and will be removed in Shapely 2.0. Use the `geoms` property to access the constituent parts of a multi-part geometry*')

DIFF_SUFFIXES = ['xs', 'rem']

def project_gdf(gdf: gpd.geodataframe.GeoDataFrame) -> gpd.geodataframe.GeoDataFrame:
	'''
	Geoviews requires certain projections for plotting

	Parameters:
		gdf:	Pandas geodataframe. Geometry can be either MultiLineString or MultiPolygon.

	Returns:
		gdf_proj: 	Projected pandas geodataframe (EPSG:4326)
	'''

	gdf_proj = gdf.to_crs('EPSG:4326')
	return gdf_proj

def rename_geometry_column(gdf: gpd.geodataframe.GeoDataFrame) -> gpd.geodataframe.GeoDataFrame:
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

def remove_invalid_geometry(gdf: gpd.geodataframe.GeoDataFrame) -> gpd.geodataframe.GeoDataFrame:
	'''
	Geoviews cannot include null geometries. This function removes any null geometries. 

	Parameters:
		gdf:	Pandas geodataframe. Geometry can be either MultiLineString or MultiPolygon.

	Returns:
		gdf_renamed: 	Pandas geodataframe with all non-null geometries 
	'''
	gdf_valid = gdf[~gdf.geom_type.isna()]
	return gdf_valid

def prep_gdf(gdf: gpd.geodataframe.GeoDataFrame) -> gpd.geodataframe.GeoDataFrame:
	'''
	Pull together all pre-processing steps for plotting the results 

	Parameters:
		gdf:	Pandas geodataframe. Geometry can be either MultiLineString or MultiPolygon.

	Returns:
		gdf_final: 	Pandas geodataframe with all necessary requirements for Geoviews plotting
	'''
	gdf_proj = project_gdf(gdf)
	if 'geometry' not in gdf_proj.columns:
		gdf_proj = rename_geometry_column(gdf_proj)
	else:
		pass
	gdf_final = remove_invalid_geometry(gdf_proj)
	return gdf_final

def normalize_data(data: np.array) -> np.array:
	'''
	Normalize values in a list over 0, 1

	Parameters:
		data: 		Array of values to normalize over 0, 1

	Returns:
		norm_data:	Normalized array of values


	'''
	norm_data = (data - np.min(data)) / (np.max(data) - np.min(data))
	return norm_data

def shift_color_map(cmap, vmin, vmid, vmax, name='shiftedcmap'):
    '''
    Function to offset the "center" of a colormap. 
    Holoviews doesn't appear to offer the same flexibility of color normalization as matplotlib
    So we skew the colorbar itself instead. 
    Adapted from: https://stackoverflow.com/questions/7404116/defining-the-midpoint-of-a-colormap-in-matplotlib
    Question: does this work on not 0 to 1 range?

    Parameters
    -----
		cmap: 	The matplotlib colormap to be altered
	    vmin: 	Offset from lowest point in the colormap's range.
	    vmid: 	The new center of the colormap. 
	    vmax: 	Offset from highest point in the colormap's range.

    Returns:
      new_cmap: Matplotlib colormap skewed as specified. 
    '''
    cdict = {
        'red': [],
        'green': [],
        'blue': [],
        'alpha': []
    }
    norm_vals = normalize_data(np.array([np.log(vmin), np.log(vmid), np.log(vmax)]))
    vmin = norm_vals[0]
    vmid = norm_vals[1]
    vmax = norm_vals[2]

    # regular index to compute the colors
    reg_index = np.linspace(0, 1, 257)

    # shifted index to match the data
    shift_index = np.hstack([
        np.linspace(0.0, vmid, 128, endpoint=False), 
        np.linspace(vmid, 1.0, 129, endpoint=True)
    ])

    for ri, si in zip(reg_index, shift_index):
        ncmap = matplotlib.cm.get_cmap(cmap)
        r, g, b, a = ncmap(ri)

        cdict['red'].append((si, r, r))
        cdict['green'].append((si, g, g))
        cdict['blue'].append((si, b, b))
        cdict['alpha'].append((si, a, a))

    new_cmap = matplotlib.colors.LinearSegmentedColormap(name, cdict)
    # plt.register_cmap(cmap=new_cmap)

    return new_cmap

def is_diff(var: str) -> bool:
	'''
	Determine if the plotting variable is 'xs' or 'rem', 
	which defines how the colorbar should be scaled. 

	Parameters:
		var:	Plotting variable

	Returns:
		bool:	True if it is 'xs' or 'rem', else False. 
	''' 
	suffix = var.split('_')[-1]
	if suffix[0:2] in DIFF_SUFFIXES:
		return True
	elif suffix[0:3] in DIFF_SUFFIXES:
		return True
	else:
		return False

def define_colorbar_extremes(gdf: gpd.geodataframe.GeoDataFrame, var: str, diff: bool, targ: float, geometry_type: str):
	'''
	Defines minimum, maximum, and midpoint values for skewed colorbar. 
	These display the hotspot data in a way that is easier to visualize. 

	Parameter:
		gdf:			Pandas geodataframe. Geometry can be either MultiLineString or MultiPolygon.
		var:			Variable to plot. 
		diff:			Boolean indiciating whether or not we're plotting excess. 
		targ:			The target, defined in p.calc.NAME_OF_VAR <- we could define a dictionary of
							these so we don't have to keep passing the values. 
		geometry_type:	Indicator of whether we are plotting lines (reaches) or polygons (NHD catchments or HUCs)

	Returns:
		vmin:			Minimum value for colormap
		vmid:			Midpoint value for colormap
		vmax:			Maximum value for colormap
	'''
	gdp = gdf.loc[:,(var, 'geometry')] 
	if diff == False:
	    vmin = gdp[var].min()
	    vmid = targ
	    vmax = gdp[var].quantile(0.99)
	else:
	    vmin = targ
	    # Reaches
	    if geometry_type == "MultiLineString":
	    	vmid = gdf[var.split('_')[0] + '_conc'].quantile(0.90)
	    	vmax = gdf[var.split('_')[0] + '_conc'].quantile(0.99)
	    # NHD Catchments
	    elif geometry_type == "MultiPolygon":
	    	vmid = gdf[var.split('_')[0] + '_loadrate'].quantile(0.85)
	    	vmax = gdf[var.split('_')[0] + '_loadrate'].quantile(0.99)
	    # HUCs
	    elif geometry_type == "Polygon":
	    	try:
	    		vmid = gdf[var].quantile(0.90)
	    		vmax = gdf[var].quantile(0.99)
	    	except:
	    		vmid = gdf[var].quantile(0.90)
	    		vmax = gdf[var].quantile(0.99)


	print(f'Reach values (min, mid, max) = ({vmin}, {vmid}, {vmax})')    
	return vmin, vmid, vmax

def huc_var(gdf, var):
	'''
	Determines key variables for a HUC plot. 

	Parameters:
		gdf: 	Geodataframe of HUCs.
		var:	Should be 'natural', 'protected', or 'naturalprotected'

	Returns:
		var, vmin, vmax, title
	'''
	if var == 'natural':
		var = 'perc_natural'
		vmin = 0
		vmax = 30
		title = 'Percent Natural Land'
	elif var == 'protected':
		var = 'total_perc_protected'
		vmin = 0
		vmax = max(gdf[var])
		title = 'Percent Protected Land'
	elif var == 'naturalprotected':
		var = 'Tot_PercNatProtec'
		vmin = 0
		vmax = 30
		title = 'Percent Protected Natural Land'
	else:
		print("Please enter one of the following var types for a HUC plot:")
		print("'natural', 'protected', or 'naturalprotected'")
	return var, vmin, vmax, title

def plot(
			gdf: gpd.geodataframe.GeoDataFrame, var: str, targ: float,
			cmap = 'OrRd', line_width = 0.1, colorbar = True,
			height = 750, width = 500, tools = ['hover'], 
			basemap = gv.tile_sources.CartoLight(), 
			cnorm = 'log', skew_cbar = True):
	'''
	Main plotting function for all DRWI geometries, 
	including reaches (MultiLineString), NHD catchments (MultiPolygon),
	and HUCs (MultiPolygon)

	Parameters:
		gdf:			Pandas geodataframe. Geometry can be either MultiLineString or MultiPolygon.
		var:			Variable to plot. 
		targ:			The target, defined in p.calc.NAME_OF_VAR <- we could define a dictionary of
							these so we don't have to keep passing the values. 
		cmap: 			Color map for plotting. Default is the orange to red colormap. 
		line_width:		The width of lines to plot (currently used for Polygon boundaries only). Default is 0.1.
		colorbar:		Boolean indicating whether or not to plot colorbar. 
		height:			Height of plot
		width:			Width of plot 
		tools:			List indicating which bokeh tools to plot. Default is to just have hover. 
		basemap:		Which geoviews tile source to plot over (https://geoviews.org/user_guide/Working_with_Bokeh.html)
		cnorm:			Default to a logscale. 

	Returns:
		TBD
	''' 
	# determine if HUC plot 
	if gdf.index.name == 'huc12':
		huc = True
		if var in ['protected', 'natural', 'naturalprotected']:
			var, vmin, vmax, title = huc_var(gdf, var)
			cnorm = 'linear'
			gdf = gdf[gdf[var] < vmax]
		else:
			pass
	else:
		huc = False

	# Ensure variable is valid 
	if var not in gdf.columns:
		print("Invalid Variable. var must be in the list below:")
		if huc == True:
			if var in ['protected', 'natural', 'naturalprotected']:
				print(['protected', 'natural', 'naturalprotected'])
			else:
				print(gdf.columns)
			return
		else:
			print(gdf.columns)
			return 

	# Prepare GDF for plotting functions 
	gdf = prep_gdf(gdf)

	# Determine if difference variable
	diff = is_diff(var)

	# Define geometry type to determine which plotting function to use 
	geometry_type = gdf.geom_type.unique()[0]

	# Change colorbar
	if skew_cbar == True:
		vmin, vmid, vmax = define_colorbar_extremes(gdf, var, diff, targ, geometry_type)
		cmap = shift_color_map(cmap, vmin, vmid, vmax)
	else:
		pass

	# filter out any data less than target for xs plots
	if diff:
		gdf = gdf[gdf[var] > targ]
		# vmin = gdf[var].min()
	else:
		pass

	if geometry_type == "MultiLineString":
		map_plot = plot_lines(gdf, var, cmap=cmap, colorbar=colorbar, cnorm=cnorm, height=height, width=width, tools=tools, basemap=basemap, vmin=vmin, vmax=vmax)
	elif geometry_type in ["MultiPolygon", "Polygon"]:
		map_plot = plot_polys(gdf, var, cmap=cmap, line_width=line_width, colorbar=True, cnorm=cnorm, height=height, width=width, tools=tools, basemap=basemap, vmin=vmin, vmax=vmax)
	else:
		print(f"Error! Not equipped to handle {geometry_type}.")
		print("Please ensure your geometries are MultiLineString, MultiPolygon, or Polygon")
		return

	return map_plot 


def plot_polys(gdf: gpd.geodataframe.GeoDataFrame, var: str, **kwargs) -> gv.element.geo.Polygons:
	'''
	Plot polygons in the DRWI (e.g., NHD+ catchments or HUCs)
	https://towardsdatascience.com/10-examples-to-master-args-and-kwargs-in-python-6f1e8cc30749

	Parameters:
		gdf:	Pandas geodataframe with MultiPolygon geometry
		var:	Variable to plot. 

	Returns:
		poly_map: 	Polygon map colored by variable of choice plotted on basemap. 
	'''
	gdf[gdf.index.name] = gdf.index 
	poly_map = gv.Polygons(gdf, vdims=[gdf.index.name, var]).opts(
																	height = kwargs['height'],
																	width = kwargs['width'],
																	colorbar = kwargs['colorbar'],
																	color = var,
																	cmap = kwargs['cmap'],
																	cnorm = kwargs['cnorm'],
																	clim = (kwargs['vmin'], kwargs['vmax']),
																	line_width = kwargs['line_width'],
																	title = var,
																	tools = kwargs['tools']
												)
	return poly_map * kwargs['basemap'] 


def plot_lines(gdf: gpd.geodataframe.GeoDataFrame, var: str, **kwargs) -> gv.element.geo.Path:
	'''
	Plot lines in the DRWI (e.g., stream reaches)

	Parameters:
		gdf:	Pandas geodataframe with MultiLineStringGeometry
		var:	Variable to plot. 


	Returns:
		line_map: Polyline map colored by variable of choice plotted on basemap.  
	'''
	line_map = gv.Path(gdf, vdims=[var]).opts(
												height = kwargs['height'],
												width = kwargs['width'],
												color = var, 
												colorbar = kwargs['colorbar'],
												cnorm = kwargs['cnorm'], 
												cmap = kwargs['cmap'], 
												clim = (kwargs['vmin'], kwargs['vmax']),
												title = var, 
												# tools = kwargs['tools'],
											)

	return line_map * kwargs['basemap'] 




