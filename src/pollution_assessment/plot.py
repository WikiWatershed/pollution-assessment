from pathlib import Path

import numpy as np
import pandas as pd
import geopandas as gpd

# geo packages
from shapely.geometry import Polygon
import contextily as ctx

# packages for viz 
import matplotlib
import matplotlib.pyplot as plt
from matplotlib_scalebar.scalebar import ScaleBar
from  matplotlib.colors import LogNorm
import holoviews as hv
import colorcet as cc
from colorcet.plotting import swatch, swatches, sine_combs

from pollution_assessment import calc


# *****************************************************************************
# Functions
# *****************************************************************************

def CalcMinMax(
    reach_df: gpd.GeoDataFrame,
    catch_df: gpd.GeoDataFrame,
    var_reach: str,
    var_catch: str
):
    '''
    Find the range of values to define color bar
    '''
    vmin = min(reach_df[reach_df[var_reach] > 0][var_reach].min(), catch_df[catch_df[var_catch] > 0][var_catch].min())
    vmax = max(reach_df[var_reach].max(), catch_df[var_catch].max())
    return vmin, vmax


def FormatAxes(
    ax: plt.Axes,
    bounds: list = [-8.56 * 10**6,  -8.17 * 10**6, 4.65* 10**6, 5.26 * 10**6]):
    '''
    Format map axes
    Default is the full extent of the DRB
    '''
    ax.set_xlim(bounds[0], bounds[1])
    ax.set_ylim(bounds[2], bounds[3])
    ax.axes.xaxis.set_visible(False)
    ax.axes.yaxis.set_visible(False)


class MidPointLogNorm(LogNorm):
    '''
    Centers longscale colorbar around provided value
    Created using: https://stackoverflow.com/questions/48625475/python-shifted-logarithmic-colorbar-white-color-offset-to-center
    '''
    def __init__(self, vmin=None, vmax=None, midpoint=None, clip=False):
        LogNorm.__init__(self,vmin=vmin, vmax=vmax, clip=clip)
        self.midpoint=midpoint
    def __call__(self, value, clip=None):
        # I'm ignoring masked values and all kinds of edge cases to make a
        # simple example...
        x, y = [np.log(self.vmin), np.log(self.midpoint), np.log(self.vmax)], [0, 0.5, 1]
        return np.ma.masked_array(np.interp(np.log(value), x, y))


def LatLonExtent(
    cluster_name: str,
    cluster_gdf: gpd.GeoDataFrame
):
    '''
    Define latitude and longitude extent of a particular cluster 
    '''

    # initialize list
    lats = []
    lons = []

    # get values
    values = cluster_gdf[cluster_gdf.index==cluster_name].geometry.bounds
    y_extent = (values.maxy - values.miny) 
    x_extent = (values.maxx - values.minx)
    y_extent = y_extent[0] 
    x_extent = x_extent[0]
    
    # add 5 percent cushion
    x_cushion = x_extent * 0.05
    y_cushion = y_extent * 0.05

    # maintain aspect ratio 
    # start by finding base aspect ratio of full DRB 
    aspect = (5.26 * 10**6 - 4.65* 10**6)/ (8.56 * 10**6 - 8.17 * 10**6)
    base_aspect = (y_extent + y_cushion) / (x_extent + x_cushion)
    

    # adjust zoomed aspect ratio 
    if base_aspect > aspect:
        lat_max = values.maxy + y_cushion
        lat_min = values.miny - y_cushion
        
        x_tot = (y_extent + 2*y_cushion) / aspect 
        x_pad = (x_tot - x_extent) / 2
        
        lon_max = values.maxx + x_pad
        lon_min = values.minx - x_pad
        h_v = "vertical"

    elif base_aspect < aspect:
        lon_max = values.maxx + x_cushion
        lon_min = values.minx - x_cushion 
        
        y_tot = (x_extent + 2*x_cushion) * aspect
        y_pad = (y_tot - y_extent) / 2
        
        lat_max = values.maxy + y_pad
        lat_min = values.miny - y_pad
        
        h_v = "horizontal"
        
    else:
        lon_max = values.maxx + x_extent
        lon_min = values.minx - x_extent
        lat_max = values.maxy + y_extent
        lat_min = values.miny - y_extent
        
        h_v = "exact"
    
    
    # get area of new plot
    # used to define zoom level for basemap 
    area = x_extent*y_extent / 1000000000
    
    return lon_max[0], lon_min[0], lat_max[0], lat_min[0], area, h_v


def PlotMaps(gdf_reach, gdf_catch, 
    var_reach, var_catch, 
    targ_reach, targ_catch, 
    colormap='cet_CET_L18', 
    cl=None, cluster_gdf=None, 
    fa=False, focusarea_gdf=None, 
    zoom=False, diff=False, include_reach=False
):
    '''
    creates a side by side map of data from reaches and subcatchments, and saves to SVG. 
    Might need to add: naming convention if restoration vs base. 
    Alternatively, can return the fig, ax so that manual adjustments can be made within the cell.
    '''
    # create df for plot (dp), remove <0 values for plotting
    dp_reach = gdf_reach.loc[:,(var_reach, gdf_reach.geometry.name)]  # Avoids 'SettingWithCopyWarning'. See https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy. dp_catch = gdf_catch[[var_catch, 'geometry']].copy()  # Make explict copy, to avoid 'SettingWithCopyWarning'
    dp_catch = gdf_catch.loc[:,(var_catch, gdf_catch.geometry.name)]  # Avoids 'SettingWithCopyWarning'. 
    
    mask_reach = dp_reach[var_reach] < targ_reach / 10
    mask_catch = dp_catch[var_catch] < targ_catch / 10
    
    dp_reach.loc[mask_reach,var_reach] = targ_reach / 10
    dp_catch.loc[mask_catch,var_catch] = targ_catch / 10

    # initialize figure
    fig, (ax1, ax2) = plt.subplots(1,2)
    # ax3 = fig.add_axes([0.85, 0.1, 0.1, 0.8])
    
    #plot reach and catchment
    # keep min & max constant for all plots for each pollutant

    # Set midpoint lower if a difference calculation, 
    if diff == False:
        min_reach = dp_reach[var_reach].min()
        min_catch = dp_catch[var_catch].min()
        mid_reach = targ_reach
        mid_catch = targ_catch
        max_reach = dp_reach[var_reach].quantile(0.99)
        max_catch = dp_catch[var_catch].quantile(0.99)
    else:
        min_reach = targ_reach 
        min_catch = targ_catch 
        mid_reach = gdf_reach[var_reach.split('_')[0] + '_conc'].quantile(0.90)
        mid_catch = gdf_catch[var_catch.split('_')[0] + '_loadrate'].quantile(0.85)
        max_reach = gdf_reach[var_reach.split('_')[0] + '_conc'].quantile(0.99)
        max_catch = gdf_catch[var_catch.split('_')[0] + '_loadrate'].quantile(0.99)
    # Display min, mid, max
    print(f'Reach values (min, mid, max) = ({min_reach}, {mid_reach}, {max_reach})')    
    print(f'Catch values (min, mid, max) = ({min_catch}, {mid_catch}, {max_catch})')    

    # normalize around target with MidPointLogNorm
    lognorm_reach = MidPointLogNorm(vmin=min_reach,vmax=max_reach, 
                                    midpoint=mid_reach)
    lognorm_catch = MidPointLogNorm(vmin=min_catch,vmax=max_catch, 
                                    midpoint=mid_catch)

    
    # Set alphas so that reaches below the threshold are grey and catchments below threshold are transparent
    r_alphas = [1 if i < min_reach else 0 for i in dp_reach[var_reach]]
    c_alphas = [0 if i < min_catch else 1 for i in dp_catch[var_catch]]
    
    r = dp_reach.plot(column=var_reach, lw=1, ax=ax1,
                        norm=lognorm_reach,
                        cmap = colormap)# matplotlib.colors.LogNorm(vmin, vmax), cmap='RdYlGn_r')
    if zoom != False:
        r_below = dp_reach.plot(lw=1, ax=ax1, color='#D4DADC', alpha=r_alphas)
    
    c = dp_catch.plot(column=var_catch, lw=0.1, ax=ax2, 
                        norm=lognorm_catch,
                        cmap=colormap, alpha=c_alphas)
    if include_reach == True:
        if zoom == False:
            major_streams = gdf_reach[gdf_reach['streamorder'] >= 5].loc[:,('streamorder', gdf_reach.geometry.name)] 
            rch = major_streams.plot(linewidth=major_streams['streamorder'] % 4, ax=ax2, color='cornflowerblue', zorder=10)
        else:
            major_streams = gdf_reach[gdf_reach['streamorder'] >= 4].loc[:,('streamorder', gdf_reach.geometry.name)] 
            rch = major_streams.plot(linewidth=(major_streams['streamorder']+ 1) % 4, ax=ax2, color='cornflowerblue', zorder=10)

    # plot cluster, if applicable
    if cl != None:
        # Display Cluster Name
        print('Cluster Name = ', cl)
        # plot cluster
        cl_reach = cluster_gdf[cluster_gdf.index == cl].plot(lw=1, ax=ax1, facecolor="none", edgecolor="black", zorder=10)
        cl_catch = cluster_gdf[cluster_gdf.index == cl].plot(lw=1, ax=ax2, facecolor="none", edgecolor="black")

    # plot focus areas within clusters
    if fa == True:
        # fas = gdf_catch[gdf_catch.cluster == cl]['fa_name'].unique().dropna()
        fas_in_cluster = focusarea_gdf[focusarea_gdf.cluster == cl]
        # print("name discrepancies:", fas, focusarea_gdf.index.unique())
        
        # fas_in_cluster = focusarea_gdf.loc[fas, :]
        fa_reach = fas_in_cluster.plot(lw=0.7, ax = ax1, facecolor="none", edgecolor="grey", zorder=10)
        fa_catch = fas_in_cluster.plot(lw=0.7, ax=ax2, facecolor = "none", edgecolor="grey")

    # set figure size 
    fig.set_size_inches(12,12)

    # zoom in to cluster if zoom = True 
    if zoom == True:
        if cl == None:
            print("No cluster entered!")
        else:
            lon_max, lon_min, lat_max, lat_min, area, h_v = LatLonExtent(cl, cluster_gdf)
            for ax in [ax1, ax2]:
                FormatAxes(ax, bounds=[lon_min, lon_max, lat_min, lat_max])
    else:
        for ax in [ax1, ax2]:
            FormatAxes(ax)

    # set axis titles
    ax1.set_title(var_reach + " (mg/L) for Reaches")
    ax2.set_title(var_catch + " (kg/ha) for Catchments")

    # add colorbar - catchment 
    cax = fig.add_axes([0.95, 0.18, 0.02, 0.64]) # adjusts the position of the color bar: right position, bottom, width, top 
    sm = plt.cm.ScalarMappable(cmap=colormap, 
                               norm=lognorm_catch)
    cbr = fig.colorbar(sm, cax=cax,)
    cbr.ax.tick_params(labelsize=8)
    cbr.ax.minorticks_off()

    # add colorbar - reach
    cax2 = fig.add_axes([0.48, 0.18, 0.02, 0.64]) # adjusts the position of the color bar: right position, bottom, width, top 
    sm2 = plt.cm.ScalarMappable(cmap=colormap,
                               norm=lognorm_reach)
    cbr2 = fig.colorbar(sm2, cax=cax2,)
    cbr2.ax.minorticks_off()
    cbr2.ax.tick_params(labelsize=8) 

    for ax in [ax1, ax2]:
        if zoom==False:
            ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron, crs=dp_reach.crs.to_string(), zoom=7, interpolation='sinc')
            ctx.add_basemap(ax, source=ctx.providers.CartoDB.PositronOnlyLabels, crs=dp_reach.crs.to_string(), zoom=7, zorder=2, interpolation='sinc')
        else:
            # change zoom of basemap based on coverage area
            if area < 7:
                ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron, crs=dp_reach.crs.to_string(), zoom=10, interpolation='sinc')
                ctx.add_basemap(ax, source=ctx.providers.CartoDB.PositronOnlyLabels, crs=dp_reach.crs.to_string(), zoom=10, zorder=2, interpolation='sinc')
            else:
                ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron, crs=dp_reach.crs.to_string(), zoom=9, interpolation='sinc')
                ctx.add_basemap(ax, source=ctx.providers.CartoDB.PositronOnlyLabels, crs=dp_reach.crs.to_string(), zoom=9, zorder=2, interpolation='sinc')

    fig.tight_layout(pad=5)

    # Figure file naming - #cluster_FA_ZOOM_varreach_varcatch.svg
    # can adjust this convention as desired 
    if cl == None:
        cl_name = "DRWI_"
    else:
        cl_name = calc.clusters[cl] + "_"
    if fa==False:
        fa_name = ""
    else:
        fa_name = "FA_"
    if zoom==False:
        zoom_name = ""
    else:
        zoom_name = "Zoom_"

    # Save figure
    plt.savefig(
        Path.cwd() / 'figure_output' 
        / f'{cl_name}{fa_name}{zoom_name}{var_reach}_{var_catch}'
    )
    # Display figure
    plt.show()


def LatLonExtent_FA(
    fa_list: list,
    focusarea_gdf: gpd.GeoDataFrame
):
    '''
    Get lat and lon extent of a focus area 
    '''
    mn_x = np.inf
    mx_x = -np.inf
    mn_y = np.inf
    mx_y = -np.inf

    for fa in fa_list:
        values = focusarea_gdf[focusarea_gdf.index==fa].geometry.bounds
        try:
            mn_x = min(mn_x, values.minx[0])
            mx_x = max(mx_x, values.maxx[0])
            mn_y = min(mn_y, values.miny[0])
            mx_y = max(mx_y, values.maxy[0])
        except:
            pass
        
    y_extent = (mx_y - mn_y)
    x_extent = (mx_x - mn_x)


    # add 5 percent cushion
    x_cushion = x_extent * 0.05
    y_cushion = y_extent * 0.05

    aspect = 1 # (5.26 * 10**6 - 4.65* 10**6)/ (8.56 * 10**6 - 8.17 * 10**6)
    base_aspect = (y_extent + y_cushion) / (x_extent + x_cushion)


    if base_aspect > aspect:
        lat_max = mx_y + y_cushion
        lat_min = mn_y - y_cushion

        x_tot = (y_extent + 2*y_cushion) / aspect 
        x_pad = (x_tot - x_extent) / 2
        
        lon_max = mx_x + x_pad
        lon_min = mn_x - x_pad
        
        h_v = "vertical"

    elif base_aspect < aspect:
        lon_max = mx_x + x_cushion
        lon_min = mn_x - x_cushion 

        y_tot = (x_extent + 2*x_cushion) * aspect
        y_pad = (y_tot - y_extent) / 2

        lat_max = mx_y + y_pad
        lat_min = mn_y - y_pad

        h_v = "horizontal"

    else:
        lon_max = mx_x + x_extent
        lon_min = mn_x - x_extent
        lat_max = mx_y + y_extent
        lat_min = mn_y - y_extent

        h_v = "exact"
 
    area = x_extent*y_extent / 1000000000
    
    return lon_max, lon_min, lat_max, lat_min, area, h_v



def Extent_Map(
    gdf_catch: gpd.GeoDataFrame,
    bounds_ls: list,
    cl: str,
    cluster_gdf,
    base_reach_gdf):
    '''
    Create an extent plot 
    '''

    # remove <0 values for plotting, setting to target/100
    
    fig, ax = plt.subplots(1,1)
    
    dp_catch = gdf_catch.loc[:, gdf_catch.geometry.name]
    dp_catch.plot(facecolor='grey', edgecolor='grey', ax=ax)
    
    for pts in bounds_ls:
        lats = [pts[2], pts[2], pts[3], pts[3]]
        lons = [pts[1], pts[0], pts[0], pts[1]]
        
        polygon_geom = Polygon(zip(lons, lats))
        p = gpd.GeoSeries(polygon_geom)
        d = p.plot(edgecolor='red', facecolor="none", ax=ax, lw=2)
    
    ax.set_title("Extent Map: \n %s Cluster" % cl) 
    
    cl_catch = cluster_gdf[cluster_gdf.index == cl].plot(lw=1, ax=ax, facecolor="none", edgecolor="black")

    gdf_reach = base_reach_gdf
    major_streams = gdf_reach[gdf_reach['streamorder'] >= 5].loc[:,('streamorder', gdf_reach.geometry.name)] 
    rch = major_streams.plot(linewidth=major_streams['streamorder'] % 4, ax=ax, color='cornflowerblue', zorder=10)

    fig.set_size_inches(8,8)
    FormatAxes(ax)
    ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron, crs=gdf_reach.crs.to_string(), zoom=7)



def PlotZoom(gdf_reach, gdf_catch, var_reach, var_catch, targ_reach, targ_catch, cl=None):
    '''
    Can probably delete.
    '''
    # initialize figure
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2,2)
    fas_in_cluster = focusarea_gdf[focusarea_gdf.cluster == cl]
    
    # plot cluster
    for ax in [ax1, ax2, ax3, ax4]:
        cluster_gdf[cluster_gdf.index == cl].plot(lw=1, ax=ax, facecolor="none", edgecolor="black", zorder=10)
        fas_in_cluster.plot(lw=0.7, ax = ax, facecolor="none", edgecolor="grey", zorder=10)
        fig.set_size_inches(12,12)

    lon_max, lon_min, lat_max, lat_min, area, h_v  = LatLonExtent(cl)
    if h_v == "vertical":
        for ax in [ax1, ax2]:
            FormatAxes(ax, bounds=[lon_min, lon_max, lat_min + (lat_max - lat_min) / 2, lat_max])
        for ax in [ax3, ax4]:
            FormatAxes(ax, bounds=[lon_min, lon_max, lat_min, lat_min + (lat_max - lat_min) / 2])    
        plot_order = [ax1, ax2, ax3, ax4]
    elif h_v == 'horizontal':
        for ax in [ax1, ax3]:
            FormatAxes(ax, bounds=[lon_min, lon_min - (lon_min - lon_max) / 2, lat_min, lat_max])
        for ax in [ax2, ax4]:
            FormatAxes(ax, bounds=[lon_min - (lon_min - lon_max) / 2, lon_max, lat_min, lat_max])
        plot_order = [ax1, ax3, ax2, ax4]

    # plot reach and catchment
    # normalize around target with MidPointLogNorm
    r1 = gdf_reach.plot(column=var_reach, lw=1, ax=plot_order[0],
                          norm= MidPointLogNorm(vmin=gdf_reach[var_reach].min(),
                                                vmax=gdf_reach[var_reach].max(),
                                                midpoint=targ_reach),
                          cmap = 'RdYlGn_r')
    c1 = gdf_catch.plot(column=var_catch, lw=0.1, ax=plot_order[1],
                          norm= MidPointLogNorm(vmin=gdf_catch[var_catch].min(),
                                                vmax=gdf_catch[var_catch].max(),
                                                midpoint=targ_catch),
                          cmap='RdYlGn_r')

    r2 = gdf_reach.plot(column=var_reach, lw=1, ax=plot_order[2],
                          norm= MidPointLogNorm(vmin=gdf_reach[var_reach].min(),
                                                vmax=gdf_reach[var_reach].max(),
                                                midpoint=targ_reach),
                          cmap = 'RdYlGn_r')
    c2 = gdf_catch.plot(column=var_catch, lw=0.1, ax=plot_order[3],
                          norm= MidPointLogNorm(vmin=gdf_catch[var_catch].min(),
                                                vmax=gdf_catch[var_catch].max(),
                                                midpoint=targ_catch),
                          cmap='RdYlGn_r')

    if h_v == "vertical":
        ax1.set_title(var_reach + " for Reaches \n %s %s Cluster" % ("Northern", cl))
        ax2.set_title(var_catch + " for Catchments \n %s %s Cluster" % ("Northern", cl))
        ax3.set_title(var_reach + " for Reaches \n %s %s Cluster" % ("Southern", cl))
        ax4.set_title(var_catch + " for Catchments \n %s %s Cluster" % ("Southern", cl))
    elif h_v == "horizontal":
        ax1.set_title("%s for Reaches \n %s %s Cluster" % (var_reach, "Eastern", cl))
        ax2.set_title("%s for Reaches \n %s %s Cluster" % (var_reach, "Western", cl))
        ax3.set_title("%s for Catchments \n %s %s Cluster" % (var_catch, "Eastern", cl))
        ax4.set_title("%s for Catchments \n %s %s Cluster" % (var_catch, "Western", cl))        

    for ax in [ax1, ax2, ax3, ax4]:
        if area < 7:
            ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron, crs=gdf_reach.crs.to_string(), zoom=10, interpolation='sinc')
            ctx.add_basemap(ax, source=ctx.providers.CartoDB.PositronOnlyLabels, crs=gdf_reach.crs.to_string(), zoom=10, zorder=2, interpolation='sinc')
        else:
            ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron, crs=gdf_reach.crs.to_string(), zoom=9, interpolation='sinc')
            ctx.add_basemap(ax, source=ctx.providers.CartoDB.PositronOnlyLabels, crs=gdf_reach.crs.to_string(), zoom=9, zorder=2, interpolation='sinc')

    plt.show()
    


# SINGLE PANE PLOTTING FUNCTIONS
def remove_negatives(
    df_geom: gpd.GeoDataFrame,
    var_geom: str,
    targ_geom: float,
    comid_type: str
):
    
    dp_geom = df_geom.loc[:,(var_geom, df_geom.geometry.name)]
    
    mask_geom = dp_geom[var_geom] < targ_geom / 10
    
    dp_geom.loc[mask_geom,var_geom] = targ_geom / 10
    
    return(dp_geom)

def color_normalization_bounds(
    dp_geom: gpd.GeoDataFrame,
    df_geom: gpd.GeoDataFrame,
    var_geom: str,
    targ_geom: float,
    comid_type: str,
    diff: bool = False
):
    if comid_type == 'catchment':
        suffix = '_loadrate'
    if comid_type == 'reach':
        suffix = '_conc'
        
    if diff == False:
        min_geom = dp_geom[var_geom].min()
        mid_geom = targ_geom
        max_geom = dp_geom[var_geom].quantile(0.99)
    else:
        min_geom = targ_geom
        mid_geom = df_geom[var_geom.split('_')[0] + suffix].quantile(0.90)
        max_geom = df_geom[var_geom.split('_')[0] + suffix].quantile(0.99)
    
    return(min_geom, mid_geom, max_geom)


def set_transparent(
    min_geom: float,
    dp_geom: gpd.GeoDataFrame,
    var_geom: str,
    comid_type: str
):
    if comid_type == 'catchment':
        alphas = [0 if i < min_geom else 1 for i in dp_geom[var_geom]]
    if comid_type == 'reach':
        alphas = [1 if i < min_geom else 0 for i in dp_geom[var_geom]]

    return(alphas)


def plot_FA_boundaries(
    focusarea_gdf: gpd.GeoDataFrame,
    ax1: plt.Axes,
    cl: str = None,
    fa: str = None
):
    # plot focus areas within clusters
    if fa == None:
        fas_in_cluster = focusarea_gdf[focusarea_gdf.cluster == cl]
    else:
        fas_in_cluster = focusarea_gdf[focusarea_gdf.index.isin(fa)]
    
    fas_in_cluster.plot(lw=1.25,
                        ax=ax1,
                        facecolor="none",
                        edgecolor="black",
                        zorder=10)
    
    return(fas_in_cluster)


def add_basemap(
    ax: plt.Axes,
    area: float,
    dp_geom: gpd.GeoDataFrame
):
    if area < 0.05:
        ctx.add_basemap(ax,
                        source=ctx.providers.CartoDB.Positron,
                        crs=dp_geom.crs.to_string(),
                        zoom=13,
                        interpolation='sinc')
        ctx.add_basemap(ax,
                        source=ctx.providers.CartoDB.PositronOnlyLabels,
                        crs=dp_geom.crs.to_string(),
                        zoom=13,
                        zorder=2,
                        interpolation='sinc')
    elif area < 1:
        ctx.add_basemap(ax,
                        source=ctx.providers.CartoDB.Positron,
                        crs=dp_geom.crs.to_string(),
                        zoom=11,
                        interpolation='sinc')
        ctx.add_basemap(ax,
                        source=ctx.providers.CartoDB.PositronOnlyLabels,
                        crs=dp_geom.crs.to_string(),
                        zoom=11,
                        zorder=2,
                        interpolation='sinc')
    elif area < 4:
        ctx.add_basemap(ax,
                        source=ctx.providers.CartoDB.Positron,
                        crs=dp_geom.crs.to_string(),
                        zoom=10,
                        interpolation='sinc')
        ctx.add_basemap(ax,
                        source=ctx.providers.CartoDB.PositronOnlyLabels,
                        crs=dp_geom.crs.to_string(),
                        zoom=10,
                        zorder=2,
                        interpolation='sinc')
    else:
        ctx.add_basemap(ax,
                        source=ctx.providers.CartoDB.Positron,
                        crs=dp_geom.crs.to_string(),
                        zoom=9,
                        interpolation='sinc')
        ctx.add_basemap(ax,
                        source=ctx.providers.CartoDB.PositronOnlyLabels,
                        crs=dp_geom.crs.to_string(),
                        zoom=9,
                        zorder=2,
                        interpolation='sinc')

        
def add_colorbar(
    fig: plt.Figure,
    lognorm_geom,
    colormap: str = 'cet_CET_L18'
):
    cax2 = fig.add_axes([0.89, 0.107, 0.04, 0.733]) # adjusts the position of the color bar: right position, bottom, width, top 
    sm2 = plt.cm.ScalarMappable(cmap=colormap,
                               norm=lognorm_geom)
    cbr2 = fig.colorbar(sm2, cax=cax2,)
    cbr2.ax.minorticks_off()
    cbr2.ax.tick_params(labelsize=8)
    

def PlotMaps_FA_single_pane(
    df_geom: gpd.GeoDataFrame,
    var_geom: str,
    targ_geom: float,
    comid_type: str,
    colormap: str = 'cet_CET_L18',
    cl: str = None,
    cluster_gdf: gpd.GeoDataFrame = None,
    fa: str = None,
    focusarea_gdf: gpd.GeoDataFrame = None,
    include_reach: bool = False,
    streamorder_gdf: gpd.GeoDataFrame = None,
    diff: bool = False
):
    '''
    plot maps with focus areas
    '''
    # remove <0 values for plotting, setting to target/100
    dp_geom = remove_negatives(df_geom,
                               var_geom,
                               targ_geom,
                               comid_type)

    # initialize figure
    fig, ax1 = plt.subplots(figsize = (7,7))

    # Set midpoint lower if a difference calculation, 
    min_geom, mid_geom, max_geom = color_normalization_bounds(dp_geom,
                                                              df_geom,
                                                              var_geom,
                                                              targ_geom,
                                                              comid_type,
                                                              diff=False)

    # normalize around target with MidPointLogNorm
    lognorm_geom = MidPointLogNorm(vmin=min_geom,
                                   vmax=max_geom,
                                   midpoint=mid_geom)

    # Set alphas so that reaches below the threshold are grey and catchments below thrsehold are transparent
    alphas = set_transparent(min_geom,
                             dp_geom,
                             var_geom,
                             comid_type)

    # Plot catchments or reaches
    if comid_type == 'catchment':
        dp_geom.plot(column=var_geom,
                     lw=0.1,
                     ax=ax1,
                     norm=lognorm_geom,
                     zorder=1,
                     cmap=colormap,
                     alpha=alphas)
        
        if include_reach == True:
            major_streams = streamorder_gdf[streamorder_gdf['streamorder'] >= 3].loc[:,('streamorder', streamorder_gdf.geometry.name)] 

            rch = major_streams.plot(linewidth=(major_streams['streamorder'] - 1) / 2 , ax=ax1, color='cornflowerblue')
        
        ax1.set_title(var_geom + " (kg/ha) for Catchments: \n %s Cluster" % cl)

    if comid_type == 'reach':
        # Add streamreaches
        dp_geom.plot(column=var_geom,
                     lw=2,
                     ax=ax1,
                     norm=lognorm_geom,
                     zorder=1,
                     cmap=colormap)
        
        # Add grey streamreaches where threshold is met
        dp_geom.plot(lw=1.5,
                     ax=ax1,
                     color='#D4DADC',
                     alpha=alphas)

        ax1.set_title(var_geom + " (mg/L) for Reaches: \n %s Cluster" % cl)

    # Plot cluster, if applicable
    if cl != None:
        # Display Cluster Name
        print('Cluster Name = ', cl)

    # Plot focus areas within clusters
    fas_in_cluster = plot_FA_boundaries(focusarea_gdf, ax1, cl=cl, fa=fa)

    # Zoom to boundary
    if cl == None:
        print("No cluster entered!")
    else:
        lon_max, lon_min, lat_max, lat_min, area, h_v = LatLonExtent_FA(list(fas_in_cluster.index), focusarea_gdf)
        FormatAxes(ax1, bounds=[lon_min, lon_max, lat_min, lat_max])

    # Add basemap
    add_basemap(ax1, area, dp_geom)
    
    # Format & add scalebar & colorbar
    ax1.add_artist(ScaleBar(1))

    add_colorbar(fig, lognorm_geom, colormap)
 
    fig.tight_layout(pad=5)
    
    # naming - #cluster_FA_ZOOM_varreach_varcatch.svg
    # can adjust this convention as desired 
    if cl == None:
        cl_name = "DRWI_"
    else:
        cl_name = calc.clusters[cl] + "_"
        
    if fa==False:
        fa_name = ""
    else:
        fa_name = "FA_"
        
    # Save figure
    plt.savefig(
        Path.cwd() / 'figure_output' 
        / f'{cl_name}{fa_name}{var_geom}_{comid_type}'
    )
    plt.show()
    
    return [lon_max, lon_min, lat_max, lat_min, fig]


def plot_remaining_work(
    gdf: gpd.GeoDataFrame,
    threshold: int,
    incl_boundary: bool = False,
    boundarygdf: gpd.GeoDataFrame = None
):
    thresholds = [30, 55, 85]
    
    if threshold not in thresholds:
        raise ValueError('Invalid threshold. Expected one of: %s' % thresholds)
    
    fig, ax = plt.subplots(figsize=(12,12))

    cmap_min = 0
    cmap_max = 100
    
    # normalize around target with MidPointLogNorm
    norm = matplotlib.colors.Normalize(vmin=cmap_min,
                                       vmax=cmap_max)
    
    # Only include areas where meeting the threshold of protected natural land is feasible
    feasible_gdf = gdf[gdf['perc_natural'] >= threshold]
        

    feasible_gdf.plot(column=feasible_gdf['Tot_PercNatProtec'],
                      ax=ax,
                      norm=norm,
                      cmap='cet_CET_CBTL4')
    
    if incl_boundary == True:
        boundarygdf.plot(ax=ax,
                         facecolor='none',
                         edgecolor='grey',
                         lw=0.8)


    # Format axis
    FormatAxes(ax)
        
    # Turn off ticks
    plt.tick_params(axis='x',
                    bottom=False,
                    labelbottom=False)
    plt.tick_params(axis='y',
                    left=False,
                    labelleft=False)
    
    # Add title
    plt.title(f'Percent of Protected Natural Land in HUC 12s where \n{threshold}% Natural Land Threshold is Possible')

    # Add colorbar
    cax = fig.add_axes([0.78, 0.13, 0.03, 0.732])
    sm = plt.cm.ScalarMappable(norm=norm, cmap='cet_CET_CBTL4')
    cbr = fig.colorbar(sm, cax=cax,)
    cbr.ax.tick_params(labelsize=10)
    cbr.ax.minorticks_off()

    # Add basemap and labels
    ctx.add_basemap(ax,
                    source=ctx.providers.CartoDB.Positron,
                    crs=feasible_gdf.crs.to_string(),
                    zoom=7,
                    zorder=0,
                    interpolation='sinc')
    ctx.add_basemap(ax,
                    source=ctx.providers.CartoDB.PositronOnlyLabels,
                    crs=feasible_gdf.crs.to_string(),
                    zoom=7,
                    zorder=2,
                    interpolation='sinc')
    
    
    plt.savefig(
        Path.cwd() / 'figure_output' 
        / f'{threshold}_naturalland'
    )
        
    plt.show()

    
    
    
def plot_protec_nat(
    naturalland_gdf: gpd.GeoDataFrame,
    nat_protect_type: str,
    incl_boundary: bool = False,
    boundary_gdf: bool = None
):
    nat_protect_types = ['natural', 'protected', 'naturalprotected']
    if nat_protect_type not in nat_protect_types:
        raise ValueError('Invalid nat_protec_type. Expected one of: %s' % nat_protect_types)
        
    fig, ax = plt.subplots(figsize=(12,12))
    
    # Colormap options: https://holoviews.org/user_guide/Colormaps.html
    # Add protected lands to plot
    if nat_protect_type == 'natural':
        plot_column = 'perc_natural'
        title = 'Percent Natural Land'
        cmap = 'Greens'
    elif nat_protect_type == 'protected':
        plot_column = 'total_perc_protected'
        title = 'Percent Protected Land'
        cmap = 'YlGn'
    elif nat_protect_type == 'naturalprotected':
        plot_column = 'Tot_PercNatProtec'
        title = 'Percent Protected Natural Land'
        cmap = 'Greens'
    
    # normalize around target with MidPointLogNorm
    norm = matplotlib.colors.Normalize(vmin=0,
                                       vmax=100)
        
    if incl_boundary == True:
        naturalland_gdf.plot(column=plot_column,
                             ax=ax,
                             cmap=cmap,
                             norm=norm)
        boundary_gdf.plot(ax=ax,
                          facecolor='none',
                          edgecolor='black')
    else:
        naturalland_gdf.plot(column=plot_column,
                             ax=ax,
                             cmap=cmap,
                             norm=norm)

    # Format axis
    FormatAxes(ax)
    
    # Turn off ticks
    plt.tick_params(axis='x',
                    bottom=False,
                    labelbottom=False)
    plt.tick_params(axis='y',
                    left=False,
                    labelleft=False)
    
    # Add title
    plt.title(title, fontsize=14)

    # Add colorbar
    cax = fig.add_axes([0.78, 0.13, 0.03, 0.732])
    sm = plt.cm.ScalarMappable(cmap=cmap,
                               norm=norm)
    cbr = fig.colorbar(sm,
                       cax=cax)
    cbr.ax.tick_params(labelsize=10)
    cbr.ax.minorticks_off()


    # Add basemap and labels
    ctx.add_basemap(ax,
                    source=ctx.providers.CartoDB.Positron,
                    crs=naturalland_gdf.crs.to_string(),
                    zoom=7,
                    interpolation='sinc')
    ctx.add_basemap(ax,
                    source=ctx.providers.CartoDB.PositronOnlyLabels,
                    crs=naturalland_gdf.crs.to_string(),
                    zoom=7,
                    zorder=2,
                    interpolation='sinc')
    
    plt.savefig(
        Path.cwd() / 'figure_output' 
        / f'all_{nat_protect_type}_lands'
    )
    
    
    plt.show()