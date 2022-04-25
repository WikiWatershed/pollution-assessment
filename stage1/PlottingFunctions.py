from  matplotlib.colors import LogNorm
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib_scalebar.scalebar import ScaleBar
import geopandas as gpd
import plotly.express as px
from shapely.geometry import Polygon
import contextily as ctx

def CalcMinMax(reach_df, catch_df, var_reach, var_catch):
    '''
    Find the range of values to define color bar
    '''
    vmin = min(reach_df[reach_df[var_reach] > 0][var_reach].min(), catch_df[catch_df[var_catch] > 0][var_catch].min())
    vmax = max(reach_df[var_reach].max(), catch_df[var_catch].max())
    return vmin, vmax


def FormatAxes(ax, bounds=[-8.56 * 10**6,  -8.17 * 10**6, 4.65* 10**6, 5.26 * 10**6]):
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


def LatLonExtent(cluster_name, cluster_gdf):
    '''
    Define latitude and longitude extent of a particular cluster 
    '''

    # initialize list
    lats = []
    lons = []

    # get values
    values = cluster_gdf[cluster_gdf.index==cluster_name].geom.bounds
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


def PlotMaps(df_reach, df_catch, var_reach, var_catch, targ_reach, targ_catch, cl=None, cluster_gdf=None, fa=False, focusarea_gdf = None, zoom=False, diff=False, include_reach=False):
    '''
    creates a side by side map of data from reaches and subcatchments, and saves to SVG. 
    Might need to add: naming convention if restoration vs base. 
    Alternatively, can return the fig, ax so that manual adjustments can be made within the cell.
    '''


    # remove <0 values for plotting, setting to target/100
    dp_reach = df_reach.loc[:,(var_reach, 'geom')]  # Avoids 'SettingWithCopyWarning'. See https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copydp_catch = df_catch[[var_catch, 'geom_catchment']].copy()  # Make explict copy, to avoid 'SettingWithCopyWarning'
    dp_catch = df_catch.loc[:,(var_catch, 'geom_catchment')]  # Avoids 'SettingWithCopyWarning'. 
    
    mask_reach = dp_reach[var_reach] < targ_reach / 100
    mask_catch = dp_catch[var_catch] < targ_catch / 100
    
    dp_reach.loc[mask_reach,var_reach] = targ_reach / 100
    dp_catch.loc[mask_catch,var_catch] = targ_catch / 100

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
        max_reach = dp_reach[var_reach].max()
        max_catch = dp_catch[var_catch].max()
    else:
        min_reach = targ_reach / 100
        min_catch = targ_catch / 100
        mid_reach = targ_reach / 30
        mid_catch = targ_catch / 30
        max_reach = df_reach[var_reach.split('_')[0] + '_conc'].max()
        max_catch = df_catch[var_catch.split('_')[0] + '_loadrate'].max()

    # normalize around target with MidPointLogNorm
    r = dp_reach.plot(column=var_reach, lw=1, ax=ax1,
                      norm= MidPointLogNorm(vmin=min_reach,
                                            vmax=max_reach, 
                                            midpoint=mid_reach),
                      cmap = 'RdYlGn_r')# matplotlib.colors.LogNorm(vmin, vmax), cmap='RdYlGn_r')
    c = dp_catch.plot(column=var_catch, lw=0.1, ax=ax2, 
                      norm= MidPointLogNorm(vmin=min_catch,
                                            vmax=max_catch, 
                                            midpoint=mid_catch),
                      cmap='RdYlGn_r')
    if include_reach == True:
        if zoom == False:
            major_streams = df_reach[df_reach['streamorder'] >= 5].loc[:,('streamorder', 'geom')] 
            rch = major_streams.plot(linewidth=major_streams['streamorder'] % 4, ax=ax2, color='cornflowerblue', zorder=10)
        else:
            major_streams = df_reach[df_reach['streamorder'] >= 4].loc[:,('streamorder', 'geom')] 
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
        # fas = df_catch[df_catch.cluster == cl]['fa_name'].unique().dropna()
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
    sm = plt.cm.ScalarMappable(cmap='RdYlGn_r', 
                               norm= MidPointLogNorm(vmin=min_catch,
                                                     vmax=max_catch, 
                                                     midpoint=mid_catch))
    cbr = fig.colorbar(sm, cax=cax,)
    cbr.ax.tick_params(labelsize=8)
    cbr.ax.minorticks_off()

    # add colorbar - reach
    cax2 = fig.add_axes([0.48, 0.18, 0.02, 0.64]) # adjusts the position of the color bar: right position, bottom, width, top 
    sm2 = plt.cm.ScalarMappable(cmap='RdYlGn_r',
                               norm=MidPointLogNorm(vmin=min_reach,
                                                    vmax=max_reach, 
                                                    midpoint=mid_reach))
    cbr2 = fig.colorbar(sm2, cax=cax2,)
    cbr2.ax.minorticks_off()
    cbr2.ax.tick_params(labelsize=8) 

    for ax in [ax1, ax2]:
        if zoom==False:
            ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron, crs=dp_reach.crs.to_string(), zoom=7)
        else:
            # change zoom of basemap based on coverage area
            if area < 7:
                ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron, crs=dp_reach.crs.to_string(), zoom=10)
            else:
                ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron, crs=dp_reach.crs.to_string(), zoom=9)

    # naming - #cluster_FA_ZOOM_varreach_varcatch.svg
    # can adjust this convention as desired 
    if cl == None:
        cl_name = ""
    else:
        cl_name = cl + "_"
    if fa==False:
        fa_name = ""
    else:
        fa_name = "FA_"
    if zoom==False:
        zoom_name = ""
    else:
        zoom_name = "Zoom_"

    fig.tight_layout(pad=5)
    plt.savefig('figs/%s%s%s%s_%s.svg' % (cl_name, fa_name, zoom_name, var_reach, var_catch)) # to automatically save - can adjust dpi, etch 
    plt.savefig('figs/%s%s%s%s_%s.png' % (cl_name, fa_name, zoom_name, var_reach, var_catch)) # to automatically save - can adjust dpi, etch 
    plt.show()


def LatLonExtent_FA(fa_list, focusarea_gdf):
    '''
    Get lat and lon extent of a foucs area 
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


def PlotMaps_FA(df_reach, df_catch, var_reach, var_catch, targ_reach, targ_catch, cl=None, cluster_gdf=None, fa=None, focusarea_gdf=None, diff=False, include_reach=True):
    '''
    plot maps with focus areas
    '''
    # remove <0 values for plotting, setting to target/100
    dp_reach = df_reach.loc[:,(var_reach, 'geom')]  # Avoids 'SettingWithCopyWarning'. See https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copydp_catch = df_catch[[var_catch, 'geom_catchment']].copy()  # Make explict copy, to avoid 'SettingWithCopyWarning'
    dp_catch = df_catch.loc[:,(var_catch, 'geom_catchment')]  # Avoids 'SettingWithCopyWarning'. 
    
    mask_reach = dp_reach[var_reach] < targ_reach / 100
    mask_catch = dp_catch[var_catch] < targ_catch / 100
    
    dp_reach.loc[mask_reach,var_reach] = targ_reach / 100
    dp_catch.loc[mask_catch,var_catch] = targ_catch / 100

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
        max_reach = dp_reach[var_reach].max()
        max_catch = dp_catch[var_catch].max()
    else:
        min_reach = targ_reach / 100
        min_catch = targ_catch / 100
        mid_reach = targ_reach / 30
        mid_catch = targ_catch / 30
        max_reach = df_reach[var_reach.split('_')[0] + '_conc'].max()
        max_catch = df_catch[var_catch.split('_')[0] + '_loadrate'].max()

    # normalize around target with MidPointLogNorm
    r = dp_reach.plot(column=var_reach, lw=1, ax=ax1,
                      norm= MidPointLogNorm(vmin=min_reach,
                                            vmax=max_reach, 
                                            midpoint=mid_reach),
                      cmap = 'RdYlGn_r')# matplotlib.colors.LogNorm(vmin, vmax), cmap='RdYlGn_r')
    c = dp_catch.plot(column=var_catch, lw=0.1, ax=ax2, 
                      norm= MidPointLogNorm(vmin=min_catch,
                                            vmax=max_catch, 
                                            midpoint=mid_catch),
                      cmap='RdYlGn_r')
    if include_reach == True:
        major_streams = df_reach[df_reach['streamorder'] >= 3].loc[:,('streamorder', 'geom')] 
        
        
        rch = major_streams.plot(linewidth=(major_streams['streamorder'] - 1) / 2 , ax=ax2, color='cornflowerblue')
        # (major_streams['streamorder']+ 1) % 4

    # plot cluster, if applicable
    if cl != None:
        # Display Cluster Name
        print('Cluster Name = ', cl)
        # plot cluster
        # cl_reach = cluster_gdf[cluster_gdf.index == cl].plot(lw=1, ax=ax1, facecolor="none", edgecolor="black", zorder=10)
        # cl_catch = cluster_gdf[cluster_gdf.index == cl].plot(lw=1, ax=ax2, facecolor="none", edgecolor="black")

    # plot focus areas within clusters
    if fa == None:
        # fas = df_catch[df_catch.cluster == cl]['fa_name'].unique().dropna()
        fas_in_cluster = focusarea_gdf[focusarea_gdf.cluster == cl]
    else:
        fas_in_cluster = focusarea_gdf[focusarea_gdf.index.isin(fa)]
        # print("name discrepancies:", fas, focusarea_gdf.index.unique())
    
        # fas_in_cluster = focusarea_gdf.loc[fas, :]
    fa_reach = fas_in_cluster.plot(lw=1.25, ax = ax1, facecolor="none", edgecolor="black", zorder=10)
    fa_catch = fas_in_cluster.plot(lw=1.25, ax=ax2, facecolor = "none", edgecolor="black")

    # set figure size 
    fig.set_size_inches(12,12)

    if cl == None:
        print("No cluster entered!")
    else:
        lon_max, lon_min, lat_max, lat_min, area, h_v = LatLonExtent_FA(list(fas_in_cluster.index), focusarea_gdf)
        for ax in [ax1, ax2]:
            FormatAxes(ax, bounds=[lon_min, lon_max, lat_min, lat_max])

    # set axis titles
    ax1.set_title(var_reach + " (mg/L) for Reaches: \n %s Cluster" % cl)
    ax2.set_title(var_catch + " (kg/ha) for Catchments: \n %s Cluster" % cl)

    # add colorbar - catchment 
    cax = fig.add_axes([0.95, 0.3, 0.02, 0.4]) # adjusts the position of the color bar: right position, bottom, width, top 
    sm = plt.cm.ScalarMappable(cmap='RdYlGn_r', 
                               norm= MidPointLogNorm(vmin=min_catch,
                                                     vmax=max_catch, 
                                                     midpoint=mid_catch))
    cbr = fig.colorbar(sm, cax=cax,)
    cbr.ax.tick_params(labelsize=8)
    cbr.ax.minorticks_off()

    # add colorbar - reach
    cax2 = fig.add_axes([0.48, 0.3, 0.02, 0.4]) # adjusts the position of the color bar: right position, bottom, width, top 
    sm2 = plt.cm.ScalarMappable(cmap='RdYlGn_r',
                               norm=MidPointLogNorm(vmin=min_reach,
                                                    vmax=max_reach, 
                                                    midpoint=mid_reach))
    cbr2 = fig.colorbar(sm2, cax=cax2,)
    cbr2.ax.minorticks_off()
    cbr2.ax.tick_params(labelsize=8) 

    for ax in [ax1, ax2]:
       
        # change zoom of basemap based on coverage area
        if area < 0.05:
            ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron, crs=dp_reach.crs.to_string(), zoom=13)
        elif area < 1:
            ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron, crs=dp_reach.crs.to_string(), zoom=11)
        elif area < 4:
            ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron, crs=dp_reach.crs.to_string(), zoom=10)
#         elif area < 5:
#             ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron, crs=dp_reach.crs.to_string(), zoom=8)
        else:
            ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron, crs=dp_reach.crs.to_string(), zoom=9)

    # naming - #cluster_FA_ZOOM_varreach_varcatch.svg
    # can adjust this convention as desired 
    if cl == None:
        cl_name = ""
    else:
        cl_name = cl + "_"
    if fa==False:
        fa_name = ""
    else:
        fa_name = "FA_"
    
    for ax in [ax1, ax2]:
        ax.add_artist(ScaleBar(1))

    fig.tight_layout(pad=5)
    # plt.savefig('figs/%s%s%s%s_%s.svg' % (cl_name, fa_name, zoom_name, var_reach, var_catch)) # to automatically save - can adjust dpi, etch 
    # plt.savefig('figs/%s%s%s%s_%s.png' % (cl_name, fa_name, zoom_name, var_reach, var_catch)) # to automatically save - can adjust dpi, etch 
    plt.show()
    
    return [lon_max, lon_min, lat_max, lat_min]




def Extent_Map(df_catch, bounds_ls, cl, cluster_gdf, base_reach_gdf):
    '''
    Create an extent plot 
    '''

    # remove <0 values for plotting, setting to target/100
    
    fig, ax = plt.subplots(1,1)
    
    dp_catch = df_catch.loc[:,('geom_catchment')]
    dp_catch.plot(facecolor='grey', edgecolor='grey', ax=ax)
    
    for pts in bounds_ls:
        lats = [pts[2], pts[2], pts[3], pts[3]]
        lons = [pts[1], pts[0], pts[0], pts[1]]
        
        polygon_geom = Polygon(zip(lons, lats))
        p = gpd.GeoSeries(polygon_geom)
        d = p.plot(edgecolor='red', facecolor="none", ax=ax, lw=2)
    
    ax.set_title("Extent Map: \n %s Cluster" % cl) 
    
    cl_catch = cluster_gdf[cluster_gdf.index == cl].plot(lw=1, ax=ax, facecolor="none", edgecolor="black")

    df_reach = base_reach_gdf
    major_streams = df_reach[df_reach['streamorder'] >= 5].loc[:,('streamorder', 'geom')] 
    rch = major_streams.plot(linewidth=major_streams['streamorder'] % 4, ax=ax, color='cornflowerblue', zorder=10)

    fig.set_size_inches(8,8)
    FormatAxes(ax)
    ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron, crs=df_reach.crs.to_string(), zoom=7)



def PlotZoom(df_reach, df_catch, var_reach, var_catch, targ_reach, targ_catch, cl=None):
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
    r1 = df_reach.plot(column=var_reach, lw=1, ax=plot_order[0],
                          norm= MidPointLogNorm(vmin=df_reach[var_reach].min(),
                                                vmax=df_reach[var_reach].max(),
                                                midpoint=targ_reach),
                          cmap = 'RdYlGn_r')
    c1 = df_catch.plot(column=var_catch, lw=0.1, ax=plot_order[1],
                          norm= MidPointLogNorm(vmin=df_catch[var_catch].min(),
                                                vmax=df_catch[var_catch].max(),
                                                midpoint=targ_catch),
                          cmap='RdYlGn_r')

    r2 = df_reach.plot(column=var_reach, lw=1, ax=plot_order[2],
                          norm= MidPointLogNorm(vmin=df_reach[var_reach].min(),
                                                vmax=df_reach[var_reach].max(),
                                                midpoint=targ_reach),
                          cmap = 'RdYlGn_r')
    c2 = df_catch.plot(column=var_catch, lw=0.1, ax=plot_order[3],
                          norm= MidPointLogNorm(vmin=df_catch[var_catch].min(),
                                                vmax=df_catch[var_catch].max(),
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
            ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron, crs=df_reach.crs.to_string(), zoom=10)
        else:
            ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron, crs=df_reach.crs.to_string(), zoom=9)
    plt.show()