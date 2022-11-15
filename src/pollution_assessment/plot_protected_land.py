from pathlib import Path
import geopandas as gpd
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
from  matplotlib.colors import LogNorm
import contextily as ctx
import colorcet as cc
from colorcet.plotting import swatch, swatches, sine_combs
import holoviews as hv
import pollution_assessment as pa
from pollution_assessment import plot


# *****************************************************************************
# Functions
# *****************************************************************************

# Create label series for DRWI bar charts
def bar_labels(
    bar_values: pd.Series
) -> pd.Series:
    """ Create a series of labels for bar charts. Excluding any zero values
    
    Args:
        bar_values: values to use as labels
    
    Returns:
        A series of labels including blanks.
    """
    
    labels = ["" if x < 0.01 else round(x,1) for x in bar_values]
    return labels


# Sort focus areas by natural land levels
def sort_nat_levels(
    natland_gdf: gpd.GeoDataFrame,
    low_bound: float = 30,
    high_bound: float = 55
) -> list:
    """ Sort focus areas into three degrees of natural land cover based on user 
    specified bounds.
    
    Args:
        natland_gdf: Dataframe containing 'perc_natural' column
        low_bound: threshold between low and medium degrees of natural land cover 
                (default=30)
        high_bound: threshold between medium and high degrees of natural land 
                cover (default=55)
        
    Returns:
        Three lists of indices from the input geodataframe: indices with a low 
        level of natural cover, indices with a medium level of natural land
        cover, and indices with a high level of natural land cover. 
    """
    
    high_nat = natland_gdf[natland_gdf['perc_natural'] > high_bound].index.to_list()
    med_nat = natland_gdf[natland_gdf['perc_natural'].between(low_bound,high_bound)].index.to_list()
    low_nat = natland_gdf[natland_gdf['perc_natural'] < low_bound].index.to_list()
    
    return high_nat, med_nat, low_nat


# Single panel bar chart of natural land
def plot_natural(
    sorted_gdf: gpd.GeoDataFrame,
    legend_loc: str, anchor: list,
    huc: bool = False
):
    """ Plot a barchart of natural lands in a dataframe.
    
    Args: 
        sorted_gdf: GeoDataFrame containing 'perc_natural', 'WCPA_PercNat',
                & 'FieldDoc_PercNat' columns. The order of this gdf will
                determine the order of horizontal bars in the final plot.
        legend_loc: location of legend. Valid options include: 'upper left', 
                'upper right', 'lower left', 'lower right', 'upper center', 
                'lower center', 'center left', 'center right', 'center' & 'best'
        anchor: location to anchor the legend to (see bbox_to_anchor for more)
        
    Returns:
        A singular stacked bar chart showing percent of lands that are DRWI protected 
        natural lands and WeConservePA protected natural lands overlaying the total 
        percent of natural lands in a particular geometry. DRWI bars are labeled
        according to their individual with. 
    """
    
    # Plot all natural land
    if huc == True: 
        natcolor = '#BAC3CB'
    else:
        natcolor = 'lightslategrey'
        
    natural = plt.barh(sorted_gdf.index, 
                       width=sorted_gdf['perc_natural'],
                       color=natcolor) 

    # Plot perc natural protected by WCPA
    wcpa_width = sorted_gdf['WCPA_PercNat']
    nonDRWI = plt.barh(sorted_gdf.index, width=wcpa_width, color='#FAC748')

    # Plot perc natural protected by DRWI
    fd_width = sorted_gdf['FieldDoc_PercNat']
    fd_width = fd_width.fillna(0)
    DRWI = plt.barh(sorted_gdf.index, width=fd_width, color='#6EAF46', left=wcpa_width)
    labels = ["" if x < 0.01 else round(x,1) for x in fd_width]
    
    if huc == True:
        opp_width = sorted_gdf['OppParcel_Perc']
        opp = plt.barh(sorted_gdf.index, width=opp_width, color='lightslategrey', left=(fd_width+wcpa_width))
    else:
        plt.bar_label(DRWI, labels, color='white', padding=5, label_type='edge')

    thirty = plt.axvline(30, color='red', linestyle=':')
    nitrate = plt.axvline(55, color='darkorange', linestyle=':')
    biodiversity = plt.axvline(85, color='#0343DF', linestyle=':')
    plt.xlim(0,100)
    
    cols = sorted_gdf.columns
    if 'huc12' in cols:
        plt.xlabel('Percent of Entire HUC12')
    else:
        plt.xlabel('Percent of Entire FA')
    plt.autoscale(enable=True, axis='y', tight=True)

    if huc ==True:
        plt.legend([natural, DRWI, nonDRWI, opp, thirty, nitrate, biodiversity],
                   ['All Natural Land', 'DRWI Protected \nNatural Land', 
                    'Other Protected \nNatural Land',
                    'Opportunity Parcels',
                    'Thirty-by-Thirty Goal', 'Nitrate Goal', 
                    'Excellent Reference \nConditions for Aquatic \nBiodiversity Goal'],
                   loc=legend_loc, bbox_to_anchor=anchor, frameon=False)
    else:
        plt.legend([natural, DRWI, nonDRWI, thirty, nitrate, biodiversity],
                   ['All Natural Land', 'DRWI Protected \nNatural Land', 
                    'Other Protected \nNatural Land',
                    'Thirty-by-Thirty Goal', 'Nitrate Goal', 
                    'Excellent Reference \nConditions for Aquatic \nBiodiversity Goal'],
                   loc=legend_loc, bbox_to_anchor=anchor, frameon=False)

    
# Create bar chart of natural lands and protection practices broken down by cluster
def plot_natural_cluster(
    fa_gdf: gpd.GeoDataFrame,
    legend_loc: str,
    anchor: list
):
    """ Plot a barchart of natural lands in a cluster.
    
    Args: 
        fa_gdf: GeoDataFrame containing 'perc_natural', 'WCPA_PercNat',
                & 'FieldDoc_PercNat' columns. The order of this gdf will
                determine the order of horizontal bars in the final plot.
        legend_loc: location of legend. Valid options include: 'upper left', 
                'upper right', 'lower left', 'lower right', 'upper center', 
                'lower center', 'center left', 'center right', 'center' & 'best'
        anchor: location to anchor the legend to (see bbox_to_anchor for more)
        
    Returns:
        A 4x2 grid of focus area percent natural lands broken up by cluster. Each 
        panel includes a singular stacked bar chart showing percent of lands that are 
        DRWI protected natural lands and WeConservePA protected natural lands 
        overlaying the total percent of natural lands in a particular geometry. DRWI 
        bars are labeled according to their individual with. 
    """
    
    clusters = fa_gdf['cluster'].unique()
    clusters = clusters.tolist()

    fig, ax = plt.subplots(4,2,figsize=(20,25))
    n=1

    for cluster in clusters:
        # Select just FAs within a given cluster and sort by percent natural
        sgdf = fa_gdf[fa_gdf['cluster'] == cluster]
        sorted_gdf = sgdf.sort_values('perc_natural')

        # Initiate plot
        ax = plt.subplot(4,2,n) 

        plot_natural(sorted_gdf, legend_loc, anchor)

        n = n+1

        plt.title(f'{cluster} Natural Lands')

    plt.tight_layout(h_pad=2, w_pad=-7)

    

# Create bar chart of natural lands and protection practices broken down by natural land level
def plot_natural_level(
    fa_gdf: gpd.GeoDataFrame,
    legend_loc: str,
    anchor: list,
    nat_low_bound: float = 30,
    nat_high_bound: float = 55
):
    """ Plot a barchart of natural lands in a a given natural land level (high, 
    medium, or low.
    
    Args: 
        fa_gdf: GeoDataFrame containing 'perc_natural', 'WCPA_PercNat',
                & 'FieldDoc_PercNat' columns. The order of this gdf will
                determine the order of horizontal bars in the final plot.
        legend_loc: location of legend. Valid options include: 'upper left', 
                'upper right', 'lower left', 'lower right', 'upper center', 
                'lower center', 'center left', 'center right', 'center' & 'best'
        anchor: location to anchor the legend to (see bbox_to_anchor for more)
        low_bound: threshold between low and medium degrees of natural land cover 
                (default=30)
        high_bound: threshold between medium and high degrees of natural land 
                cover (default=55)
        
    Returns:
        A 4x2 grid of focus area percent natural lands broken up by cluster. Each 
        panel includes a singular stacked bar chart showing percent of lands that are 
        DRWI protected natural lands and WeConservePA protected natural lands 
        overlaying the total percent of natural lands in a particular geometry. DRWI 
        bars are labeled according to their individual with. 
    """
    
    # Initiate figure 
    fig, ax = plt.subplots(3,1, figsize=(15,30))

    fa_gdf['all_protected_nat'] = fa_gdf['WCPA_PercNat'] + fa_gdf['FieldDoc_PercNat']
    n=1

    high_nat, med_nat, low_nat = sort_nat_levels(fa_gdf, nat_low_bound, nat_high_bound)
    
    nat_level = [high_nat, med_nat, low_nat]

    for i in nat_level:
        ax = plt.subplot(3,1,n)
        n=n+1

        sub_fajoin_gdf = fa_gdf[fa_gdf.index.isin(i)]

        sorted_gdf = sub_fajoin_gdf.sort_values('all_protected_nat')

        plot_natural(sorted_gdf, legend_loc, anchor)

        if i == high_nat:
            title = '55 - 100% Natural Lands'
        if i == med_nat:
            title = '30 - 55% Natural Lands'
        if i == low_nat:
            title = '0 - 30% Natural Lands'

        plt.title(f'Delaware River Basin Focus Area: {title}')
    
    plt.savefig(
        Path.cwd() / 'figure_output/focusarea_natural_land_barplot',
        bbox_inches='tight'
    )
        

# Create bar chart of natural lands and protection practices broken down by natural land level
def plot_hucs_natural_level(
    huc_gdf: gpd.GeoDataFrame,
    legend_loc: str,
    anchor: list,
    nat_low_bound: float = 30,
    nat_high_bound: float = 55
):
    """ Plot a barchart of natural lands in a a given natural land level (high, 
    medium, or low.
    
    Args: 
        fa_gdf: GeoDataFrame containing 'perc_natural', 'WCPA_PercNat',
                & 'FieldDoc_PercNat' columns. The order of this gdf will
                determine the order of horizontal bars in the final plot.
        legend_loc: location of legend. Valid options include: 'upper left', 
                'upper right', 'lower left', 'lower right', 'upper center', 
                'lower center', 'center left', 'center right', 'center' & 'best'
        anchor: location to anchor the legend to (see bbox_to_anchor for more)
        low_bound: threshold between low and medium degrees of natural land cover 
                (default=30)
        high_bound: threshold between medium and high degrees of natural land 
                cover (default=55)
        
    Returns:
        A 4x2 grid of focus area percent natural lands broken up by cluster. Each 
        panel includes a singular stacked bar chart showing percent of lands that are 
        DRWI protected natural lands and WeConservePA protected natural lands 
        overlaying the total percent of natural lands in a particular geometry. DRWI 
        bars are labeled according to their individual with. 
    """
    
    # Initiate figure 
    fig, ax = plt.subplots(2,1, figsize=(15,95), gridspec_kw={'height_ratios': [5.7, 1]})

    n=1

    high_nat, med_nat, low_nat = sort_nat_levels(huc_gdf, nat_low_bound, nat_high_bound)
    
    nat_level = [high_nat, med_nat]
    
    # Drop hucs where there are no opportunity parcels
    opp_huc_gdf = huc_gdf[huc_gdf['OppParcel_Perc'] > 1]

    for i in nat_level:
        ax = plt.subplot(2,1,n)
        n=n+1

        sub_huc_gdf = opp_huc_gdf[opp_huc_gdf.index.isin(i)]
        
        sub_huc_gdf = sub_huc_gdf.reset_index()
        sub_huc_gdf = sub_huc_gdf.rename(columns={'index': 'huc12'})
        sub_huc_gdf = sub_huc_gdf.set_index('HUC12 Name')

        sorted_gdf = sub_huc_gdf.sort_values('Tot_PercNatProtec')

        plot_natural(sorted_gdf, 'lower right', (1.2,0.78), huc=True)

        if i == high_nat:
            title = '55 - 100% Natural Lands'
        if i == med_nat:
            title = '0 - 55% Natural Lands'

        plt.title(f'Delaware River Basin HUC12: {title}')
        
    plt.savefig(
        Path.cwd() / 'figure_output/huc_natural_land_barplot',
        bbox_inches='tight'
    )