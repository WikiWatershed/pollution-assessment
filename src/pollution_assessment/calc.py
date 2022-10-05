import pandas as pd
import geopandas as gpd


# *****************************************************************************
# Global variable objects
# *****************************************************************************

# Two COMID types, for distingusing data for a stream reach vs it's catchment
comid_types = ['reach', 'catch']
"""list: The two geometry COMID types, 'reach' and 'catch', for distingusing 
data for a stream reach vs it's catchment
"""


# Pollutants dictionary, for name conversions & interating over keys or values
pollutants = {
    'TotalN': 'tn', 
    'TotalP': 'tp',
    'Sediment': 'tss',
}
"""dict: Pollutants dictionary, for name conversions & interating over keys 
or values.

The keys are the labels returned by Sara D's `run_srat_with_bmps.py`.  
The values are the abbreviations used for the Pollution Assessent.
"""


# Threshold/Target Values for Acceptable Water Quality

# Catchment Target Load Rate (kg/ha)
tn_loadrate_target  = 17.07  # Includes Organic N
tp_loadrate_target  = 0.31
tss_loadrate_target = 923.80

# Reach Target Concenctration (mg/l)
tn_conc_target  = 4.73  # Includes Organic N
tp_conc_target  = 0.09
tss_conc_target = 237.30

# Create a dictionary of these Targets, to use later for iterating functions
targets = {
    'tn':  {'loadrate_target':tn_loadrate_target,
                   'conc_target': tn_conc_target},
           'tp':  {'loadrate_target':tp_loadrate_target,
                   'conc_target': tp_conc_target},
           'tss': {'loadrate_target':tss_loadrate_target,
                   'conc_target': tss_conc_target}
          }


# Run Groups
run_groups = {
    0: 'No restoration or protection', 
    1: 'Direct WPF Restoration', 
    2: 'Direct and Indirect WPF Restoration', 
    3: 'All Restoration', 
    4: 'Direct WPF Protection'
}

comid_test_dict = {
    4648450:    'no point sources',
    4648684:    'Upper E Branch Brandywine',
    932040160:  'large point sources',
    2583195:    'protection projects',
    932040230:  'restoration and protection projects',
}

# *****************************************************************************
# Functions
# *****************************************************************************

def select_run(
    comid_type: str,
    df_in: pd.DataFrame, 
    group: str, 
    ps: bool = False,
) -> pd.DataFrame:
    """ Select wikiSRAT results by run_group and source group
    
    Select a single set of values for every COMID, by selecting the run group 
    and whether or not you want point source values or totals (default)

    Args:
        df_in: WikiSRAT results for multiple run groups
        comid_type: 'reach' or 'catch'
        group: Run group name
        ps: Values derived from point sources (True) or totals from all 
            sources (False). Defaults to False.

    Returns:
        A dataframe of a single set of wikiSRAT results for every COMID,
        with COMID set as the index. 
    """
    if comid_type == 'reach':
        ps_name = 'Point Source Derived Concentration'
    elif comid_type == 'catch':
        ps_name = 'Point Sources'
    else:
        print("Error: comid_type must be 'reach' or 'catch'")

    if ps == True:
        df_out = df_in.loc[
            (df_in.run_group == group) &
            (df_in.Source == ps_name)
            ]
    else:
        df_out = df_in.loc[
            (df_in.run_group == group) &
            (df_in.Source != ps_name)
            ]
    
    df_out.set_index('comid', inplace=True)

    return df_out


def join_results(
    comid_type: str,
    gdf: gpd.GeoDataFrame,
    df_in: pd.DataFrame, 
    group: str, 
    ps: bool = False,
) -> gpd.GeoDataFrame:
    """ Join geodataframe with selected wikiSRAT results
    
    Args:
        comid_type: 'reach' or 'catch'
        gdf: PA2 results GeoDataFrame with geometries for mapping
        df_in: WikiSRAT results for multiple run groups
        group: Run group name
        ps: Values derived from point sources (True) or totals from all 
            sources (False). Defaults to False.

    Returns:
        A GeoDataFrame of a single set of wikiSRAT results for every COMID,
        with COMID set as the index. 
    """
    df = select_run(comid_type, df_in, group, ps)
    
    gdf_results = gdf.join(df)
    gdf_results.drop(['huc', 'gwlfe_endpoint','huc_level'], axis='columns', inplace=True)

    return gdf_results


def calc_loadrate(
    gdf: gpd.GeoDataFrame,
    df_in: pd.DataFrame, 
    pollutant_key: str,
    group: str, 
    ps: bool = False,
) -> pd.DataFrame:
    """ Select and calculate area-normalized catchment loading rates (kg/ha/y)
    for a single pollutant from any run_group.
    
    Args:
        gdf: GeoDataFrame with geometries for mapping and related info
        df_in: WikiSRAT results for multiple run groups
        group: Run group name
        pollutant_key: Key to pollutant dict
        ps: Values derived from point sources (True) or totals from all 
            sources (False). Defaults to False.

    Returns:
        The input GeoDataFrame with three extra `_loadrate` columns added .
    """
    df = select_run('catch', df_in, group, ps)

    df_out = df[f'{pollutant_key}'] / gdf.catchment_hectares
    
    return df_out


def add_loadrate(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """ Add baseline calculated area-normalized catchment loading rate (kg/ha/y)
    columns to the combined results GeoDataFrame.
    
    Args:
        gdf: PA2 results GeoDataFrame with geometries for mapping

    Returns:
        The input GeoDataFrame with three extra `_loadrate` columns added .
    """
    # Calculate and add each new column by looping through `pollutants` dict
    for pollutant in pollutants.keys():
        gdf [f'{pollutants[pollutant]}_loadrate'] = (
            gdf[f'{pollutant}'] / gdf.catchment_hectares)
    
    return gdf


def add_excess(comid_type: str, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """ Add calculated excess pollution columns to the combined PA2 results GeoDataFrame
    
    excess pollution = total pollution – threshold pollution target

    Args:
        gdf: PA2 results GeoDataFrame with geometries for mapping

    Returns:
        The input GeoDataFrame with three extra `_xs` columns added .
    """

    calc_suffix = 'xs'

    if comid_type == 'reach':
        quantity_type = 'conc'
    elif comid_type == 'catch':
        quantity_type = 'loadrate'
    else:
        print("Error: comid_type must be 'reach' or 'catch'")
    
    # Calculate and add each new column by looping through `pollutants` dict
    for pollutant in pollutants.values():
        gdf[f'{pollutant}_{quantity_type}_{calc_suffix}'] = (
            gdf[f'{pollutant}_{quantity_type}'] 
            - targets[pollutant][f'{quantity_type}_target']
        )
    return gdf


def add_xsnps(
    comid_type: str, 
    gdf: gpd.GeoDataFrame,
    df_in: pd.DataFrame,
) -> gpd.GeoDataFrame:
    """ Add calculated excess non-point source pollution columns to the combined 
    PA2 results GeoDataFrame.
    
    excess nonpoint source pollution = excess pollution 
                                   – point source pollution

    Args:
        comid_type: 'reach' or 'catch'
        gdf: PA2 results GeoDataFrame with geometries for mapping
        df_in: WikiSRAT results for multiple run groups
        group: Run group name
        ps: Values derived from point sources (True) or totals from all 
            sources (False). Defaults to False.

    Returns:
        The input GeoDataFrame with three extra `_xs` columns added .
    """

    calc_suffix = 'xsnps'
    input_suffix = 'xs'

    if comid_type == 'reach':
        quantity_type = 'conc'
        normalize_by = 1
    elif comid_type == 'catch':
        quantity_type = 'loadrate'
        normalize_by = gdf.catchment_hectares
    else:
        print("Error: comid_type must be 'reach' or 'catch'")

    df = select_run(comid_type, df_in, run_groups[0], ps=True)

    # Calculate and add each new column by looping through `pollutants` dict
    for pollutant in pollutants.items():
        gdf[f'{pollutant[1]}_{quantity_type}_{calc_suffix}'] = (
            gdf[f'{pollutant[1]}_{quantity_type}_{input_suffix}'] 
            - df[f'{pollutant[0]}'] / normalize_by
        )
    # Set 'tss_loadrate_xsnps' = 'tss_loadrate_xs', to avoid NaN
    if comid_type == 'catch':
        gdf['tss_loadrate_xsnps'] = gdf['tss_loadrate_xs']

    return gdf


def add_remaining(
    comid_type: str, 
    gdf: gpd.GeoDataFrame,
    df_in: pd.DataFrame,
    group_key: int, 
) -> gpd.GeoDataFrame:
    """ Add calculated "remaining" pollution columns to the PA2 results GeoDataFrame.
    
    Remaining pollution is calcuated by subtracting reductions from restoration 
    from excess non-point source pollution.
    
    excess nonpoint source pollution = excess pollution 
                                   – point source pollution

    Args:
        comid_type: 'reach' or 'catch'
        gdf: PA2 results GeoDataFrame with geometries for mapping
        df_in: WikiSRAT results for multiple run groups
        group_key: Run group key

    Returns:
        The input GeoDataFrame with three extra `_xs` columns added .
    """

    calc_suffix = f'rem{group_key}'
    input_suffix = 'xsnps'

    if comid_type == 'reach':
        quantity_type = 'conc'
        normalize_by = 1
    elif comid_type == 'catch':
        quantity_type = 'loadrate'
        normalize_by = gdf.catchment_hectares
    else:
        print("Error: comid_type must be 'reach' or 'catch'")

    base_df = select_run(comid_type, df_in, run_groups[0], ps=False)
    rest_df = select_run(comid_type, df_in, run_groups[group_key], ps=False)

    # Calculate and add each new column by looping through `pollutants` dict
    for pollutant in pollutants.items():
        reduced_df = (  base_df[f'{pollutant[0]}'] 
                      - rest_df[f'{pollutant[0]}'])/ normalize_by
        
        gdf[f'{pollutant[1]}_{quantity_type}_{calc_suffix}'] = (
            gdf[f'{pollutant[1]}_{quantity_type}_{input_suffix}'] 
            - reduced_df
        )
    
    return gdf


def add_avoided(
    comid_type: str, 
    gdf: gpd.GeoDataFrame,
    df_in: pd.DataFrame,
    group_key: int, 
) -> gpd.GeoDataFrame:
    """ Add calculated "avoided" pollution columns to the PA2 results GeoDataFrame.
    
    Avoided pollution is calcuated by subtracting .
    
    Args:
        comid_type: 'reach' or 'catch'
        gdf: PA2 results GeoDataFrame with geometries for mapping
        df_in: WikiSRAT results for multiple run groups
        group_key: Run group key

    Returns:
        The input GeoDataFrame with three extra `_avoid` columns added .
    """

    calc_suffix = f'avoid'

    if comid_type == 'reach':
        quantity_type = 'conc'
        normalize_by = 1
    elif comid_type == 'catch':
        quantity_type = 'loadrate'
        normalize_by = gdf.catchment_hectares
    else:
        print("Error: comid_type must be 'reach' or 'catch'")

    base_df = select_run(comid_type, df_in, run_groups[0], ps=False)
    prot_df = select_run(comid_type, df_in, run_groups[group_key], ps=False)

    # Calculate and add each new column by looping through `pollutants` dict
    for pollutant in pollutants.items():
        avoided_df = (- base_df[f'{pollutant[0]}'] 
                      + prot_df[f'{pollutant[0]}']
                     )/ normalize_by
        
        gdf[f'{pollutant[1]}_{quantity_type}_{calc_suffix}'] = ( 
            + avoided_df
        )
    
    return gdf