# Import packages
from pathlib import Path
import pandas as pd
import geopandas as gpd


# *****************************************************************************
# Functions
# *****************************************************************************

def summary_stats(
    gdf: gpd.GeoDataFrame,
    rest: bool = False
) -> pd.DataFrame:
    
    """Summarize protection and restoration efforts by practice,
    project count, area, and load reduction (where applicable).
    
    Args:
        gdf: GeoDataFrame containing the protection or restoration
            efforts to be summarized
        rest: Boolean to switch between restoration and protection
        
    Returns:
        A DataFrame of count, area, and load reduction by practice as well
        as totals.
    """
    
    if gdf.crs.to_string() != 'ESRI:102003':
        gdf.to_crs(crs='ESRI:102003', inplace=True)
    
    gdf['area_ac'] = gdf.geometry.area/4046.86
    
    column_list = gdf.columns
    
    if 'OBJECTID' in column_list:
        count = gdf.groupby('RECLASS2')['OBJECTID'].count()
        area = round(gdf.groupby('RECLASS2')['area_ac'].sum(),2)
        
    else:
        count = gdf.groupby('practice_type')['practice_id'].count()
        
        has_geom = gdf[gdf['geometry'] != None]
        
        area = round(gdf.groupby('practice_type')['area_ac'].sum(),2)
        
    if rest == True: 
        tn_load_reduced = gdf.groupby('practice_type')['tn'].sum()
        tp_load_reduced = gdf.groupby('practice_type')['tp'].sum()        
        tss_load_reduced = gdf.groupby('practice_type')['tss'].sum()       
        
        frame = {'practice_count': count, 'area_ac': area,
                 'tn_load_reduced': tn_load_reduced,
                 'tp_load_reduced': tp_load_reduced,
                 'tss_load_reduced': tss_load_reduced}
        
        summary_df = pd.DataFrame(frame)
        
        totals_dict = {'practice_type': 'TOTAL',
                       'practice_count': summary_df['practice_count'].sum(),
                       'area_ac': summary_df['area_ac'].sum(),
                       'tn_load_reduced': summary_df['tn_load_reduced'].sum(),
                       'tp_load_reduced': summary_df['tp_load_reduced'].sum(),
                       'tss_load_reduced': summary_df['tss_load_reduced'].sum()}
    
    else:
        frame = {'practice_count': count, 'area_ac': area}
        
        summary_df = pd.DataFrame(frame)
        
        totals_dict = {'practice_type': 'TOTAL',
                       'practice_count': summary_df['practice_count'].sum(),
                       'area_ac': summary_df['area_ac'].sum()}

    totals = pd.DataFrame([totals_dict]).set_index('practice_type')
    
    summary_df = pd.concat([summary_df, totals])
    
    summary_df = summary_df[summary_df['practice_count'] > 0]
    
    return(summary_df)


def PADEP_BMPS_summary_stats(
    df: pd.DataFrame
) -> pd.DataFrame:
    
    
    """Summarize PA DEP BMPs by county, area, and load reduction.
    
    Args:
        df: DataFrame containing the protection or restoration
            efforts to be summarized
        
    Returns:
        A DataFrame of count, area, and load reduction by practice as well
        as totals.
    """
        
    df1 = df[df['units2'] == 'ac']
    df2 = df[df['units2'] == 'ft']

    count = df.groupby('bmp/practice')['id'].count()

    area = round(df.groupby('bmp/practice')['extent2'].sum(),2)

    tn_load_reduced = round(df.groupby('bmp/practice')['tn_lbs_reduced'].sum(),2)
    tp_load_reduced = round(df.groupby('bmp/practice')['tp_lbs_reduced'].sum(),2)
    tss_load_reduced = round(df.groupby('bmp/practice')['tss_lbs_reduced'].sum(),2)

    frame = {'id_count': count,
             'area_ac': area,
             'tn_load_reduced': tn_load_reduced,
             'tp_load_reduced': tp_load_reduced,
             'tss_load_reduced': tss_load_reduced}

    summary_df1 = pd.DataFrame(frame)


    count = df2.groupby('bmp/practice')['id'].count()

    length = round(df2.groupby('bmp/practice')['extent2'].sum(),2)

    tn_load_reduced = round(df1.groupby('bmp/practice')['tn_lbs_reduced'].sum(),2)
    tp_load_reduced = round(df1.groupby('bmp/practice')['tp_lbs_reduced'].sum(),2)
    tss_load_reduced = round(df1.groupby('bmp/practice')['tss_lbs_reduced'].sum(),2)

    frame = {'id_count': count,
             'length_ft': length}

    summary_df2 = pd.DataFrame(frame)

    summary_df = summary_df1.merge(summary_df2, left_index=True, right_index=True, how='outer')
    summary_df['id_count'] = summary_df['id_count_x'].fillna(0) + summary_df['id_count_y'].fillna(0)
    summary_df = summary_df.drop(['id_count_x', 'id_count_y'], axis=1)

    totals_dict = {'bmp/practice': 'TOTAL',
                   'id_count': summary_df['id_count'].sum(),
                   'area_ac': summary_df['area_ac'].sum(),
                   'length_ft': summary_df['length_ft'].sum(),
                   'tn_load_reduced': summary_df['tn_load_reduced'].sum(),
                   'tp_load_reduced': summary_df['tp_load_reduced'].sum(),
                   'tss_load_reduced': summary_df['tss_load_reduced'].sum()}

    totals = pd.DataFrame([totals_dict]).set_index('bmp/practice')

    summary_stats = pd.concat([summary_df, totals])

    return(summary_stats)


def PA_NJ_rest_summary_stats(
    df: pd.DataFrame,
    FIPS_df: pd.DataFrame
) -> pd.DataFrame:
    
    """Summarize PA & NJ agricultural and developed load reduction efforts
    by practice, area, and load reduction.
    
    Args:
        df: DataFrame containing the protection or restoration
            efforts to be summarized
        FIPS_df: DataFrame of FIPS codes and county names
        
    Returns:
        A DataFrame of count and load reduction (both agricultural and developed
        by practice as wellas totals.
    """
    
    count = df.groupby('county_FIPS')['comid'].count()
  
    ag_tn_load_reduced = round(df.groupby('county_FIPS')['tn_ag_reduction_lbs'].sum(),2)
    ag_tp_load_reduced = round(df.groupby('county_FIPS')['tp_ag_reduction_lbs'].sum(),2)
    ag_tss_load_reduced = round(df.groupby('county_FIPS')['tss_ag_reduction_lbs'].sum(),2)
    dev_tn_load_reduced = round(df.groupby('county_FIPS')['tn_dev_reduction_lbs'].sum(),2)
    dev_tp_load_reduced = round(df.groupby('county_FIPS')['tp_dev_reduction_lbs'].sum(),2)
    dev_tss_load_reduced = round(df.groupby('county_FIPS')['tss_dev_reduction_lbs'].sum(),2)

    frame = {'comid_count': count,
             'tn_ag_reduction_lbs': ag_tn_load_reduced,
             'tp_ag_reduction_lbs': ag_tp_load_reduced,
             'tss_ag_reduction_lbs': ag_tss_load_reduced,
             'tn_dev_reduction_lbs': dev_tn_load_reduced,
             'tp_dev_reduction_lbs': dev_tp_load_reduced,
             'tss_dev_reduction_lbs': dev_tss_load_reduced}

    summary_df = pd.DataFrame(frame)

    totals_dict = {'county': 'TOTAL',
                   'comid_count': summary_df['comid_count'].sum(),
                   'tn_ag_reduction_lbs': summary_df['tn_ag_reduction_lbs'].sum(),
                   'tp_ag_reduction_lbs': summary_df['tp_ag_reduction_lbs'].sum(),
                   'tss_ag_reduction_lbs': summary_df['tss_ag_reduction_lbs'].sum(),
                   'tn_dev_reduction_lbs': summary_df['tn_dev_reduction_lbs'].sum(),
                   'tp_dev_reduction_lbs': summary_df['tp_dev_reduction_lbs'].sum(),
                   'tss_dev_reduction_lbs': summary_df['tss_dev_reduction_lbs'].sum()}

    summary_df = summary_df.reset_index()
    
    # There was a hidden formatting issue that was only resolved by converting
    # all FIPS first to integers then to strings then encoding those strings to utf-8
    # https://stackoverflow.com/questions/6269765/what-does-the-b-character-do-in-front-of-a-string-literal
    FIPS_df = FIPS_df.astype({'FIPS': 'str'})
    summary_df = summary_df.astype({'county_FIPS': 'int'})
    summary_df = summary_df.astype({'county_FIPS': 'str'})
    
    FIPS_df['FIPS'] = FIPS_df['FIPS'].str.encode('utf-8')
    summary_df['county_FIPS'] = summary_df['county_FIPS'].str.encode('utf-8')

    summary_df = summary_df.merge(FIPS_df, left_on='county_FIPS', right_on='FIPS', how='left')
    
    #summary_df['FIPS'] = summary_df['FIPS'].str.decode('utf-8')
    
    summary_df = summary_df.set_index('County')
    summary_df = summary_df.drop(['county_FIPS', 'FIPS'], axis=1)
    
    totals = pd.DataFrame([totals_dict]).set_index('county')

    summary_stats = pd.concat([summary_df, totals])
    
    return(summary_stats)