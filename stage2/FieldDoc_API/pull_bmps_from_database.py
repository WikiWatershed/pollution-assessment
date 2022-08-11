import json
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import geopandas as gpd
import warnings
from pathlib import Path

warnings.filterwarnings('ignore', message='.*initial implementation of Parquet.*')

# Pull the data from the database
config_file = json.load(open('config.json'))
PG_CONFIG = config_file['PGtest']
PG_Connection = psycopg2.connect(
        host=PG_CONFIG['host'],
        database=PG_CONFIG['database'],
        user=PG_CONFIG['user'],
        password=PG_CONFIG['password'],
        port=PG_CONFIG['port'])

db_connection_url = "postgresql://{}:{}@{}:{}/{}".format(PG_CONFIG['user'], PG_CONFIG['password'],
                                                         PG_CONFIG['host'], PG_CONFIG['port'],
                                                         PG_CONFIG['database'])
con = create_engine(db_connection_url)

restoration_select = "select comid,huc12,practice_name,practice_id,program_name,program_id,organization,description," \
            "practice_type,created_at,modified_at,tn,tp,tss,bmp_size,bmp_size_unit,geom " \
            "from datapolassess.fd_api_restoration_comid order by practice_id, comid;"
restoration_df = gpd.read_postgis(restoration_select, con)
restoration_cols = ['comid', 'huc12', 'practice_name', 'practice_id','program_name','program_id','organization',
                    'description','practice_type','created_at','modified_at','bmp_size','bmp_size_unit','geometry']
restoration_df.set_index(['comid', 'practice_id'], inplace=True)

protection_select = "select comid,huc12,practice_name,practice_id,program_name,program_id,organization,description," \
                     "practice_type,created_at,modified_at,bmp_size,bmp_size_unit,geom from " \
                     "datapolassess.fd_api_protection_comid order by practice_id, comid;"
protection_df = gpd.read_postgis(protection_select, con)
protection_cols = ['comid', 'huc12', 'practice_name', 'practice_id', 'program_name', 'program_id', 'organization',
                   'description', 'practice_type', 'created_at', 'modified_at', 'tn', 'tp', 'tss', 'bmp_size',
                   'bmp_size_unit', 'geometry']
protection_df.set_index(['comid', 'practice_id'], inplace=True)

# Create the primary key for the dataset
restoration_df.sort_index(inplace=True)
protection_df.sort_index(inplace=True)

# OUTPUT TO GEOSPATIAL FORMAT
# TODO: CANNOT OUTPUT A DATE COLUMN
restoration_df_temp = restoration_df.drop(columns=['created_at', 'modified_at'])
restoration_df_temp.to_file('data_output/SHP/restoration_df.gpkg', driver='GPKG')
protection_df_temp = protection_df.drop(columns=['created_at', 'modified_at'])
protection_df_temp.to_file('data_output/SHP/protection_df.gpkg', driver='GPKG')

# Amend data types that aren't pulling from the database
restoration_df.huc12 = restoration_df.huc12.astype('category')
restoration_df.practice_name = restoration_df.practice_name.astype('category')
restoration_df.program_name = restoration_df.program_name.astype('category')
restoration_df.organization   = restoration_df.organization.astype('category')
restoration_df.description   = restoration_df.description.astype('category')
restoration_df.practice_type   = restoration_df.practice_type.astype('category')
restoration_df.created_at  = pd.to_datetime(restoration_df.created_at, utc=True)
restoration_df.modified_at = pd.to_datetime(restoration_df.modified_at, utc=True)
restoration_df.bmp_size_unit   = restoration_df.bmp_size_unit.astype('category')

protection_df.huc12 = protection_df.huc12.astype('category')
protection_df.practice_name = protection_df.practice_name.astype('category')
protection_df.program_name = protection_df.program_name.astype('category')
protection_df.organization   = protection_df.organization.astype('category')
protection_df.description   = protection_df.description.astype('category')
protection_df.practice_type   = protection_df.practice_type.astype('category')
protection_df.created_at  = pd.to_datetime(protection_df.created_at, utc=True)
protection_df.modified_at = pd.to_datetime(protection_df.modified_at, utc=True)
protection_df.bmp_size_unit   = protection_df.bmp_size_unit.astype('category')

restoration_df.head(3)
protection_df.head(3)

# Save the data to parquet files
data_folder = Path('data_output/')
restoration_df.to_parquet(data_folder /'restoration_df.parquet',compression='gzip')
protection_df.to_parquet(data_folder /'protection_df.parquet',compression='gzip')

