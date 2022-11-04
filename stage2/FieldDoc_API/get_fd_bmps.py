import os
import requests
import json
import sys
from geopandas import GeoDataFrame
from shapely.geometry import Polygon
from shapely.geometry import Point
from shapely.geometry import LineString
from shapely.geometry import MultiPoint
from shapely.geometry import MultiLineString
from shapely.geometry import MultiPolygon
from shapely.geometry import shape
import psycopg2

#%%
class drwiBmps:
    def __init__(self, _config_file, _rest_prot='restoration'):

        self._api_token = _config_file['fd_api_key']
        self._rest_prot = _rest_prot

        # Determine if we want to make a protection or restoration request
        if self._rest_prot == 'protection':
            # DRWI Protection Program IDs
            self._programs = '5,6,7'
        else:
            # DRWI Restoration Program IDs
            self._programs = '1,8,9'

        # Number of pages to iterate through, number of items per page (10 - 100)
        self._pages = list(range(1, 2))
        self._limit = str(100)

        # Set up request
        self._url = 'https://api.fielddoc.org/v1/practices/53062?access_token='
        self._url = ''.join((self._url, self._api_token))
        self._headers = {'Content-Type': 'application/json'}

        self._r_bmp = None
        self._r_practice_ids = None

    def get_practice_ids(self):
        self._r_practice_ids = []
        for page in self._pages:
            self._url = ''.join(('https://api.fielddoc.org/v1/practices?program=', self._programs, '&limit=100&page='))
            self._url = ''.join((self._url, str(page), '&access_token=', self._api_token))
            _r_practices = requests.get(self._url, headers=self._headers, allow_redirects=True, verify=True)
            _return = eval(str(json.loads(_r_practices.text)))
            for practice in _return['data']:
                self._r_practice_ids.append(practice['id'])
        return self._r_practice_ids

    def get_restoration_bmp_data(self):
        c = 0
        bmp_data = []
        print(
            'Requesting practice information for {} restoration practices'.format(
                len(self._r_practice_ids)
            )
        )
        for id in self._r_practice_ids:
            print(
                'Practice {} of {} ID: {}'.format(c + 1, len(self._r_practice_ids), id),
                end=' ',
            )
            # if c == 20:
            #     break
            single_bmp = {}
            self._url = ''.join(('https://api.fielddoc.org/v1/practices/', str(id), '?access_token=', self._api_token))
            self._r_bmp = requests.get(self._url, headers=self._headers, allow_redirects=True, verify=True)
            _return = eval(str(json.loads(self._r_bmp.text)))
            print(_return['name'])

            try:
                single_bmp['practice_name'] = _return['name']
                single_bmp['practice_id'] = _return['id']
                single_bmp['program_name'] = (
                    _return['program']['name']
                    if _return['program'] is not None
                    else None
                )
                single_bmp['program_id'] = _return['program']['id']
                single_bmp['organization'] = (
                    _return['organization']['name']
                    if _return['organization'] is not None
                    else None
                )
                single_bmp['description'] = _return['description']
                single_bmp['practice_type'] = (
                    _return['practice_type']['name']
                    if _return['practice_type'] is not None
                    else None
                )
                single_bmp['created_at'] = _return['created_at']
                single_bmp['modified_at'] = _return['modified_at']
                # single_bmp['geom'] = json.dumps(_return['geometry'])
                for feature in _return['metrics']['features']:
                    if feature['model_key'] == 'reduction_lbyr.tn':
                        single_bmp['tn'] = round(feature['current_value'], 2)
                if 'tn' not in single_bmp.keys():
                    single_bmp['tn'] = 0.0
                for feature in _return['metrics']['features']:
                    if feature['model_key'] == 'reduction_lbyr.tp':
                        single_bmp['tp'] = round(feature['current_value'], 2)
                if 'tp' not in single_bmp.keys():
                    single_bmp['tp'] = 0.0
                for feature in _return['metrics']['features']:
                    if feature['model_key'] == 'reduction_lbyr.tss':
                        single_bmp['tss'] = round(feature['current_value'], 2)
                if 'tss' not in single_bmp.keys():
                    single_bmp['tss'] = 0.0
                single_bmp['geom'] = _return['geometry']
                single_bmp['drainage_geom'] = _return['drainage_geometry']

                bmp_data.append(single_bmp)
            except Exception as e:
                single_bmp['practice_name'] = _return['name']
                single_bmp['practice_id'] = _return['id']
                single_bmp['program_name'] = (
                    _return['program']['name']
                    if _return['program'] is not None
                    else None
                )
                single_bmp['program_id'] = _return['program']['id']
                single_bmp['organization'] = (
                    _return['organization']['name']
                    if _return['organization'] is not None
                    else None
                )
                single_bmp['description'] = _return['description']
                single_bmp['practice_type'] = (
                    _return['practice_type']['name']
                    if _return['practice_type'] is not None
                    else None
                )
                single_bmp['created_at'] = _return['created_at']
                single_bmp['modified_at'] = _return['modified_at']
                # single_bmp['geom'] = json.dumps(_return['geometry'])
                single_bmp['tn'] = 0.0
                single_bmp['tp'] = 0.0
                single_bmp['tss'] = 0.0
                single_bmp['geom'] = _return['geometry']
                single_bmp['drainage_geom'] = _return['drainage_geometry']
                bmp_data.append(single_bmp)
                print('Problem with return!')
                print(e)
                print(_return)
            c += 1

        bmp_df = GeoDataFrame(bmp_data)
        bmp_df['geometry'] = bmp_df.apply(
            lambda row: shape(row['geom']) if row['geom'] is not None else None, axis=1
        )
        bmp_df['drainage_geometry'] = bmp_df.apply(
            lambda row: shape(row['drainage_geom'])
            if row['drainage_geom'] is not None
            else None,
            axis=1,
        )

        return bmp_df.drop(columns=["geom", "drainage_geom"]).copy(), bmp_data

    def get_protection_bmp_data(self):
        c = 0
        bmp_data = []
        print(
            'Requesting practice information for {} protection practices'.format(
                len(self._r_practice_ids)
            )
        )
        for id in self._r_practice_ids:
            print(
                'Practice {} of {} ID: {}'.format(c + 1, len(self._r_practice_ids), id),
                end=" ",
            )
            # if c == 20:
            #     break
            single_bmp = {}
            self._url = ''.join(('https://api.fielddoc.org/v1/practices/', str(id), '?access_token=', self._api_token))
            self._r_bmp = requests.get(self._url, headers=self._headers, allow_redirects=True, verify=True)
            _return = eval(str(json.loads(self._r_bmp.text)))
            print(_return['name'])

            try:
                single_bmp['practice_name'] = _return['name']
                single_bmp['practice_id'] = _return['id']
                single_bmp['program_name'] = (
                    _return['program']['name']
                    if _return['program'] is not None
                    else None
                )
                single_bmp['program_id'] = _return['program']['id']
                single_bmp['organization'] = (
                    _return['organization']['name']
                    if _return['organization'] is not None
                    else None
                )
                single_bmp['description'] = _return['description']
                single_bmp['practice_type'] = (
                    _return['practice_type']['name']
                    if _return['practice_type'] is not None
                    else None
                )
                single_bmp['created_at'] = _return['created_at']
                single_bmp['modified_at'] = _return['modified_at']

                for feature in _return['metrics']['features']:
                    if feature['model_key'] == 'tot_pwr':
                        single_bmp['tot_pwr'] = round(feature['current_value'], 2)
                if 'tot_pwr' not in single_bmp.keys():
                    single_bmp['tot_pwr'] = 0.0
                for feature in _return['metrics']['features']:
                    if feature['model_key'] == 'head_pwr':
                        single_bmp['head_pwr'] = round(feature['current_value'], 2)
                if 'head_pwr' not in single_bmp.keys():
                    single_bmp['head_pwr'] = 0.0
                for feature in _return['metrics']['features']:
                    if feature['model_key'] == 'nat_land':
                        single_bmp['nat_land'] = round(feature['current_value'], 2)
                if 'nat_land' not in single_bmp.keys():
                    single_bmp['nat_land'] = 0.0
                for feature in _return['metrics']['features']:
                    if feature['model_key'] == 'ara_pwr':
                        single_bmp['ara_pwr'] = round(feature['current_value'], 2)
                if 'ara_pwr' not in single_bmp.keys():
                    single_bmp['ara_pwr'] = 0.0
                for feature in _return['metrics']['features']:
                    if feature['model_key'] == 'wet_pwr':
                        single_bmp['wet_pwr'] = round(feature['current_value'], 2)
                if 'wet_pwr' not in single_bmp.keys():
                    single_bmp['wet_pwr'] = 0.0
                for feature in _return['metrics']['features']:
                    if feature['model_key'] == 'dev_land':
                        single_bmp['dev_land'] = round(feature['current_value'], 2)
                if 'dev_land' not in single_bmp.keys():
                    single_bmp['dev_land'] = 0.0
                for feature in _return['metrics']['features']:
                    if feature['model_key'] == 'ag_land':
                        single_bmp['ag_land'] = round(feature['current_value'], 2)
                if 'ag_land' not in single_bmp.keys():
                    single_bmp['ag_land'] = 0.0
                for feature in _return['metrics']['features']:
                    if feature['model_key'] == 'str_bank':
                        single_bmp['str_bank'] = round(feature['current_value'], 2)
                if 'str_bank' not in single_bmp.keys():
                    single_bmp['str_bank'] = 0.0
                single_bmp['geom'] = _return['geometry']
                bmp_data.append(single_bmp)
            except Exception as e:
                single_bmp['practice_name'] = _return['name']
                single_bmp['practice_id'] = _return['id']
                single_bmp['program_name'] = (
                    _return['program']['name']
                    if _return['program'] is not None
                    else None
                )
                single_bmp['program_id'] = _return['program']['id']
                single_bmp['organization'] = (
                    _return['organization']['name']
                    if _return['organization'] is not None
                    else None
                )
                single_bmp['description'] = _return['description']
                single_bmp['practice_type'] = (
                    _return['practice_type']['name']
                    if _return['practice_type'] is not None
                    else None
                )
                single_bmp['created_at'] = _return['created_at']
                single_bmp['modified_at'] = _return['modified_at']
                single_bmp['tot_pwr'] = 0.0
                single_bmp['head_pwr'] = 0.0
                single_bmp['ara_pwr'] = 0.0
                single_bmp['wet_pwr'] = 0.0
                single_bmp['str_bank'] = 0.0
                single_bmp['nat_land'] = 0.0
                single_bmp['dev_land'] = 0.0
                single_bmp['ag_land'] = 0.0
                single_bmp['geom'] = _return['geometry']
                bmp_data.append(single_bmp)
                print('Problem with return!')
                print(e)
                print(_return)
            c += 1

        bmp_df = GeoDataFrame(bmp_data)
        bmp_df['geometry'] = bmp_df.apply(
            lambda row: shape(row['geom']) if row['geom'] is not None else None, axis=1
        )
        return bmp_df.drop(columns=['geom']).copy(), bmp_data

    def import_restoration_bmps(self, _PG_Connection, _bmps_dict):
        for bmp in _bmps_dict:
            try:
                if bmp['drainage_geom'] is None:
                    cur = _PG_Connection.cursor()
                    cur.execute(
                        "insert into datapolassess.fd_api_restoration (practice_name ,practice_id ,program_name ,program_id ,"
                        "organization ,description ,practice_type ,created_at ,modified_at ,tn ,tp ,tss ,geom ) "
                        "values ('{}', {}, '{}', {}, '{}', '{}', '{}', '{}', '{}', {}, {}, {}, "
                        "ST_ForceCollection(ST_SetSRID('{}'::geometry, 4326))::geometry(geometrycollection,4326)"
                        ");".format(
                            str(bmp['practice_name']).replace("'", ""), bmp['practice_id'], bmp['program_name'],
                            bmp['program_id'],
                            str(bmp['organization']).replace("'", ""), str(bmp['description']).replace("'", ""),
                            str(bmp['practice_type']).replace("'", ""),
                            bmp['created_at'],
                            bmp['modified_at'], bmp['tn'], bmp['tp'], bmp['tss'],
                            shape(bmp["geom"]).wkb_hex))
                    _PG_Connection.commit()
                    cur.close()
                elif 'tp' not in bmp.keys():
                    cur = _PG_Connection.cursor()
                    cur.execute(
                        "insert into datapolassess.fd_api_restoration (practice_name ,practice_id ,program_name ,program_id ,"
                        "organization ,description ,practice_type ,created_at ,modified_at ,geom ) "
                        "values ('{}', {}, '{}', {}, '{}', '{}', '{}', '{}', '{}', "
                        "ST_ForceCollection(ST_SetSRID('{}'::geometry, 4326))::geometry(geometrycollection,4326)"
                        ");".format(
                            str(bmp['practice_name']).replace("'", ""), bmp['practice_id'], bmp['program_name'],
                            bmp['program_id'],
                            str(bmp['organization']).replace("'", ""), str(bmp['description']).replace("'", ""),
                            str(bmp['practice_type']).replace("'", ""),
                            bmp['created_at'], bmp['modified_at'],
                            shape(bmp["geom"]).wkb_hex))
                    _PG_Connection.commit()
                    cur.close()
                else:
                    cur = _PG_Connection.cursor()
                    cur.execute(
                        "insert into datapolassess.fd_api_restoration (practice_name ,practice_id ,program_name ,program_id ,"
                        "organization ,description ,practice_type ,created_at ,modified_at ,tn ,tp ,tss ,geom ,drainage_geom) "
                        "values ('{}', {}, '{}', {}, '{}', '{}', '{}', '{}', '{}', {}, {}, {}, "
                        "ST_ForceCollection(ST_SetSRID('{}'::geometry, 4326))::geometry(geometrycollection,4326)"
                        ", ST_Multi(ST_SetSRID('{}'::geometry, 4326))::geometry(multipolygon,4326));".format(
                            str(bmp['practice_name']).replace("'", ""), bmp['practice_id'], bmp['program_name'],
                            bmp['program_id'],
                            str(bmp['organization']).replace("'", ""), str(bmp['description']).replace("'", ""),
                            str(bmp['practice_type']).replace("'", ""), bmp['created_at'],
                            bmp['modified_at'], bmp['tn'], bmp['tp'], bmp['tss'],
                            shape(bmp["geom"]).wkb_hex,
                            shape(bmp["drainage_geom"]).wkb_hex))
                    _PG_Connection.commit()
                    cur.close()
            except Exception as e:
                print(bmp['practice_id'])
                print(e)
                # print(bmp['geom']['type'])
                # print(bmp['tn'])

    def import_protection_bmps(self, _PG_Connection, _bmps_dict):
        for bmp in _bmps_dict:
            try:
                if 'tot_pwr' not in bmp.keys():
                    cur = _PG_Connection.cursor()
                    cur.execute(
                        "insert into datapolassess.fd_api_protection (practice_name ,practice_id ,program_name ,program_id ,"
                        "organization ,description ,practice_type ,created_at ,modified_at ,geom ) "
                        "values ('{}', {}, '{}', {}, '{}', '{}', '{}', '{}', '{}', "
                        "ST_Multi(ST_SetSRID('{}'::geometry, 4326))::geometry(multipolygon,4326)"
                        ");".format(
                            str(bmp['practice_name']).replace("'", ""), bmp['practice_id'], bmp['program_name'],
                            bmp['program_id'],
                            str(bmp['organization']).replace("'", ""), str(bmp['description']).replace("'", ""),
                            str(bmp['practice_type']).replace("'", ""),
                            bmp['created_at'],bmp['modified_at'],
                            shape(bmp["geom"]).wkb_hex))
                    _PG_Connection.commit()
                    cur.close()
                else:
                    cur = _PG_Connection.cursor()
                    cur.execute(
                        "insert into datapolassess.fd_api_protection (practice_name ,practice_id ,program_name ,program_id ,"
                        "organization ,description ,practice_type ,created_at ,modified_at ,tot_pwr ,head_pwr ,ara_pwr,wet_pwr ,str_bank ,nat_land ,dev_land ,ag_land ,geom ) "
                        "values ('{}', {}, '{}', {}, '{}', '{}', '{}', '{}', '{}', '{}', {}, '{}', {}, '{}', '{}', '{}', '{}', "
                        "ST_Multi(ST_SetSRID('{}'::geometry, 4326))::geometry(multipolygon,4326)"
                        ");".format(
                            str(bmp['practice_name']).replace("'", ""), bmp['practice_id'], bmp['program_name'],
                            bmp['program_id'],
                            str(bmp['organization']).replace("'", ""), str(bmp['description']).replace("'", ""),
                            str(bmp['practice_type']).replace("'", ""),
                            bmp['created_at'],bmp['modified_at'],
                            bmp['tot_pwr'], bmp['head_pwr'], bmp['ara_pwr'], bmp['wet_pwr'],
                            bmp['str_bank'], bmp['nat_land'], bmp['dev_land'], bmp['ag_land'],
                            shape(bmp["geom"]).wkb_hex))
                    _PG_Connection.commit()
                    cur.close()
            except Exception as e:
                print(bmp['practice_id'])
                print(e)
                print(bmp['geom']['type'])
                # print(bmp['tn'])


#%%
if __name__ == '__main__':
    try:
        proj_type = sys.argv[1]
    except:
        proj_type = 'restoration'
        print("User can specify 'protection' or 'restoration' programs to generate the different output files.")
        print('Default: {}'.format(proj_type))

    config_file = json.load(open('config.json'))
    PG_CONFIG = config_file['PGtest']
    PG_Connection = psycopg2.connect(
        host=PG_CONFIG['host'],
        database=PG_CONFIG['database'],
        user=PG_CONFIG['user'],
        password=PG_CONFIG['password'],
        port=PG_CONFIG['port']
    )

    fielddoc = drwiBmps(_config_file=config_file, _rest_prot=proj_type)
    print('Getting {} Practice IDs'.format(proj_type))
    practices = fielddoc.get_practice_ids()
    print('{} {} practices found'.format(len(practices), proj_type))

    if proj_type == 'protection':
        print('Getting Protection Information')
        p_bmps_df, p_bmps_dict = fielddoc.get_protection_bmp_data()
        print(
            'Saving protection data on {} practices to parquet'.format(
                len(p_bmps_df.index)
            )
        )
        if len(p_bmps_df.index) != len(practices):
            print(
                '***WARNING: {} practices dropped in parquet export!***'.format(
                    len(practices) - len(p_bmps_df.index)
                )
            )
        p_bmps_df.to_parquet(
            os.path.realpath(
                os.path.join(
                    os.path.dirname(os.path.realpath(__file__)),
                    '../private/protection_bmps_from_FieldDoc.parquet',
                )
            )
        )
        print('Importing {} protection BMPs into database'.format(len(p_bmps_dict)))
        if len(p_bmps_dict) != len(practices):
            print(
                '***WARNING: {} practices dropped in database import!***'.format(
                    len(practices) - len(p_bmps_dict)
                )
            )
        fielddoc.import_protection_bmps(PG_Connection, p_bmps_dict)
    else:
        print('Getting Load Reduction Information')
        r_bmps_df, r_bmps_dict = fielddoc.get_restoration_bmp_data()
        print(
            'Saving data on {} restoration practices to parquet'.format(
                len(r_bmps_df.index)
            )
        )
        if len(r_bmps_df.index) != len(practices):
            print(
                '***WARNING: {} practices dropped in parquet export!***'.format(
                    len(practices) - len(r_bmps_df.index)
                )
            )
        r_bmps_df['drainage_geometry'] = r_bmps_df['drainage_geometry'].astype(
            "geometry"
        )
        r_bmps_df.to_parquet(
            os.path.realpath(
                os.path.join(
                    os.path.dirname(os.path.realpath(__file__)),
                    '../private/restoration_bmps_from_FieldDoc.parquet',
                )
            )
        )
        print('Importing {} restoration BMPs into database'.format(len(r_bmps_dict)))
        if len(r_bmps_dict) != len(practices):
            print(
                '***WARNING: {} practices dropped in database import!***'.format(
                    len(practices) - len(r_bmps_dict)
                )
            )
        fielddoc.import_restoration_bmps(PG_Connection, r_bmps_dict)
# %%
