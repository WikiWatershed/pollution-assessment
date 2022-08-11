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
        for id in self._r_practice_ids:
            # if c == 20:
            #     break
            single_bmp = {}
            self._url = ''.join(('https://api.fielddoc.org/v1/practices/', str(id), '?access_token=', self._api_token))
            self._r_bmp = requests.get(self._url, headers=self._headers, allow_redirects=True, verify=True)
            _return = eval(str(json.loads(self._r_bmp.text)))

            try:
                single_bmp['practice_name'] = _return['name']
                single_bmp['practice_id'] = _return['id']
                single_bmp['program_name'] = _return['program']['name']
                single_bmp['program_id'] = _return['program']['id']
                single_bmp['organization'] = _return['organization']['name']
                single_bmp['description'] = _return['description']
                single_bmp['practice_type'] = _return['practice_type']['name']
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
            except:
                single_bmp['practice_name'] = _return['name']
                single_bmp['practice_id'] = _return['id']
                single_bmp['program_name'] = _return['program']['name']
                single_bmp['program_id'] = _return['program']['id']
                single_bmp['organization'] = _return['organization']['name']
                single_bmp['description'] = _return['description']
                single_bmp['practice_type'] = _return['practice_type']['name']
                single_bmp['created_at'] = _return['created_at']
                single_bmp['modified_at'] = _return['modified_at']
                # single_bmp['geom'] = json.dumps(_return['geometry'])
                single_bmp['tn'] = 0.0
                single_bmp['tp'] = 0.0
                single_bmp['tss'] = 0.0
                single_bmp['geom'] = _return['geometry']
                single_bmp['drainage_geom'] = _return['drainage_geometry']
                bmp_data.append(single_bmp)
                # print('BMP had no reduction calculated')
                # print(_return['id'])
                # print(_return['practice_type']['name'])
                # print(_return)
            c += 1

        parsed_data = []
        for item in bmp_data:
            try:
                if item['geom']['type'] == 'Polygon':
                    parsed_data.append([item['practice_name'], item['practice_id'], item['program_name'],
                                item['program_id'], item['organization'], item['description'],
                                item['practice_type'], item['created_at'], item['modified_at'],
                                item['tn'], item['tp'], item['tss'],
                                Polygon(item['geom']['coordinates'][0]),
                                        Polygon(item['drainage_geom']['coordinates'][0])])
                elif item['geom']['type'] == 'MultiPolygon':
                    parsed_data.append([item['practice_name'], item['practice_id'], item['program_name'],
                                item['program_id'], item['organization'], item['description'],
                                item['practice_type'], item['created_at'], item['modified_at'],
                                item['tn'], item['tp'], item['tss'],
                                MultiPolygon(item['geom']['coordinates'][0]),
                                        Polygon(item['drainage_geom']['coordinates'][0])])
                elif item['geom']['type'] == 'Point':
                    parsed_data.append([item['practice_name'], item['practice_id'], item['program_name'],
                                        item['program_id'], item['organization'], item['description'],
                                        item['practice_type'], item['created_at'], item['modified_at'],
                                        item['tn'], item['tp'], item['tss'],
                                        Point(item['geom']['coordinates'][0]),
                                        Polygon(item['drainage_geom']['coordinates'][0])])
                elif item['geom']['type'] == 'MultiPoint':
                    parsed_data.append([item['practice_name'], item['practice_id'], item['program_name'],
                                        item['program_id'], item['organization'], item['description'],
                                        item['practice_type'], item['created_at'], item['modified_at'],
                                        item['tn'], item['tp'], item['tss'],
                                        MultiPoint(item['geom']['coordinates'][0]),
                                        Polygon(item['drainage_geom']['coordinates'][0])])
                elif item['geom']['type'] == 'LineString':
                    parsed_data.append([item['practice_name'], item['practice_id'], item['program_name'],
                                        item['program_id'], item['organization'], item['description'],
                                        item['practice_type'], item['created_at'], item['modified_at'],
                                        item['tn'], item['tp'], item['tss'],
                                        LineString(item['geom']['coordinates'][0]),
                                        Polygon(item['drainage_geom']['coordinates'][0])])
                elif item['geom']['type'] == 'MultiLineString':
                    parsed_data.append([item['practice_name'], item['practice_id'], item['program_name'],
                                        item['program_id'], item['organization'], item['description'],
                                        item['practice_type'], item['created_at'], item['modified_at'],
                                        item['tn'], item['tp'], item['tss'],
                                        MultiLineString(item['geom']['coordinates'][0]),
                                        Polygon(item['drainage_geom']['coordinates'][0])])
                else:
                    print(item['practice_id'])
                    print(item['practice_name'])
                    print(item['geom']['type'])
            except:
                pass
                # print('Failed to export practice:')
                # print(item['practice_id'])
                # print(item['practice_name'])
                # print(item['geom']['type'])

        bmp_df = GeoDataFrame(data=parsed_data, columns=['practice_name', 'practice_id', 'program_name', 'program_id',
                                                  'organization', 'description', 'practice_type', 'created_at',
                                                  'modified_at', 'tn', 'tp', 'tss', 'geometry','drainage_geometry'])
        return bmp_df, bmp_data

    def get_protection_bmp_data(self):
        c = 0
        bmp_data = []
        for id in self._r_practice_ids:
            # if c == 20:
            #     break
            single_bmp = {}
            self._url = ''.join(('https://api.fielddoc.org/v1/practices/', str(id), '?access_token=', self._api_token))
            self._r_bmp = requests.get(self._url, headers=self._headers, allow_redirects=True, verify=True)
            _return = eval(str(json.loads(self._r_bmp.text)))

            try:
                single_bmp['practice_name'] = _return['name']
                single_bmp['practice_id'] = _return['id']
                single_bmp['program_name'] = _return['program']['name']
                single_bmp['program_id'] = _return['program']['id']
                single_bmp['organization'] = _return['organization']['name']
                single_bmp['description'] = _return['description']
                single_bmp['practice_type'] = _return['practice_type']['name']
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
            except:
                single_bmp['practice_name'] = _return['name']
                single_bmp['practice_id'] = _return['id']
                single_bmp['program_name'] = _return['program']['name']
                single_bmp['program_id'] = _return['program']['id']
                single_bmp['organization'] = _return['organization']['name']
                single_bmp['description'] = _return['description']
                single_bmp['practice_type'] = _return['practice_type']['name']
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
            c += 1

        parsed_data = []
        for item in bmp_data:
            try:
                if item['geom']['type'] == 'Polygon':
                    parsed_data.append([item['practice_name'], item['practice_id'], item['program_name'],
                                item['program_id'], item['organization'], item['description'],
                                item['practice_type'], item['created_at'], item['modified_at'],
                                        item['tot_pwr'],item['head_pwr'],item['ara_pwr'],item['wet_pwr'],
                                        item['str_bank'],item['nat_land'],item['dev_land'],item['ag_land'],
                                Polygon(item['geom']['coordinates'][0])])
                elif item['geom']['type'] == 'MultiPolygon':
                    parsed_data.append([item['practice_name'], item['practice_id'], item['program_name'],
                                item['program_id'], item['organization'], item['description'],
                                item['practice_type'], item['created_at'], item['modified_at'],
                                        item['tot_pwr'], item['head_pwr'], item['ara_pwr'], item['wet_pwr'],
                                        item['str_bank'], item['nat_land'], item['dev_land'], item['ag_land'],
                                MultiPolygon(item['geom']['coordinates'][0])])
                elif item['geom']['type'] == 'Point':
                    parsed_data.append([item['practice_name'], item['practice_id'], item['program_name'],
                                        item['program_id'], item['organization'], item['description'],
                                        item['practice_type'], item['created_at'], item['modified_at'],
                                        item['tot_pwr'], item['head_pwr'], item['ara_pwr'], item['wet_pwr'],
                                        item['str_bank'], item['nat_land'], item['dev_land'], item['ag_land'],
                                        Point(item['geom']['coordinates'][0])])
                elif item['geom']['type'] == 'MultiPoint':
                    parsed_data.append([item['practice_name'], item['practice_id'], item['program_name'],
                                        item['program_id'], item['organization'], item['description'],
                                        item['practice_type'], item['created_at'], item['modified_at'],
                                        item['tot_pwr'], item['head_pwr'], item['ara_pwr'], item['wet_pwr'],
                                        item['str_bank'], item['nat_land'], item['dev_land'], item['ag_land'],
                                        MultiPoint(item['geom']['coordinates'][0])])
                elif item['geom']['type'] == 'LineString':
                    parsed_data.append([item['practice_name'], item['practice_id'], item['program_name'],
                                        item['program_id'], item['organization'], item['description'],
                                        item['practice_type'], item['created_at'], item['modified_at'],
                                        item['tot_pwr'], item['head_pwr'], item['ara_pwr'], item['wet_pwr'],
                                        item['str_bank'], item['nat_land'], item['dev_land'], item['ag_land'],
                                        LineString(item['geom']['coordinates'][0])])
                elif item['geom']['type'] == 'MultiLineString':
                    parsed_data.append([item['practice_name'], item['practice_id'], item['program_name'],
                                        item['program_id'], item['organization'], item['description'],
                                        item['practice_type'], item['created_at'], item['modified_at'],
                                        item['tot_pwr'], item['head_pwr'], item['ara_pwr'], item['wet_pwr'],
                                        item['str_bank'], item['nat_land'], item['dev_land'], item['ag_land'],
                                        MultiLineString(item['geom']['coordinates'][0])])
                else:
                    print(item['practice_id'])
                    print(item['practice_name'])
                    print(item['geom']['type'])
            except:
                pass
                # print('Failed to export practice:')
                # print(item['practice_id'])
                # print(item['practice_name'])
                # print(item['geom']['type'])

        bmp_df = GeoDataFrame(data=parsed_data,
                              columns=['practice_name', 'practice_id', 'program_name', 'program_id',
                                       'organization', 'description', 'practice_type', 'created_at',
                                       'modified_at', 'tot_pwr', 'head_pwr', 'ara_pwr', 'wet_pwr', 'str_bank',
                                       'nat_land', 'dev_land', 'ag_land', 'geometry'])
        return bmp_df, bmp_data

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
                print(bmp['geom']['type'])
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
        port=PG_CONFIG['port'])

    fielddoc = drwiBmps(_config_file=config_file, _rest_prot=proj_type)
    print('Getting Practice IDs')
    practices = fielddoc.get_practice_ids()
    print(len(practices))

    if proj_type == 'protection':
        print('Getting Protection Information')
        bmps_df, bmps_dict = fielddoc.get_protection_bmp_data()
        print('Importing protection BMPs into database')
        fielddoc.import_protection_bmps(PG_Connection, bmps_dict)
    else:
        print('Getting Load Reduction Information')
        bmps_df, bmps_dict = fielddoc.get_restoration_bmp_data()
        print('Importing restoration BMPs into database')
        fielddoc.import_restoration_bmps(PG_Connection, bmps_dict)

    print(len(bmps_dict))