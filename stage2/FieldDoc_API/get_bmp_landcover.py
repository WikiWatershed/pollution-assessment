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
class getBmpLandcover:
    def __init__(self, _config_file):

        self._api_token = _config_file['fd_api_key']

        # Set up request
        self._url_fzs_nlcd = 'https://watersheds.cci.drexel.edu/api/fzs/'
        self._url_fzs_buildout = 'http://watersheds.cci.drexel.edu/api/fzs_buildout/'
        self._headers = {'Content-Type': 'application/json'}

        self._bmps = None
        self._r_practice_ids = None

    def get_bmps(self, _PG_Connection, _bmps_dict):
        cur = _PG_Connection.cursor()
        self._bmps = cur.execute(
            "SELECT practice_id, st_asGeoJSON(geom) as geom from datapolassess.fd_api_protection  ;"
        )
        print(self._bmps)
        _PG_Connection.commit()
        cur.close()

    def get_nlcd_landcover(self):
        self._r_practice_ids = []
        for page in self._pages:
            self._url = ''.join(('https://api.fielddoc.org/v1/practices?program=', self._programs, '&limit=100&page='))
            self._url = ''.join((self._url, str(page), '&access_token=', self._api_token))
            _r_practices = requests.get(self._url, headers=self._headers, allow_redirects=True, verify=True)
            _return = eval(str(json.loads(_r_practices.text)))
            for practice in _return['data']:
                self._r_practice_ids.append(practice['id'])
        return self._r_practice_ids

#%%
if __name__ == '__main__':

    config_file = json.load(open('config.json'))
    PG_CONFIG = config_file['PGtest']
    PG_Connection = psycopg2.connect(
        host=PG_CONFIG['host'],
        database=PG_CONFIG['database'],
        user=PG_CONFIG['user'],
        password=PG_CONFIG['password'],
        port=PG_CONFIG['port']
    )

    bmp_class = getBmpLandcover(_config_file=config_file)
    bmp_class.get_bmps()



# %%
