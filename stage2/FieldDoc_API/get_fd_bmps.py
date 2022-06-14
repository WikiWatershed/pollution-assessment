import requests
import json
import sys
import pandas as pd
from geopandas import GeoDataFrame
from shapely.geometry import Polygon


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

    def get_bmp_data(self):
        bmp_data = []
        for id in self._r_practice_ids:
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
                single_bmp['tn'] = round(_return['metrics']['features'][0]['current_value'], 2)
                single_bmp['tp'] = round(_return['metrics']['features'][1]['current_value'], 2)
                single_bmp['tss'] = round(_return['metrics']['features'][2]['current_value'], 2)
                single_bmp['geom'] = _return['geometry']

                bmp_data.append(single_bmp)
            except:
                pass
                # print('BMP had no reduction calculated')
                # print(_return['id'])
                # print(_return['practice_type']['name'])
                # print(_return)

        parsed_data = [[item['practice_name'], item['practice_id'], item['program_name'],
                        item['program_id'], item['organization'], item['description'],
                        item['practice_type'], item['created_at'], item['modified_at'],
                        item['tn'], item['tp'], item['tss'],
                        Polygon(item['geom']['coordinates'][0])] for item in bmp_data]

        bmp_df = pd.DataFrame(data=parsed_data, columns=['practice_name', 'practice_id', 'program_name', 'program_id',
                                                  'organization', 'description', 'practice_type', 'created_at',
                                                  'modified_at', 'tn', 'tp', 'tss', 'geom'])
        return bmp_df


if __name__ == '__main__':
    try:
        proj_type = sys.argv[1]
    except:
        proj_type = 'restoration'
        print("User can specify 'protection' or 'restoration' programs to generate the different output files.")
        print('Default: {}'.format(proj_type))
    config_file = json.load(open('config.json'))
    fielddoc = drwiBmps(_config_file=config_file, _rest_prot=proj_type)
    practices = fielddoc.get_practice_ids()
    print(len(practices))
    bmps = fielddoc.get_bmp_data()
    print(bmps)
