"""
@COMPANY WHALE
@AUTHOR ChenZhou
@DESC This is the realization of Admin Area using AMap API.
@DATE 2019/09
"""

import requests
import time
import random
import math
import json
import logging
import pandas as pd
import re

class AdminAreaTractor:

    def __init__(self, url, key, callback='', output='JSON', offset = 1, area_filter = '',\
        subdistrict = 0, max_retry = 10, extensions = 'all',\
        field=['name','citycode', 'adcode', 'polyline', 'center', 'level', 'districts']):
        
        self.api_url = url
        self.param_dic = {   
            "callback": callback,
            "output": output,
            "key": key,
            "offset": offset,
            "subdistrict": subdistrict,
            "extensions": extensions,
            "filter": area_filter
            }
        self.status = False
        self.max_retry = max_retry
        self.result = []
        self.tunnel = []
        self.field = field

        logging.basicConfig(format='%(asctime)s-[%(levelname)s]: %(message)s',
                    level=logging.INFO)
        logging.info("\n\
            -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-\n\
            -*-*-*-*- Admin Area Tractor Created! -*-*-*-\n\
            -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-")

    def initialTest(self):
        self.param_dic["keywords"] = "杭州"
        self.param_dic["page"] = "1"

        r = requests.get(self.api_url, params = self.param_dic)

        if r.status_code == 200:
            logging.info("============ API Test Success! ==========")
            logging.info(r.text)
            self.status = True
        else:
            logging.warning("***** API Test Failed! Please Try Again! *****")
            self.status = False

    def singleRequest(self):
        try:
            r = requests.get(self.api_url, params = self.param_dic)
        except:
            self.status = False 
        finally:
            if r.status_code == 200:
                return r.text
            else:
                self.status = False
                result = ""
                retry = 1
                time.sleep(random.random())
                while(not self.status):

                    logging.warning("***** API Retrying...... *****")

                    r = requests.get(self.api_url, params = self.param_dic)

                    if r.status_code == 200:
                        self.status = True
                        result = r.text
                        break

                    retry += 1
                    if retry >= self.max_retry:
                        logging.warning("***** Retry Exceeding Maximum! *****")
                        break 

                    time.sleep(random.random())

                return result

    def buildFieldDic(self, dic):

        field_dic = {}
        for f in self.field:
            if f != 'districts':
                if f in dic.keys():
                    field_dic[f] = dic[f]
                else:
                    field_dic[f] = ''
            else:
                if f in dic.keys():
                    field_dic[f] = dic[f]
                else:
                    field_dic[f] = {}

        return field_dic

    def jsonParser(self, json_str):
        if json_str == "":
            logging.warning("***** None Parsed! *****")
            return -1
        try:
            json_struct = json.loads(json_str)
            try:
                if json_struct["status"] == "1":

                    potential = json_struct["districts"]
                    for p in potential:
                        pp = self.buildFieldDic(p)
                        self.tunnel.extend([pp])
                    return len(potential)

                else:
                    logging.error(json_struct["infocode"] + json_struct["info"])
                    return -4
            except:
                logging.error("***** Request Error! *****")
                return -3
        except:
            logging.error("***** Parsing Error! *****")
            return -2

    def infoExtract(self, loc_list):

        retry = 0
        while(not self.status):
            self.initialTest()
            retry += 1
            time.sleep(random.random())
            if retry >= self.max_retry:
                logging.warning("***** Retry Exceeding Maximum! *****")
                return -2

        self.result = [] # initialize new storage
        logging.info("============> Start Admin Area Tractor! ===========>")

        for loc_idx in range(len(loc_list)):
            self.param_dic["keywords"] = loc_list[loc_idx]
            self.param_dic["page"] = "1"

            result = self.singleRequest()
            if self.jsonParser(result) > 0:
                self.result.extend(self.tunnel)
            else:
                logging.error("***** Parsing Location Num " + str(loc_idx) + " Error!")
                self.result.extend([{}])
            self.tunnel = []

        logging.info(self.result)
        logging.info("<============ Admin Area Tractor Finished <===========")
       
        return 0

    def rectangleArea(self, loc_string):
        lonlat_list = re.split('[|;]',loc_string)

        min_lon = 112
        max_lon = 112

        min_lat = 35
        max_lat = 35

        for ll in lonlat_list:
            tmp = ll.split(',')
            lon = float(tmp[0])
            lat = float(tmp[1])
            
            min_lon = min(lon,min_lon)
            max_lon = max(lon,max_lon)

            min_lat = min(lat,min_lat)
            max_lat = max(lat,max_lat)

        return [(min_lon,min_lat),(max_lon,max_lat)]



if __name__ == "__main__":
    api_url = "https://restapi.amap.com/v3/config/district"
    api_key =  "Classified. You can obtain one on AMap Website as an Enterprise."
    extractor = AdminAreaTractor(api_url, api_key)
    
    extractor.initialTest()
    loc_list = ['洛阳']
    extractor.infoExtract(loc_list)

    print(extractor.rectangleArea(extractor.result[0]['polyline']))
    # [(111.138444, 33.570533), (112.984738, 35.070236)]