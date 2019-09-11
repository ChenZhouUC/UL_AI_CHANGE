"""
@COMPANY WHALE
@AUTHOR ChenZhou
@DESC This is the realization of Geographical Info Extractor using AMap API.
@DATE 2019/09
"""

import requests
import time
import random
import math
import json
import logging

''' # Logging Formatize
logging.basicConfig(format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                    level=logging.DEBUG)
'''

class GeoInfoExtractor:
    def __init__(self, url, key, callback='', signtr='', output='JSON', \
        batch_size = 10, max_retry = 10, \
        field=['formatted_address', 'country', 'province', 'citycode', 'city', 'district', 'adcode', 'location', 'level']):
        
        self.api_url = url
        self.param_dic = {   
            "sig": signtr,
            "callback": callback,
            "output": output,
            "key": key
            }
        self.status = False
        self.max_retry = max_retry
        self.batch_size = batch_size
        self.result = []
        self.tunnel = []
        self.field = field

        logging.basicConfig(format='%(asctime)s-[%(levelname)s]: %(message)s',
                    level=logging.INFO)
        logging.info("\n\
            -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-\n\
            -*-*-*-*- Geo Info Extractor Created! -*-*-*-*-\n\
            -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-")

    def initialTest(self):

        self.param_dic["address"] = "杭州市黄龙万科中心帷幄匠心"
        self.param_dic["city"] = "杭州"
        self.param_dic["batch"] = "false"

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
            if f in dic.keys():
                field_dic[f] = dic[f]
            else:
                field_dic[f] = ''

        return field_dic

    def jsonParser(self, json_str, batch, total_num):
        if json_str == "":
            logging.warning("***** None Parsed! *****")
            return -1
        try:
            json_struct = json.loads(json_str)
            try:
                if json_struct["status"] == "1":
                    if not batch:
                        if int(json_struct["count"]) >= 2:
                            logging.warning("***** More than 1 code returned! Automatically extract 1st one. *****")
                        elif int(json_struct["count"]) <= 0:
                            logging.error("***** Less than 1 code returned! Please check the API. *****")
                            return -5

                        potential = json_struct["geocodes"][0]
                        potential = self.buildFieldDic(potential)
                        self.tunnel.append(potential)

                        return 1
                    else:
                        num = int(json_struct["count"])

                        if num != total_num:
                            logging.error("***** Expected num not correct! *****")
                            return -6
                        for n in range(num):
                            potential = json_struct["geocodes"][n]
                            potential = self.buildFieldDic(potential)
                            self.tunnel.append(potential)

                        return 1
                else:
                    logging.error(json_struct["infocode"] + json_struct["info"])
                    return -4
            except:
                logging.error("***** Request Error! *****")
                return -3
        except:
            logging.error("***** Parsing Error! *****")
            return -2


    def infoExtract(self, addr_list, city_list, batch):

        retry = 0
        while(not self.status):
            self.initialTest()
            retry += 1
            time.sleep(random.random())
            if retry >= self.max_retry:
                logging.warning("***** Retry Exceeding Maximum! *****")
                return -2

        self.result = [] # initialize new storage
        logging.info("============> Start Geo Info Extracting ===========>")

        if batch == False:

            if len(addr_list) != len(city_list):
                logging.warning("***** Please make sure that Address List shares the same length as City List. *****")
                return -1

            self.param_dic["batch"] = "false"

            for ad_idx in range(len(addr_list)):
                self.param_dic["address"] = addr_list[ad_idx]
                self.param_dic["city"] = city_list[ad_idx]

                result = self.singleRequest()
                if self.jsonParser(result, batch, 1) > 0:
                    self.result.extend(self.tunnel)
                else:
                    logging.error("***** Parsing Address Num " + str(ad_idx) + " Error!")
                    self.result.extend([self.buildFieldDic({})])

                self.tunnel = []

        else:

            if len(city_list) != 1:
                logging.warning("***** Please make sure the only City choice. *****")
                return -1

            self.param_dic["batch"] = "true"
            self.param_dic["city"] = city_list[0]

            for ad_idx in range(math.ceil(len(addr_list)/self.batch_size)):
                idx_tmp = ad_idx * self.batch_size
                tmp_list = addr_list[idx_tmp: (idx_tmp + self.batch_size)]
                addr_tmp = '|'.join(tmp_list)
                self.param_dic["address"] = addr_tmp

                result = self.singleRequest()
                if self.jsonParser(result, batch, len(tmp_list)) > 0:
                    self.result.extend(self.tunnel)
                else:
                    logging.error("***** Parsing Address Num " + str(idx_tmp) + " Error!")
                    self.result.extend([self.buildFieldDic({})]*len(tmp_list))

                self.tunnel = []

        #logging.info(self.result)
        logging.info("<============ Geo Info Extracting Finished <===========")
       
        return 0

    def distLonLat(self, lonlat1, lonlat2):

        ll1 = lonlat1.split(',')
        lon1 = float(ll1[0])/180*math.pi
        lat1 = float(ll1[1])/180*math.pi

        ll2 = lonlat2.split(',')
        lon2 = float(ll2[0])/180*math.pi
        lat2 = float(ll2[1])/180*math.pi

        R = 6371
        d = R * math.acos(math.cos(lat1)*math.cos(lat2)*math.cos(lon1-lon2)+math.sin(lat1)*math.sin(lat2))

        return d

    def poiAmend(self, addr_list, city_list, level=['道路交叉路口','兴趣点','门牌号','热点商圈','村庄','单元号','公交站台、地铁站','道路']):
        for poi_idx in range(len(self.result)):
            poi = self.result[poi_idx]
            if 'level' in poi.keys() and poi['level'] not in level:
                self.param_dic["address"] = addr_list[poi_idx]
                self.param_dic["batch"] = "false"
                self.param_dic["city"] = city_list[0]
                result = self.singleRequest()

                if self.jsonParser(result, False, 1) > 0:
                    if "level" in self.tunnel[0].keys() and self.tunnel[0]["level"] in level and self.distLonLat(self.tunnel[0]["location"], poi["location"]) <= 1:
                        self.result[poi_idx] = self.tunnel[0]
                        logging.info("=====> POI Improved! " + self.tunnel[0]["formatted_address"] + "  [ **VS** ]  " + poi["formatted_address"] + " <=====")
                    else:
                        logging.info("=====> POI Not Improved! " + self.tunnel[0]["formatted_address"] + "  [ **VS** ]  " + poi["formatted_address"] + " <=====")

                self.tunnel = []


if __name__ == "__main__":
    api_url = "https://restapi.amap.com/v3/geocode/geo"
    api_key =  "Classified. You can obtain one on AMap Website as an Enterprise."
    extractor = GeoInfoExtractor(api_url, api_key)
    
    addr_list = ["上海交通大学闵行","浙江大学玉泉"]
    city_list = ["上海","杭州"]
    extractor.infoExtract(addr_list, city_list, False)

    addr_list = ["上海交通大学闵行","上海交通大学","上海交通大学闵行校区"]
    city_list = ["上海"]
    extractor.infoExtract(addr_list, city_list, True)

    lonlat1 = "112.442034,34.626017"
    lonlat2 = "112.443554,34.612618"
    print(extractor.distLonLat(lonlat1,lonlat2))