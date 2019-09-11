"""
@COMPANY WHALE
@AUTHOR ChenZhou
@DESC This is the realization of Poly Searcher using AMap API.
@DATE 2019/09
"""

import requests
import time
import random
import math
import json
import logging
import pandas as pd

class PolySearcher:
    # for selected <types> please refer to <coderef/amap_poicode.xlsx>, seperated by <'|'> 
    def __init__(self, url, key, callback='', signtr='', output='JSON', offset = 50, keywords = '', \
        max_retry = 10, types = '', city = ['杭州市'], extensions = 'all', top = -1, \
        field=['id', 'name', 'type', 'typecode', 'address', 'location', 'pcode', 'pname', 'citycode', \
        'cityname', 'adcode', 'adname', 'business_area', 'timestamp', 'biz_ext'],\
        typefilter = []):
        
        self.api_url = url
        self.param_dic = {   
            "sig": signtr,
            "callback": callback,
            "output": output,
            "key": key,
            "offset": offset,
            "types": types,
            "extensions": extensions
            }
        self.top = top
        self.status = False
        self.max_retry = max_retry
        self.result = []
        self.tunnel = []
        self.field = field
        self.typefilter = typefilter
        self.city = city

        logging.basicConfig(format='%(asctime)s-[%(levelname)s]: %(message)s',
                    level=logging.INFO)
        logging.info("\n\
            -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-\n\
            -*-*-*-*-*- Poly Searcher Created! -*-*-*-*-\n\
            -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-")

    def initialTest(self):
        self.param_dic["polygon"] = "111.138444,35.070236|112.984738,33.570533"
        self.city = ["洛阳市"]
        self.param_dic["types"] = ""
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
            if f != 'biz_ext':
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

    def checkerFieldDic(self, dic, mapping = {'address':'formatted_address',\
                                                'pname':'province',\
                                                'citycode':'citycode',\
                                                'cityname':'city',\
                                                'adcode':'adcode',\
                                                'adname':'district',\
                                                'location':'location'}):
        new_dic = {}
        for f in self.field:
            if f in mapping.keys():
                try:
                    new_dic[f] = dic[mapping[f]]
                except:
                    pass
        return new_dic

    def jsonParser(self, json_str):
        if json_str == "":
            logging.warning("***** None Parsed! *****")
            return -1
        try:
            json_struct = json.loads(json_str)
            try:
                if json_struct["status"] == "1":

                    potential = json_struct["pois"]
                    for p in potential:
                        if p['cityname'] in self.city:
                            for code in p['typecode'].split('|'):
                                if code not in self.typefilter:
                                    pp = self.buildFieldDic(p)
                                    self.tunnel.append(pp)
                                    break
                        else:
                            continue
                    return len(self.tunnel)

                else:
                    logging.error(json_struct["infocode"] + json_struct["info"])
                    return -4
            except:
                logging.error("***** Request Error! *****")
                return -3
        except:
            logging.error("***** Parsing Error! *****")
            return -2


    def infoExtract(self, poly_list, ct_list, types, top, inspector = None, addr_list = None): # inspector can be GeoCoder

        retry = 0
        while(not self.status):
            self.initialTest()
            retry += 1
            time.sleep(random.random())
            if retry >= self.max_retry:
                logging.warning("***** Retry Exceeding Maximum! *****")
                return -2

        self.result = [] # initialize new storage
        self.top = top
        logging.info("============> Start Poly Searcher! ===========>")

        exceeding_times = 0
        exceeding_log = []

        for poly_idx in range(len(poly_list)):
            drain_flag = True
            page = 0
            poly_poi_now = [] 
            self.param_dic["polygon"] = poly_list[poly_idx]
            self.city = ct_list[poly_idx]
            self.param_dic["types"] = types
            while(drain_flag):
                page += 1
                self.param_dic["page"] = str(page)
                logging.info("---> Pulling Page: " + str(page) + " <---")

                result = self.singleRequest()

                if self.jsonParser(result) > 0:
                    if self.top > 1:
                        poly_poi_now.extend(self.tunnel[:(self.top-len(poly_poi_now))])
                    elif self.top == 1: # check the closest one
                        inspector.infoExtract([addr_list[poly_idx]], [ct_list[poly_idx]], False)
                        try:
                            checker = inspector.result[0]
                            best = self.tunnel[0]
                            min_dist = self.distLonLat(best['location'], checker['location'])
                            if min_dist <= 0.5:
                                pass
                            else:
                                for candidate in self.tunnel:
                                    tmp_dist = self.distLonLat(candidate['location'], checker['location'])
                                    print(candidate, checker, tmp_dist)
                                    if tmp_dist < min_dist:
                                        best = candidate
                                        min_dist = tmp_dist
                            if min_dist <= 2:
                                poly_poi_now.extend([best])
                            else:
                                poly_poi_now.extend([self.checkerFieldDic(checker)])
                        except:
                            poly_poi_now.extend(self.tunnel[:1])
                    else:
                        poly_poi_now.extend(self.tunnel)
                else:
                    logging.warning("***** Parsing Keywords Num " + str(poly_idx) + " Page:" + str(page) + " Failed!")
                    logging.warning("***** Drained Keywords " + poly_list[poly_idx] + " Page:" + str(page) + " !")
                    drain_flag = False

                self.tunnel = []

                if (self.top > 0 and len(poly_poi_now) >= self.top) or page > 100:
                    break
                    
            if self.top == 1 and len(poly_poi_now) < 1:
                poly_poi_now.extend([{}])

            if int(self.param_dic["offset"]) * page >= 900:
                exceeding_times += 1
                exceeding_log.append(self.param_dic["polygon"])

            self.result.extend(poly_poi_now)

        self.flattingResult()
            
        logging.info("##### Total Get Result: " + str(len(self.result)) + " ##### " + "Drain Times: " + str(exceeding_times))
        print(exceeding_log)
        try:
            logging.info(self.result[0])
        except:
            pass
        logging.info("<============ Poly Searching Finished <===========")
       
        return 0

    def flattingResult(self, flat_key={'biz_ext':['rating','cost']}):
        
        for r in self.result:
            for f in flat_key.keys():
                if f in r.keys():
                    tmp = r[f]
                    for tk in tmp.keys():
                        if tk in flat_key[f]:
                            r[tk] = tmp[tk]
                    r.pop(f)

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

class PolyGen():

    def __init__(self, polygon):
        
        self.polygon = polygon

        logging.basicConfig(format='%(asctime)s-[%(levelname)s]: %(message)s',
                    level=logging.INFO)
        logging.info("Polygon Loaded")

    def rectGen(self, expand_ratio, min_grid_len):

        points = self.polygon.split("|")
        lu = points[0].split(',')
        rd = points[1].split(',')

        lu_lon = float(lu[0])
        lu_lat = float(lu[1])

        rd_lon = float(rd[0])
        rd_lat = float(rd[1])

        num_grid_lon = math.ceil((rd_lon - lu_lon)/min_grid_len)
        num_grid_lat = math.ceil((lu_lat - rd_lat)/min_grid_len)

        poly_res = []

        for i in range(num_grid_lon):

            lu_lon_tmp = lu_lon + (i - expand_ratio) * min_grid_len
            rd_lon_tmp = lu_lon + (i + 1 + expand_ratio) * min_grid_len

            for j in range(num_grid_lat):

                lu_lat_tmp = lu_lat - (j - expand_ratio) * min_grid_len
                rd_lat_tmp = lu_lat - (j + 1 + expand_ratio) * min_grid_len

                poly_res.append(str(lu_lon_tmp)+','+str(lu_lat_tmp)+'|'+str(rd_lon_tmp)+','+str(rd_lat_tmp))

        return poly_res

if __name__ == "__main__":
    api_url = "https://restapi.amap.com/v3/place/polygon"
    api_key =  "Classified. You can obtain one on AMap Website as an Enterprise."
    extractor = PolySearcher(api_url, api_key)
    extractor.initialTest()

    poly_list = "111.138444,34.0000|112.0000,33.570533" #"111.138444,35.070236|112.984738,33.570533"
    poly = PolyGen(poly_list)
    poly_list = poly.rectGen(0.2,0.02)

    poly_list = ['111.643444,34.390236|111.65044400000001,34.383236', '111.643444,34.390236|111.65044400000001,34.383236000000004', '112.17500000000001,34.520236|112.182,34.513236', '112.175,34.520236|112.182,34.513236', '112.385,34.665236|112.39200000000001,34.658236', '112.42999999999999,34.675236000000005|112.437,34.668236', '112.42999999999999,34.675236|112.437,34.668236', '112.465,34.620236|112.47200000000001,34.613236', '112.47,34.620236|112.477,34.613236', '112.47,34.615236|112.477,34.608236', '112.47500000000001,34.620236|112.482,34.613236', '112.47500000000001,34.615236|112.482,34.608236', '112.47,34.615235999999996|112.477,34.608236', '112.47500000000001,34.615235999999996|112.482,34.608236', '112.475,34.620236|112.482,34.613236', '112.475,34.615236|112.482,34.608236']
    #['111.59444400000001,33.804|111.622444,33.776', '111.614444,33.804|111.64244400000001,33.776', '111.614444,33.784|111.64244400000001,33.756', '111.65444400000001,34.394236|111.682444,34.366236', '111.634444,34.394236|111.66244400000001,34.366236', '111.634444,34.414236|111.66244400000001,34.386236000000004', '112.056,34.134236|112.084,34.106236', '112.076,34.154236000000004|112.104,34.126236', '112.076,34.134236|112.104,34.106236', '112.116,34.754236|112.144,34.726236', '112.116,34.734236|112.144,34.706236000000004', '112.136,34.754236|112.164,34.726236', '112.136,34.734236|112.164,34.706236000000004', '112.156,34.534236|112.184,34.506236', '112.176,34.534236|112.204,34.506236', '112.336,34.694236000000004|112.364,34.666236', '112.356,34.674236|112.384,34.646236', '112.376,34.694236000000004|112.404,34.666236', '112.376,34.674236|112.404,34.646236', '112.376,34.654236000000004|112.404,34.626236', '112.396,34.694236000000004|112.424,34.666236', '112.396,34.674236|112.424,34.646236', '112.416,34.834236000000004|112.444,34.806236', '112.416,34.694236000000004|112.444,34.666236', '112.416,34.674236|112.444,34.646236', '112.416,34.654236000000004|112.444,34.626236', '112.416,34.634236|112.444,34.606236', '112.416,34.454236|112.444,34.426236', '112.416,34.434236|112.444,34.406236', '112.436,34.834236000000004|112.464,34.806236', '112.436,34.714236|112.464,34.686236', '112.436,34.694236000000004|112.464,34.666236', '112.436,34.674236|112.464,34.646236', '112.436,34.634236|112.464,34.606236', '112.436,34.614236|112.464,34.586236', '112.456,34.714236|112.484,34.686236', '112.456,34.694236000000004|112.484,34.666236', '112.456,34.674236|112.484,34.646236', '112.456,34.654236000000004|112.484,34.626236', '112.456,34.634236|112.484,34.606236', '112.456,34.614236|112.484,34.586236', '112.456,34.174236|112.484,34.146236', '112.456,34.154236000000004|112.484,34.126236', '112.476,34.714236|112.504,34.686236', '112.476,34.694236000000004|112.504,34.666236', '112.476,34.634236|112.504,34.606236', '112.496,34.714236|112.524,34.686236', '112.496,34.694236000000004|112.524,34.666236', '112.756,34.734236|112.784,34.706236000000004', '112.776,34.734236|112.804,34.706236000000004']
    new_list = []
    for p in poly_list:
        poly = PolyGen(p)
        pp = poly.rectGen(0.2,0.002) 
        new_list.extend(pp)
    poly_list = new_list
    print(len(poly_list))
    ct_list = ["洛阳市"] * len(poly_list)
    types = "060000"
    top = -1
    
    extractor.typefilter = ['060301',\
                            '060302',\
                            '060303',\
                            '060304',\
                            '060305',\
                            '060306',\
                            '060307',\
                            '060308',\
                            '060500',\
                            '060501',\
                            '060502',\
                            '060600',\
                            '060601',\
                            '060602',\
                            '060603',\
                            '060604',\
                            '060605',\
                            '060606',\
                            '060701',\
                            '060702',\
                            '060705',\
                            '060706',\
                            '060800',\
                            '060900',\
                            '060901',\
                            '060902',\
                            '060903',\
                            '060904',\
                            '060905',\
                            '060906',\
                            '060907',\
                            '061000',\
                            '061001',\
                            '061100',\
                            '061101',\
                            '061102',\
                            '061103',\
                            '061104',\
                            '061201',\
                            '061202',\
                            '061203',\
                            '061204',\
                            '061205',\
                            '061206',\
                            '061207',\
                            '061208',\
                            '061209',\
                            '061211',\
                            '061212',\
                            '061213',\
                            '061214',\
                            '061300',\
                            '061301',\
                            '061302'
                            ]

    extractor.infoExtract(poly_list, ct_list, types, top)

    import file_reader as fr
    field=['id', 'name', 'type', 'typecode', 'address', 'location', 'pcode', 'pname', 'citycode', \
        'cityname', 'adcode', 'adname', 'business_area', 'timestamp', 'rating', 'cost']
    structured_data = fr.genStructuredData(extractor.result, field)
    new_filename = 'data/geo_code_replenish_basic.xlsx'
    fr.writeAddressFile(structured_data, new_filename)


    