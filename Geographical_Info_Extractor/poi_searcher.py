"""
@COMPANY WHALE
@AUTHOR ChenZhou
@DESC This is the realization of POI Searcher using AMap API.
@DATE 2019/09
"""

import requests
import time
import random
import math
import json
import logging
import pandas as pd


class POISearcher:
    # for selected <types> please refer to <coderef/amap_poicode.xlsx>, seperated by <'|'> 
    def __init__(self, url, key, callback='', signtr='', output='JSON', offset = 50, top = -1, \
        children = 0, max_retry = 10, types = '', city = '杭州', extensions = 'all', citylimit = 'true',\
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
            "children": children,
            "extensions": extensions,
            "city": city,
            "citylimit": citylimit
            }
        self.top = top
        self.status = False
        self.max_retry = max_retry
        self.result = []
        self.tunnel = []
        self.field = field
        self.typefilter = typefilter

        logging.basicConfig(format='%(asctime)s-[%(levelname)s]: %(message)s',
                    level=logging.INFO)
        logging.info("\n\
            -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-\n\
            -*-*-*-*-*- POI Searcher Created! -*-*-*-*-*-\n\
            -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-")

    def initialTest(self):
        self.param_dic["keywords"] = "黄龙万科"
        self.param_dic["city"] = "杭州"
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
                        for code in p['typecode'].split('|'):
                            if code not in self.typefilter:
                                pp = self.buildFieldDic(p)
                                self.tunnel.append(pp)
                                break
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


    def infoExtract(self, kw_list, ct_list, types, top, inspector = None, addr_list = None): # inspector can be GeoCoder

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
        logging.info("============> Start POI Searcher! ===========>")

        for kw_idx in range(len(kw_list)):
            drain_flag = True
            page = 0
            kw_poi_now = [] 
            self.param_dic["keywords"] = kw_list[kw_idx]
            self.param_dic["city"] = ct_list[kw_idx]
            self.param_dic["types"] = types
            while(drain_flag):
                page += 1
                self.param_dic["page"] = str(page)
                logging.info("---> Pulling Page: " + str(page) + " <---")

                result = self.singleRequest()

                if self.jsonParser(result) > 0:
                    if self.top > 1:
                        kw_poi_now.extend(self.tunnel[:(self.top-len(kw_poi_now))])
                    elif self.top == 1: # check the closest one
                        inspector.infoExtract([addr_list[kw_idx]], [ct_list[kw_idx]], False)
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
                                kw_poi_now.extend([best])
                            else:
                                kw_poi_now.extend([self.checkerFieldDic(checker)])
                        except:
                            kw_poi_now.extend(self.tunnel[:1])
                    else:
                        kw_poi_now.extend(self.tunnel)
                else:
                    logging.warning("***** Parsing Keywords Num " + str(kw_idx) + " Page:" + str(page) + " Failed!")
                    logging.warning("***** Drained Keywords " + kw_list[kw_idx] + " Page:" + str(page) + " !")
                    drain_flag = False

                self.tunnel = []

                if (self.top > 0 and len(kw_poi_now) >= self.top) or page > 100:
                    break
                    
            if self.top == 1 and len(kw_poi_now) < 1:
                kw_poi_now.extend([{}])

            self.result.extend(kw_poi_now)

        self.flattingResult()
            
        logging.info("##### Total Get Result: " + str(len(self.result)) + " #####")
        try:
            logging.info(self.result[0])
        except:
            pass
        logging.info("<============ POI Searching Finished <===========")
       
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

if __name__ == "__main__":
    api_url = "https://restapi.amap.com/v3/place/text"
    api_key =  "Classified. You can obtain one on AMap Website as an Enterprise."
    extractor = POISearcher(api_url, api_key)
    
    extractor.initialTest()
    kw_list = [""]
    ct_list = ["洛阳"] 
    types = "061400"
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

    extractor.infoExtract(kw_list, ct_list, types, top)

    import file_reader as fr
    field=['id', 'name', 'type', 'typecode', 'address', 'location', 'pcode', 'pname', 'citycode', \
        'cityname', 'adcode', 'adname', 'business_area', 'timestamp', 'rating', 'cost']
    structured_data = fr.genStructuredData(extractor.result, field)
    new_filename = 'data/geo_code_1400_basic.xlsx'
    fr.writeAddressFile(structured_data, new_filename)