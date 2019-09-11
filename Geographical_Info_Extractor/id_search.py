"""
@COMPANY WHALE
@AUTHOR ChenZhou
@DESC This is the realization of ID Searcher using AMap API.
@DATE 2019/09
"""

import requests
import time
import random
import math
import json
import logging
import pandas as pd
import file_reader as fr

class IDSearcher:
    # for selected <types> please refer to <coderef/amap_poicode.xlsx>, seperated by <'|'> 
    def __init__(self, url, key, callback='', signtr='', output='JSON', max_retry = 10, \
        field=['id', 'name', 'type', 'typecode', 'address', 'location', 'pcode', 'pname', 'citycode', \
        'cityname', 'adcode', 'adname', 'business_area', 'timestamp', 'biz_ext'],\
        ):
        
        self.api_url = url
        self.param_dic = {   
            "sig": signtr,
            "callback": callback,
            "output": output,
            "key": key
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
            -*-*-*-*-*-*- ID Searcher Created! -*-*-*-*-*-\n\
            -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-")

    def initialTest(self):
        self.param_dic["id"] = "B0FFFAB6J2"

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
                        self.tunnel.append(self.buildFieldDic(p))
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

    def infoExtract(self, id_list):

        retry = 0
        while(not self.status):
            self.initialTest()
            retry += 1
            time.sleep(random.random())
            if retry >= self.max_retry:
                logging.warning("***** Retry Exceeding Maximum! *****")
                return -2

        self.result = [] # initialize new storage
        logging.info("============> Start ID Searcher! ===========>")

        for id_idx in range(len(id_list)):
            self.param_dic["id"] = id_list[id_idx]

            result = self.singleRequest()
            if self.jsonParser(result) > 0:
                self.result.extend(self.tunnel)
            else:
                logging.error("***** Parsing Location Num " + str(id_idx) + " Error!")
                self.result.extend([{}])
            self.tunnel = []

        self.flattingResult()

        logging.info(self.result)
        logging.info("<============ ID Searching Finished <===========")
       
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

if __name__ == "__main__":
    api_url = "https://restapi.amap.com/v3/place/detail"
    api_key =  "Classified. You can obtain one on AMap Website as an Enterprise."
    extractor = IDSearcher(api_url, api_key)
    extractor.initialTest()

    '''

    ori_filename = "data/geo_code_basic.xlsx"
    colnames = ['门店名称','地址','合并地址','id','name','type','typecode','address','location','pcode','pname','citycode','cityname','adcode','adname','business_area','timestamp','rating','cost']
    loc_df = fr.readAddressFile(ori_filename, colnames, index_col=False)
    id_list = list(loc_df['id'])
    extractor.infoExtract(id_list)

    structured_data = fr.genStructuredData(extractor.result, field=['id', 'name', 'type', 'typecode', 'address', 'location', 'pcode', 'pname', 'citycode', \
        'cityname', 'adcode', 'adname', 'business_area', 'timestamp', 'rating', 'cost'])

    structured_data['门店名称'] = list(loc_df['门店名称'])
    structured_data['地址'] = list(loc_df['地址'])
    structured_data['合并地址'] = list(loc_df['合并地址'])

    new_filename = 'data/geo_code_basic_full.xlsx'
    fr.writeAddressFile(structured_data, new_filename)

    '''
    ori_filename = "data/geo_code_basic_full_checked.xlsx"
    colnames = ['门店名称','地址','合并地址','id', 'name', 'type', 'typecode', 'address', 'location', 'pcode', 'pname', 'citycode', \
        'cityname', 'adcode', 'adname', 'business_area', 'timestamp', 'rating', 'cost']
    loc_df = fr.readAddressFile(ori_filename, colnames, index_col=False)
    id_list = list(loc_df['id'])
    extractor.infoExtract(id_list)

    structured_data = fr.genStructuredData(extractor.result, field=['id', 'name', 'type', 'typecode', 'address', 'location', 'pcode', 'pname', 'citycode', \
        'cityname', 'adcode', 'adname', 'business_area', 'timestamp', 'rating', 'cost'])

    structured_data['门店名称'] = list(loc_df['门店名称'])
    structured_data['地址'] = list(loc_df['地址'])
    structured_data['合并地址'] = list(loc_df['合并地址'])

    new_filename = 'data/geo_code_basic_full.xlsx'
    fr.writeAddressFile(structured_data, new_filename)

