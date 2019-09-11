"""
@COMPANY WHALE
@AUTHOR ChenZhou
@DESC This is the realization of Round Searcher using AMap API.
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

class RoundSearcher:
    # for selected <types> please refer to <coderef/amap_poicode.xlsx>, seperated by <'|'> 
    def __init__(self, url, key, callback='', signtr='', output='JSON', offset = 50, keywords = '', radius = '1000', \
        max_retry = 10, types = '', city = '杭州市', extensions = 'all', top = -1, sortrule = 'weight', \
        field=['id', 'name', 'type', 'typecode', 'address', 'location', 'pcode', 'pname', 'citycode', 'distance', \
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
            "extensions": extensions,
            "keywords": keywords,
            "city": city,
            "sortrule": sortrule,
            "radius": radius
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
            -*-*-*-*-*- Round Searcher Created! -*-*-*-*-\n\
            -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-")

    def initialTest(self):
        self.param_dic["location"] = "111.991750,34.487970"
        self.param_dic["city"] = "洛阳市"
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

    def infoSimplifier(self, itemlist, cost=False):
        #dic = {}
        score = 0
        pos = 0
        costing = 0
        ct = 0
        for item in itemlist:
            try:
                if len(item['id']) > 0: 
                    score += 1/(math.log(pos + math.e - 1)*math.log(float(item['distance']) + math.e))
                    pos += 1
                    #dic[item['id']] = [item['name'],item['distance']]
                    if cost:
                        try:
                            costing += float(item['biz_ext']['cost'])
                            ct += 1
                        except:
                            pass
            except:
                continue
        if cost:
            if ct > 0:
                return (score, costing/ct)
            else:
                return (score, 0)
        return score


    def infoExtract(self, loc_list, ct_list, types, top, cost=False, inspector = None, addr_list = None, simplify = False): # inspector can be GeoCoder

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
        logging.info("============> Start Round Searcher! ===========>")

        exceeding_times = 0
        exceeding_log = []

        for loc_idx in range(len(loc_list)):
            drain_flag = True
            page = 0
            loc_poi_now = []

            self.param_dic["location"] = loc_list[loc_idx]
            self.param_dic["city"] = ct_list[loc_idx]
            self.param_dic["types"] = types

            while(drain_flag):
                page += 1
                self.param_dic["page"] = str(page)
                logging.info("---> Pulling Page: " + str(page) + " <---")

                result = self.singleRequest()

                if self.jsonParser(result) > 0:
                    if self.top > 1:
                        loc_poi_now.extend(self.tunnel[:(self.top-len(loc_poi_now))])
                    elif self.top == 1: # check the closest one
                        inspector.infoExtract([addr_list[loc_idx]], [ct_list[loc_idx]], False)
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
                                loc_poi_now.extend([best])
                            else:
                                loc_poi_now.extend([self.checkerFieldDic(checker)])
                        except:
                            loc_poi_now.extend(self.tunnel[:1])
                    else:
                        loc_poi_now.extend(self.tunnel)
                else:
                    logging.warning("***** Parsing Keywords Num " + str(loc_idx) + " Page:" + str(page) + " Failed!")
                    logging.warning("***** Drained Keywords " + str(loc_list[loc_idx]) + " Page:" + str(page) + " !")
                    drain_flag = False

                self.tunnel = []

                if (self.top > 0 and len(loc_poi_now) >= self.top) or page > 100:
                    break
                    
            if self.top == 1 and len(loc_poi_now) < 1:
                loc_poi_now.extend([{}])

            if int(self.param_dic["offset"]) * page >= 900:
                exceeding_times += 1
                exceeding_log.append(self.param_dic["location"])

            if simplify:
                loc_poi_now = [self.infoSimplifier(loc_poi_now, cost)]

            self.result.extend(loc_poi_now)

        if not simplify:
            self.flattingResult()
            
        logging.info("##### Total Get Result: " + str(len(self.result)) + " ##### " + "Drain Times: " + str(exceeding_times))
        print(exceeding_log)
        try:
            logging.info(self.result[0])
        except:
            pass
        logging.info("<============ Round Searching Finished <===========")
       
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
    api_url = "https://restapi.amap.com/v3/place/around"
    api_key =  "Classified. You can obtain one on AMap Website as an Enterprise."
    extractor = RoundSearcher(api_url, api_key, radius = "500")
    extractor.initialTest()

    '''
    ############# Test ##############
    poly_list = ["112.431869,34.665329"]
    ct_list = ["洛阳市"] * len(poly_list)
    types = '010000|020000|030000|040000'
    extractor.typefilter = []
    top = -1
    extractor.infoExtract(poly_list, ct_list, types, top, simplify = True)
    #################################
    '''

    '''
    ############# 车辆服务 ##############
    ori_filename = "data/geo_code_basic_full.xlsx"
    colnames = ['门店名称','地址','合并地址','id','name','type','typecode','address','location','pcode','pname','citycode','cityname','adcode','adname','business_area','timestamp','rating','cost']
    loc_df = fr.readAddressFile(ori_filename, colnames)

    poly_list = list(loc_df['location'])
    ct_list = ["洛阳市"] * len(poly_list)
    field = '车辆服务'
    types = '010000|020000|030000|040000'
    extractor.typefilter = []
    top = -1

    extractor.infoExtract(poly_list, ct_list, types, top, simplify = True)

    loc_df[field] = extractor.result
    new_filename = "data/geo_code_all.xlsx"
    fr.writeAddressFile(loc_df, new_filename)
    #################################
    
    
    ############# 餐饮服务 ##############
    ori_filename = "data/geo_code_all.xlsx"
    colnames = ['门店名称','地址','合并地址','id','name','type','typecode','address','location','pcode','pname','citycode','cityname','adcode','adname','business_area','timestamp','rating','cost']
    colnames += ['车辆服务']
    loc_df = fr.readAddressFile(ori_filename, colnames)

    poly_list = list(loc_df['location'])
    ct_list = ["洛阳市"] * len(poly_list)
    field = '餐饮服务'
    types = '050000'
    extractor.typefilter = []
    top = -1

    extractor.infoExtract(poly_list, ct_list, types, top, simplify = True, cost=True)

    loc_df[field] = extractor.result
    new_filename = "data/geo_code_all.xlsx"
    fr.writeAddressFile(loc_df, new_filename)
    #################################
    
    
    ############# 生活服务 ##############
    ori_filename = "data/geo_code_all.xlsx"
    colnames = ['门店名称','地址','合并地址','id','name','type','typecode','address','location','pcode','pname','citycode','cityname','adcode','adname','business_area','timestamp','rating','cost']
    colnames += ['车辆服务', '餐饮服务']
    loc_df = fr.readAddressFile(ori_filename, colnames)

    poly_list = list(loc_df['location'])
    ct_list = ["洛阳市"] * len(poly_list)
    field = '生活服务'
    types = '070000'
    extractor.typefilter = []
    top = -1

    extractor.infoExtract(poly_list, ct_list, types, top, simplify = True)

    loc_df[field] = extractor.result
    new_filename = "data/geo_code_all.xlsx"
    fr.writeAddressFile(loc_df, new_filename)
    #################################
    
    
    ############# 体育服务 ##############
    ori_filename = "data/geo_code_all.xlsx"
    colnames = ['门店名称','地址','合并地址','id','name','type','typecode','address','location','pcode','pname','citycode','cityname','adcode','adname','business_area','timestamp','rating','cost']
    colnames += ['车辆服务', '餐饮服务', '生活服务']
    loc_df = fr.readAddressFile(ori_filename, colnames)

    poly_list = list(loc_df['location'])
    ct_list = ["洛阳市"] * len(poly_list)
    field = '体育服务'
    types = '080100|080200|080400'
    extractor.typefilter = []
    top = -1

    extractor.infoExtract(poly_list, ct_list, types, top, simplify = True)

    loc_df[field] = extractor.result
    new_filename = "data/geo_code_all.xlsx"
    fr.writeAddressFile(loc_df, new_filename)
    #################################

    ############# 娱乐服务 ##############
    ori_filename = "data/geo_code_all.xlsx"
    colnames = ['门店名称','地址','合并地址','id','name','type','typecode','address','location','pcode','pname','citycode','cityname','adcode','adname','business_area','timestamp','rating','cost']
    colnames += ['车辆服务', '餐饮服务', '生活服务', '体育服务']
    loc_df = fr.readAddressFile(ori_filename, colnames)

    poly_list = list(loc_df['location'])
    ct_list = ["洛阳市"] * len(poly_list)
    field = '娱乐服务'
    types = '080300|080500|080600'
    extractor.typefilter = []
    top = -1

    extractor.infoExtract(poly_list, ct_list, types, top, simplify = True)

    loc_df[field] = extractor.result
    new_filename = "data/geo_code_all.xlsx"
    fr.writeAddressFile(loc_df, new_filename)
    #################################
    

    ############# 医疗服务 ##############
    ori_filename = "data/geo_code_all.xlsx"
    colnames = ['门店名称','地址','合并地址','id','name','type','typecode','address','location','pcode','pname','citycode','cityname','adcode','adname','business_area','timestamp','rating','cost']
    colnames += ['车辆服务', '餐饮服务', '生活服务', '体育服务', '娱乐服务']
    loc_df = fr.readAddressFile(ori_filename, colnames)

    poly_list = list(loc_df['location'])
    ct_list = ["洛阳市"] * len(poly_list)
    field = '医疗服务'
    types = '090100|090200|090300|090400|090500'
    extractor.typefilter = []
    top = -1

    extractor.infoExtract(poly_list, ct_list, types, top, simplify = True)

    loc_df[field] = extractor.result
    new_filename = "data/geo_code_all.xlsx"
    fr.writeAddressFile(loc_df, new_filename)
    #################################

    ############# 住宿服务 ##############
    ori_filename = "data/geo_code_all.xlsx"
    colnames = ['门店名称','地址','合并地址','id','name','type','typecode','address','location','pcode','pname','citycode','cityname','adcode','adname','business_area','timestamp','rating','cost']
    colnames += ['车辆服务', '餐饮服务', '生活服务', '体育服务', '娱乐服务', '医疗服务']
    loc_df = fr.readAddressFile(ori_filename, colnames)

    poly_list = list(loc_df['location'])
    ct_list = ["洛阳市"] * len(poly_list)
    field = '住宿服务'
    types = '100000'
    extractor.typefilter = []
    top = -1

    extractor.infoExtract(poly_list, ct_list, types, top, simplify = True, cost=True)

    loc_df[field] = extractor.result
    new_filename = "data/geo_code_all.xlsx"
    fr.writeAddressFile(loc_df, new_filename)
    #################################

    ############# 商住区 ##############
    ori_filename = "data/geo_code_all.xlsx"
    colnames = ['门店名称','地址','合并地址','id','name','type','typecode','address','location','pcode','pname','citycode','cityname','adcode','adname','business_area','timestamp','rating','cost']
    colnames += ['车辆服务', '餐饮服务', '生活服务', '体育服务', '娱乐服务', '医疗服务', '住宿服务']
    loc_df = fr.readAddressFile(ori_filename, colnames)

    poly_list = list(loc_df['location'])
    ct_list = ["洛阳市"] * len(poly_list)
    field = '商住区'
    types = '120000'
    extractor.typefilter = []
    top = -1

    extractor.infoExtract(poly_list, ct_list, types, top, simplify = True, cost=True)

    loc_df[field] = extractor.result
    new_filename = "data/geo_code_all.xlsx"
    fr.writeAddressFile(loc_df, new_filename)
    #################################

    ############# 风景区 ##############
    ori_filename = "data/geo_code_all.xlsx"
    colnames = ['门店名称','地址','合并地址','id','name','type','typecode','address','location','pcode','pname','citycode','cityname','adcode','adname','business_area','timestamp','rating','cost']
    colnames += ['车辆服务', '餐饮服务', '生活服务', '体育服务', '娱乐服务', '医疗服务', '住宿服务', '商住区']
    loc_df = fr.readAddressFile(ori_filename, colnames)

    poly_list = list(loc_df['location'])
    ct_list = ["洛阳市"] * len(poly_list)
    field = '风景区'
    types = '110000'
    extractor.typefilter = []
    top = -1

    extractor.infoExtract(poly_list, ct_list, types, top, simplify = True)

    loc_df[field] = extractor.result
    new_filename = "data/geo_code_all.xlsx"
    fr.writeAddressFile(loc_df, new_filename)
    #################################


    ############# 教育院校 ##############
    ori_filename = "data/geo_code_all.xlsx"
    colnames = ['门店名称','地址','合并地址','id','name','type','typecode','address','location','pcode','pname','citycode','cityname','adcode','adname','business_area','timestamp','rating','cost']
    colnames += ['车辆服务', '餐饮服务', '生活服务', '体育服务', '娱乐服务', '医疗服务', '住宿服务', '商住区', '风景区']
    loc_df = fr.readAddressFile(ori_filename, colnames)

    poly_list = list(loc_df['location'])
    ct_list = ["洛阳市"] * len(poly_list)
    field = '教育院校'
    types = '141201|141206'
    extractor.typefilter = []
    top = -1

    extractor.infoExtract(poly_list, ct_list, types, top, simplify = True)

    loc_df[field] = extractor.result
    new_filename = "data/geo_code_all.xlsx"
    fr.writeAddressFile(loc_df, new_filename)
    #################################
    

    
    ############# 交通枢纽 ##############
    ori_filename = "data/geo_code_all.xlsx"
    colnames = ['门店名称','地址','合并地址','id','name','type','typecode','address','location','pcode','pname','citycode','cityname','adcode','adname','business_area','timestamp','rating','cost']
    colnames += ['车辆服务', '餐饮服务', '生活服务', '体育服务', '娱乐服务', '医疗服务', '住宿服务', '商住区', '风景区', '教育院校']
    loc_df = fr.readAddressFile(ori_filename, colnames)

    poly_list = list(loc_df['location'])
    ct_list = ["洛阳市"] * len(poly_list)
    field = '交通枢纽'
    types = '150100|150200|150300|180300'
    extractor.typefilter = []
    top = -1

    extractor.infoExtract(poly_list, ct_list, types, top, simplify = True)

    loc_df[field] = extractor.result
    new_filename = "data/geo_code_all.xlsx"
    fr.writeAddressFile(loc_df, new_filename)
    #################################


    ############# 公共交通 ##############
    ori_filename = "data/geo_code_all.xlsx"
    colnames = ['门店名称','地址','合并地址','id','name','type','typecode','address','location','pcode','pname','citycode','cityname','adcode','adname','business_area','timestamp','rating','cost']
    colnames += ['车辆服务', '餐饮服务', '生活服务', '体育服务', '娱乐服务', '医疗服务', '住宿服务', '商住区', '风景区', '教育院校', '交通枢纽']
    loc_df = fr.readAddressFile(ori_filename, colnames)

    poly_list = list(loc_df['location'])
    ct_list = ["洛阳市"] * len(poly_list)
    field = '公共交通'
    types = '150500|150600|150700|150800|150900|151000|151100|151200'
    extractor.typefilter = []
    top = -1

    extractor.infoExtract(poly_list, ct_list, types, top, simplify = True)

    loc_df[field] = extractor.result
    new_filename = "data/geo_code_all.xlsx"
    fr.writeAddressFile(loc_df, new_filename)
    #################################
    

    
    ############# 购物专卖 ##############
    ori_filename = "data/geo_code_all.xlsx"
    colnames = ['门店名称','地址','合并地址','id','name','type','typecode','address','location','pcode','pname','citycode','cityname','adcode','adname','business_area','timestamp','rating','cost']
    colnames += ['车辆服务', '餐饮服务', '生活服务', '体育服务', '娱乐服务', '医疗服务', '住宿服务', '商住区', '风景区', '教育院校', '交通枢纽', '公共交通']
    loc_df = fr.readAddressFile(ori_filename, colnames)

    poly_list = list(loc_df['location'])
    ct_list = ["洛阳市"] * len(poly_list)
    field = '购物专卖'
    types = '060300|060500|060600|060800|060900|061100|061200|061300'
    extractor.typefilter = []
    top = -1

    extractor.infoExtract(poly_list, ct_list, types, top, simplify = True)

    loc_df[field] = extractor.result
    new_filename = "data/geo_code_all.xlsx"
    fr.writeAddressFile(loc_df, new_filename)
    #################################
    
    
    ############# 购物综合 ##############
    ori_filename = "data/geo_code_all.xlsx"
    colnames = ['门店名称','地址','合并地址','id','name','type','typecode','address','location','pcode','pname','citycode','cityname','adcode','adname','business_area','timestamp','rating','cost']
    colnames += ['车辆服务', '餐饮服务', '生活服务', '体育服务', '娱乐服务', '医疗服务', '住宿服务', '商住区', '风景区', '教育院校', '交通枢纽', '公共交通', '购物专卖']
    loc_df = fr.readAddressFile(ori_filename, colnames)

    poly_list = list(loc_df['location'])
    ct_list = ["洛阳市"] * len(poly_list)
    field = '购物综合'
    types = '060100|060200|060400|060700|061000|061400'
    extractor.typefilter = []
    top = -1

    extractor.infoExtract(poly_list, ct_list, types, top, simplify = True)

    loc_df[field] = extractor.result
    new_filename = "data/geo_code_all.xlsx"
    fr.writeAddressFile(loc_df, new_filename)
    #################################
    
    ############# 政府机构社会团体 ##############
    ori_filename = "data/geo_code_all.xlsx"
    colnames = ['门店名称','地址','合并地址','id','name','type','typecode','address','location','pcode','pname','citycode','cityname','adcode','adname','business_area','timestamp','rating','cost']
    colnames += ['车辆服务', '餐饮服务', '生活服务', '体育服务', '娱乐服务', '医疗服务', '住宿服务', '商住区', '风景区', '教育院校', '交通枢纽', '公共交通', '购物专卖','购物综合']
    loc_df = fr.readAddressFile(ori_filename, colnames)

    poly_list = list(loc_df['location'])
    ct_list = ["洛阳市"] * len(poly_list)
    field = '政府机构社会团体'
    types = '130000'
    extractor.typefilter = []
    top = -1

    extractor.infoExtract(poly_list, ct_list, types, top, simplify = True)

    loc_df[field] = extractor.result
    new_filename = "data/geo_code_all.xlsx"
    fr.writeAddressFile(loc_df, new_filename)
    #################################
    

    ############# 科教文化 ##############
    ori_filename = "data/geo_code_all.xlsx"
    colnames = ['门店名称','地址','合并地址','id','name','type','typecode','address','location','pcode','pname','citycode','cityname','adcode','adname','business_area','timestamp','rating','cost']
    colnames += ['车辆服务', '餐饮服务', '生活服务', '体育服务', '娱乐服务', '医疗服务', '住宿服务', '商住区', '风景区', '教育院校', '交通枢纽', '公共交通', '购物专卖', '购物综合', '政府机构社会团体']
    loc_df = fr.readAddressFile(ori_filename, colnames)

    poly_list = list(loc_df['location'])
    ct_list = ["洛阳市"] * len(poly_list)
    field = '科教文化'
    types = '140000'
    extractor.typefilter = ['141201','141206']
    top = -1

    extractor.infoExtract(poly_list, ct_list, types, top, simplify = True)

    loc_df[field] = extractor.result
    new_filename = "data/geo_code_all.xlsx"
    fr.writeAddressFile(loc_df, new_filename)
    #################################

    ############# 金融保险 ##############
    ori_filename = "data/geo_code_all.xlsx"
    colnames = ['门店名称','地址','合并地址','id','name','type','typecode','address','location','pcode','pname','citycode','cityname','adcode','adname','business_area','timestamp','rating','cost']
    colnames += ['车辆服务', '餐饮服务', '生活服务', '体育服务', '娱乐服务', '医疗服务', '住宿服务', '商住区', '风景区', '教育院校', '交通枢纽', '公共交通', '购物专卖', '购物综合', '政府机构社会团体', '科教文化']
    loc_df = fr.readAddressFile(ori_filename, colnames)

    poly_list = list(loc_df['location'])
    ct_list = ["洛阳市"] * len(poly_list)
    field = '金融保险'
    types = '160000'
    extractor.typefilter = []
    top = -1

    extractor.infoExtract(poly_list, ct_list, types, top, simplify = True)

    loc_df[field] = extractor.result
    new_filename = "data/geo_code_all.xlsx"
    fr.writeAddressFile(loc_df, new_filename)
    #################################
    '''

    ############# 公司企业 ##############
    ori_filename = "data/geo_code_all.xlsx"
    colnames = ['门店名称','地址','合并地址','id','name','type','typecode','address','location','pcode','pname','citycode','cityname','adcode','adname','business_area','timestamp','rating','cost']
    colnames += ['车辆服务', '餐饮服务', '生活服务', '体育服务', '娱乐服务', '医疗服务', '住宿服务', '商住区', '风景区', '教育院校', '交通枢纽', '公共交通', '购物专卖', '购物综合', '政府机构社会团体', '科教文化', '金融保险']
    loc_df = fr.readAddressFile(ori_filename, colnames)

    poly_list = list(loc_df['location'])
    ct_list = ["洛阳市"] * len(poly_list)
    field = '公司企业'
    types = '170000'
    extractor.typefilter = []
    top = -1

    extractor.infoExtract(poly_list, ct_list, types, top, simplify = True)

    loc_df[field] = extractor.result
    new_filename = "data/geo_code_all.xlsx"
    fr.writeAddressFile(loc_df, new_filename)
    #################################