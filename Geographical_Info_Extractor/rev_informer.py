"""
@COMPANY WHALE
@AUTHOR ChenZhou
@DESC This is the realization of Reverse Geographical Coder using AMap API.
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


class RevGeoInformer:
    # for selected <poitype> please refer to <coderef/amap_poicode.xlsx>, seperated by <'|'> 
    def __init__(self, url, key, callback='', signtr='', output='JSON', batch = 'false',\
        batch_size = 20, max_retry = 10, poitype = '', radius = 1000, extensions = 'all', roadlevel = 0, homeorcorp = 1, \
        field=['formatted_address', 'addressComponent', 'roads', 'roadinters', 'pois', 'aois']):
        
        self.api_url = url
        self.param_dic = {   
            "sig": signtr,
            "callback": callback,
            "output": output,
            "key": key,
            "batch": batch,
            "poitype": poitype,
            "radius": radius,
            "extensions": extensions,
            "roadlevel": roadlevel,
            "homeorcorp": homeorcorp
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
            -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-\n\
            -*-*-*-*- Rev Geo Informer Created! -*-*-*-*-\n\
            -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-")

    def initialTest(self):

        self.param_dic["location"] = "120.092620,30.340308"

        r = requests.get(self.api_url, params = self.param_dic)

        if r.status_code == 200:
            logging.info("============ API Test Success! ==========")
            logging.info(r.text)
            self.status = True
        else:
            logging.warning("***** API Test Failed! Please Try Again! *****")
            self.status = False

    def poiFilter(self, file_path = 'coderef/amap_poicode.xlsx', filter_list = ['餐饮服务',\
                                                                            '购物服务',\
                                                                            #'生活服务',\
                                                                            #'体育休闲服务',\
                                                                            #'医疗保健服务',\
                                                                            '风景名胜',\
                                                                            '商务住宅',\
                                                                            #'政府机构及社会团体',\
                                                                            '科教文化服务',\
                                                                            '交通设施服务'\
                                                                            #'金融保险服务',\
                                                                            #'公司企业'\
                                                                            ]):

        data = pd.read_excel(file_path, sheet_name = 'POI分类与编码（中英文）', \
                            index_col = 0, header = 0, converters={'NEW_TYPE':str})

        data_filted = data.loc[data["大类"].isin(filter_list),:]
        self.param_dic['poitype'] = '|'.join(data_filted.loc[:,'NEW_TYPE'].apply(str))
        logging.info("=======> poitype Selected ########\n " + self.param_dic['poitype'] + "\n ########## <======== ")


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

    def formatParser(self, format_list, target_dics, dist_criteria = 100):
        
        if len(format_list) == 1:
            if format_list[0] == "string":
                if type(target_dics) == str:
                    return target_dics
                else:
                    return ""
            elif format_list[0] in ["names","distances","types","areas"]:
                name_cmb = []
                for i in target_dics:
                    try:
                        name_cmb.append(i[format_list[0][:-1]])
                    except:
                        name_cmb.append("")
                return "|".join(name_cmb)
            elif format_list[0] == "count":
                if len(target_dics) == 1 and len(target_dics[0]) == 0:
                    return 0
                else:
                    return len(target_dics)
            elif format_list[0] == "typesweights":
                type_dic = {}
                for i in target_dics:
                    try:
                        if i["type"].split(";")[0] in type_dic.keys():
                            type_dic[i["type"].split(";")[0]] += float(i["poiweight"])
                        else:
                            type_dic[i["type"].split(";")[0]] = float(i["poiweight"])
                    except:
                        continue
                return type_dic
            elif format_list[0] == "distcount":
                count = 0
                for i in target_dics:
                    try:
                        if float(i["distance"]) <= dist_criteria:
                            count += 1
                    except:
                        continue

                return count
            else:
                return None
        else:
            format_dic = {}
            for fmt in format_list:
                format_dic[fmt] = self.formatParser([fmt], target_dics)
            return format_dic


    def genFeatureDic(self, dic, feature = {"addressComponent":{"towncode":["string"],"township":["string"],"businessAreas":["names","count"]},\
                                            "aois":["count","distances","areas","types", "names"],\
                                            "pois":["typesweights"],\
                                            "roads":["distcount"],\
                                            "roadinters":["distcount"]}):

        feature_dic = {}
        for f in feature.keys():
            if f in dic:
                if f == "addressComponent":
                    for ff in feature[f].keys():
                        temp = self.formatParser(feature[f][ff], dic[f][ff])
                        if type(temp) == dict:
                            for stat_flag in temp.keys():
                                feature_dic[ff+'_'+stat_flag] = temp[stat_flag]
                        else:
                            feature_dic[ff] = temp
                elif f in ["aois","pois","roads","roadinters"]:
                    temp = self.formatParser(feature[f], dic[f])
                    if type(temp) == dict:
                        for stat_flag in temp.keys():
                            feature_dic[f+'_'+stat_flag] = temp[stat_flag]
                    else:
                        feature_dic[f] = temp
            else:
                continue

        return feature_dic

    def jsonParser(self, json_str): # for now default batch = False
        if json_str == "":
            logging.warning("***** None Parsed! *****")
            return -1
        try:
            json_struct = json.loads(json_str)
            try:
                if json_struct["status"] == "1":

                    potential = json_struct["regeocode"]
                    potential = self.buildFieldDic(potential)
                    potential = self.genFeatureDic(potential)
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
        logging.info("============> Start Rev Geo Informer! ===========>")

        for loc_idx in range(len(loc_list)):
            self.param_dic["location"] = loc_list[loc_idx]

            result = self.singleRequest()
            if self.jsonParser(result) > 0:
                self.result.extend(self.tunnel)
            else:
                logging.error("***** Parsing Location Num " + str(loc_idx) + " Error!")
                self.result.extend([{}])
            self.tunnel = []

            logging.info(loc_idx)

        logging.info(self.result)
        logging.info("<============ Rev Geo Informing Finished <===========")
       
        return 0

def assignTopic(typecode, distance, area, df):
    if int(typecode[:2]) <= 4:
        df['车辆服务'] += 1/math.log(float(distance) + math.e)*max(math.log(float(area)), math.log(1000))
    elif int(typecode[:2]) == 5:
        df['餐饮服务'] += 1/math.log(float(distance) + math.e)*max(math.log(float(area)), math.log(1000))
    elif int(typecode[:2]) == 6:
        df['购物综合'] += 1/math.log(float(distance) + math.e)*max(math.log(float(area)), math.log(1000))
    elif int(typecode[:2]) == 7:
        df['生活服务'] += 1/math.log(float(distance) + math.e)*max(math.log(float(area)), math.log(1000))
    elif int(typecode[:4]) in [800,801,802,804]:
        df['体育服务'] += 1/math.log(float(distance) + math.e)*max(math.log(float(area)), math.log(1000))
    elif int(typecode[:4]) in [803,805,806]:
        df['娱乐服务'] += 1/math.log(float(distance) + math.e)*max(math.log(float(area)), math.log(1000))
    elif int(typecode[:2]) == 9:
        df['医疗服务'] += 1/math.log(float(distance) + math.e)*max(math.log(float(area)), math.log(1000))
    elif int(typecode[:2]) == 10:
        df['住宿服务'] += 1/math.log(float(distance) + math.e)*max(math.log(float(area)), math.log(1000))
    elif int(typecode[:2]) == 11:
        df['风景区'] += 1/math.log(float(distance) + math.e)*max(math.log(float(area)), math.log(1000))
    elif int(typecode[:2]) == 12:
        df['商住区'] += 1/math.log(float(distance) + math.e)*max(math.log(float(area)), math.log(1000))
    elif int(typecode[:2]) == 13:
        df['政府机构社会团体'] += 1/math.log(float(distance) + math.e)*max(math.log(float(area)), math.log(1000))
    elif int(typecode[:6]) in [141201, 141206]:
        df['教育院校'] += 1/math.log(float(distance) + math.e)*max(math.log(float(area)), math.log(1000))
    elif int(typecode[:2]) == 14 and int(typecode[:6]) not in [141201, 141206]:
        df['科教文化'] += 1/math.log(float(distance) + math.e)*max(math.log(float(area)), math.log(1000))
    elif int(typecode[:4]) in [1500, 1501, 1502, 1503, 1803]:
        df['交通枢纽'] += 1/math.log(float(distance) + math.e)*max(math.log(float(area)), math.log(1000))
    elif int(typecode[:4]) in [1505, 1506, 1507, 1508, 1509, 1510, 1511, 1512]:
        df['公共交通'] += 1/math.log(float(distance) + math.e)*max(math.log(float(area)), math.log(1000))
    elif int(typecode[:2]) == 16:
        df['金融保险'] += 1/math.log(float(distance) + math.e)*max(math.log(float(area)), math.log(1000))
    elif int(typecode[:2]) == 17:
        df['公司企业'] += 1/math.log(float(distance) + math.e)*max(math.log(float(area)), math.log(1000))
    return df


if __name__ == "__main__":
    api_url = "https://restapi.amap.com/v3/geocode/regeo"
    api_key =  "Classified. You can obtain one on AMap Website as an Enterprise."
    extractor = RevGeoInformer(api_url, api_key)
    
    extractor.initialTest()
    extractor.poiFilter()

    '''
    loc_list = ["120.092620,30.340308", "120.192620,30.240308"]
    extractor.infoExtract(loc_list)
    print(extractor.result)
    '''

    ori_filename = "data/geo_code_repl_cut_9"
    colnames = ['门店名称','地址','合并地址','id','name','type','typecode','address','location','pcode','pname','citycode','cityname','adcode','adname','business_area','timestamp','rating','cost']
    colnames += ['车辆服务', '餐饮服务', '生活服务', '体育服务', '娱乐服务', '医疗服务', '住宿服务', '商住区', '风景区', '教育院校', '交通枢纽', '公共交通', '购物专卖', '购物综合', '政府机构社会团体', '科教文化', '金融保险', '公司企业']
    loc_df = fr.readAddressFile(ori_filename+".xlsx", colnames)

    loc_list = list(loc_df['location'])

    extractor.infoExtract(loc_list)

    field = ['aois_count','aois_distances','aois_areas','aois_types','aois_names']
    new_field = fr.genStructuredData(extractor.result, field)
    for f in field:
        loc_df[f] = list(new_field[f])
    new_filename = ori_filename + "_aoi.xlsx"
    fr.writeAddressFile(loc_df, new_filename)
    

    ori_filename = new_filename
    colnames = ['门店名称','地址','合并地址','id','name','type','typecode','address','location','pcode','pname','citycode','cityname','adcode','adname','business_area','timestamp','rating','cost']
    colnames += ['车辆服务', '餐饮服务', '生活服务', '体育服务', '娱乐服务', '医疗服务', '住宿服务', '商住区', '风景区', '教育院校', '交通枢纽', '公共交通', '购物专卖', '购物综合', '政府机构社会团体', '科教文化', '金融保险', '公司企业']
    colnames += ['aois_count','aois_distances','aois_areas','aois_types','aois_names']
    loc_df = fr.readAddressFile(ori_filename, colnames)

    loc_df['餐饮业'] = loc_df['餐饮服务'].apply(lambda x:float(x[1:-1].split(',')[0]))
    loc_df['餐饮均价'] = loc_df['餐饮服务'].apply(lambda x:float(x[1:-1].split(',')[1]))
    loc_df['餐饮服务'] = loc_df['餐饮业']

    loc_df['住宿服务'] = loc_df['住宿服务'].apply(lambda x:float(x[1:-1].split(',')[0]))

    loc_df['商住业'] = loc_df['商住区'].apply(lambda x:float(x[1:-1].split(',')[0]))
    loc_df['房均价'] = loc_df['商住区'].apply(lambda x:float(x[1:-1].split(',')[1]))
    loc_df['商住区'] = loc_df['商住业']

    loc_df.drop(['商住业','餐饮业'],axis=1,inplace=True)

    for i in loc_df.index:
        if int(loc_df.loc[i,'aois_count']) > 0:
            dist = loc_df.loc[i,'aois_distances'].split('|')
            area = loc_df.loc[i,'aois_areas'].split('|')
            types = loc_df.loc[i,'aois_types'].split('|')
            for k in range(int(loc_df.loc[i,'aois_count'])):
                loc_df.loc[i,:] = assignTopic(types[k], dist[k], area[k], loc_df.loc[i,:])


    new_filename = ori_filename
    fr.writeAddressFile(loc_df, new_filename)

