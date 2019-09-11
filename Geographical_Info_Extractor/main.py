"""
@COMPANY WHALE
@AUTHOR ChenZhou
@DESC This is the realization of Geographical Info Extractor using AMap API.
@DATE 2019/09
"""

import geo_extractor as ge
import poi_searcher as ps
import file_reader as fr
import logging
from string import digits

logging.basicConfig(format='%(asctime)s-[%(levelname)s]: %(message)s',
                    level=logging.INFO)

def main():

    ori_filename = "data/combined.xlsx"
    colnames = ["门店名称","地址"]

    addr_df = fr.readAddressFile(ori_filename, colnames)
    addr_df["合并地址"] = addr_df.apply(lambda x: x["地址"] + x["门店名称"], axis = 1)
    #addr_df = addr_df.loc[:30,:]

    ############# Using Geo Coder API #############

    api_url = "https://restapi.amap.com/v3/geocode/geo"
    api_key =  "Classified. You can obtain one on AMap Website as an Enterprise."
    field=['formatted_address', 'country', 'province', 'citycode', 'city', 'district', 'adcode', 'location', 'level']
    geocoder = ge.GeoInfoExtractor(api_url, api_key)

    #addr_list = addr_df["合并地址"]
    #city_list = ["洛阳"]
    #extractor.infoExtract(addr_list, city_list, True)
    #extractor.poiAmend(list(addr_df["门店名称"]), city_list)

    ############# =================== #############
    
    ############# Using POI Search API #############
    
    api_url = "https://restapi.amap.com/v3/place/text"
    api_key =  "Classified. You can obtain one on AMap Website as an Enterprise."
    field=['id', 'name', 'type', 'typecode', 'address', 'location', 'pcode', 'pname', 'citycode', \
        'cityname', 'adcode', 'adname', 'business_area', 'timestamp', 'rating', 'cost']
    extractor = ps.POISearcher(api_url, api_key)
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

    remove_digits = str.maketrans('', '', digits)
    kw_list = list(addr_df["门店名称"]) #.apply(lambda x:x.translate(remove_digits))
    ct_list = ["洛阳"] * len(kw_list)
    types = "060000"
    top = 1
    extractor.infoExtract(kw_list, ct_list, types, top, geocoder, list(addr_df["合并地址"]))#地址

    ############# =================== #############


    structured_data = fr.genStructuredData(extractor.result, field)

    for f in field:
        addr_df[f] = list(structured_data[f])

    new_filename = 'data/geo_code_basic.xlsx'
    fr.writeAddressFile(addr_df, new_filename)


if __name__ == "__main__":

    main() 