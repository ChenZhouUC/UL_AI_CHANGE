"""
@COMPANY WHALE
@AUTHOR ChenZhou
@DESC This is the realization of Geographical Info Extractor using AMap API.
@DATE 2019/09
"""

import pandas as pd
import logging

logging.basicConfig(format='%(asctime)s-[%(levelname)s]: %(message)s',
                    level=logging.INFO)

def readAddressFile(filename, colnames, sheetname=0, index_col=None, header=0, converters={}):

    logging.info("------ Reading File Started! ------")

    try:
        data = pd.read_excel(filename, sheet_name = sheetname, index_col = index_col, header = header, converters = converters)
        print("1133413")
        logging.info("------ " + filename + " Reading Succeed! -------")
        return data.loc[:,colnames]
    except:
        logging.warning("------ " + filename + " Reading Failed! -------")
        return pd.DataFrame()
    

def genStructuredData(struct_list, field):

    pre_df = []
    for item in struct_list:
        tmp = []
        for f in field:
            if f in item.keys():
                tmp.append(item[f])
            else:
                tmp.append("")
        pre_df.append(tmp)

    df = pd.DataFrame(pre_df, columns=field)

    return df

def writeAddressFile(df, filename, index_col=False, header=True):

    logging.info("------ Writing File Started! ------")

    try:
        df.to_excel(filename, index=index_col, header=header)
        logging.info("------ " + filename + " Writing Succeed! -------")
        return 0
    except:
        logging.error("------ " + filename + " Writing Failed! -------")
        return -1



if __name__ == "__main__":

    
    ##### Bombo Result #####
    combo_df = pd.DataFrame()

    for i in range(6):
        index = str(i+1)

        ori_filename = "data/geo_code_replenish_basic_" + index + ".xlsx"
        colnames = ['id', 'name', 'type', 'typecode', 'address', 'location', 'pcode', 'pname', 'citycode', \
            'cityname', 'adcode', 'adname', 'business_area', 'timestamp', 'rating', 'cost']
        temp = readAddressFile(ori_filename, colnames,index_col=False)
        logging.info('input: '+str(len(temp)))
        combo_df = pd.concat([combo_df, temp], ignore_index=True)
        logging.info('combined: '+str(len(combo_df)))
        combo_df.drop_duplicates(inplace=True)
        logging.info('dropped: '+str(len(combo_df)))

    writeAddressFile(combo_df, "data/geo_code_replenish_basic.xlsx")
    
    
