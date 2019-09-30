"""
@COMPANY WHALE
@AUTHOR JiaXin Peng, ChenZhou
@DESC This is the realization of Admin Area using AMap API.
@DATE 2019/09
"""

################### Package Part #####################

import pandas as pd
import numpy as np
import jieba, string

from math import *
import difflib

from sklearn.ensemble import GradientBoostingRegressor
import xgboost as xgb
import lightgbm as lgb

from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
from sklearn.preprocessing import LabelEncoder
from sklearn.neighbors import KNeighborsRegressor
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt  # Matlab-style plotting
import seaborn as sns

################### Presetting Part #####################

color = sns.color_palette()
#sns.set_style("darkgrid")
np.set_printoptions(precision=4, threshold=8, edgeitems=4, linewidth=75, suppress=True, nanstr="nan", infstr="inf")
pd.set_option("display.max_columns", 20)

n_folds = 5

################### Materials #####################

# 统计词频之后的map，根据key来做特征
feature_base = {
    "超市":4924,
    "商店":3695,
    "生活":709,
    "百货":770,
    "商行":477,
    "批发":926,
    "专卖":760,
    "体验":386,
    "专柜":378,
    "商贸":419,
    "化妆品":333,
    "中心":304,
    "有限公司":246,
    "生活馆":223,
    "批零":217,
    "旗舰":208,
    "精品":203,
    "综合":201,
    "量贩":190,
    "广场":337,
    "便利":1047,
    "商城":172,
    "服饰":168,
    "便民":150,
    "购物":482,
    "丹尼斯":138,
    "汇":137,
    "部":145,
    "用品":136,
    "时尚":134,
    "坊":134,
    "市场":132,
    "直销":129,
    "国际":128,
    "大全":124,
    "街店":118,
    "代理":124,
    "专营":153,
    "定制":109,
    "美妆":106,
    "连锁":103,
    "名妆":101,
    "分店":100,
    "家居":95,
    "科技":94,
    "直营":102,
    "平价":89,
    "中心":180,
    "健康":87,
    "经营":96,
    "易客":76,
    "优品":76,
    "品牌":73,
    "工厂":72,
    "经销":114,
    "日用":127,
    "大张":70,
    "零售":69
}

# 根据关键词剔除不需要的商铺
key_word = [
    "殡仪馆", "农副产品市场", "粮行", "美的", "十字绣", "电器", "孕婴", "净水器", "鞋业",
    "干果", "女装", "农资", "水果", "服饰", "装饰",
    "生鲜", "车行", "琴行", "复合肥", "电脑", "不锈钢", "瓜子", "设备", "内衣", "电视", "净水机", "鲜果", "格力", "宠物", "配送", "加工",
    "水果店", "开锁", "土杂", "服装店", "服装", "水泵", "纸行", "材料", "灯饰", "海信", "种子",
    "农贸市场", "消防器材", "土杂店", "管业", "鞋店", "衣橱",
    "地暖", "果业", "洋河", "化工", "机电", "净水", "桶装水", "电子", "冷饮", "针织", "锁具", "果园", "机", "男装", "花艺", "热水器",
    "液压", "鞭炮", "长虹", "果", "平衡车", "热水", "土特产", "首饰",
    "水暖", "眼镜", "厨卫", "鲜花", "医院", "石材", "农副产品", "花店", "彩陶", "鞋行", "s", "厨电", "调味品", "啤", "精酿", "家纺",
    "手工", "窗帘", "制衣", "床垫", "鞋", "烟花爆竹", "数码", "香蕉", "运动", "珠宝", "TCL", "电机", "豆制品", "鞋料",
    "时尚女装", "环保", "山珍", "消防", "电气", "冰淇淋", "肥料", "大众", "管道",
    "农药", "冰箱", "面店", "壁纸", "赊店", "三轮", "卫生院", "礼品",
    "起重", "沙发", "烟花", "豆芽", "粮店", "内衣店", "皮鞋", "吊顶", "电缆", "泵业", "衣折", "乐视",
    "暖通", "炒货", "空压机", "乳业", "种业", "墙布", "厨房", "箱包", "羽绒服", "创维", "冻品", "缝纫机", "制冷", "宠物店", "农家", "奇石",
    "电子秤", "鞋材", "伊利", "联想", "仪器", "地板", "晾衣架", "缝", "水产", "石膏线",
    "联通", "酷车", "卫浴", "空调", "红富士", "烟酒", "电动车", "水泥", "水电", "棺木", "油",
    "纸业", "牧业", "面条", "陶瓷", "IPHONE", "银饰", "橱具", "烟酒", "电池", "茶", "米粉", "名烟", "名酒", "保洁",
    "通讯", "辣条", "酒运达", "苹果", "手机", "配件", "贝因美", "家电", "电瓶", "修表",
    "游泳馆", "防水", "酒", "VIVO", "轴承", "螺丝", "润滑油", "文具", "体育",
    "烧烤", "农机", "农业", "富士", "海鲜", "奥克斯", "皮带", "影碟", "葡萄", "干菜",
    "鸡", "鸭", "鱼", "玩具", "漆", "太阳能", "小米", "苹果", "寿衣", "兽", "礼服", "肥", "窗", "帘",
    "荣耀", "华为", "特曲", "香料", "老字号", "数控", "机电", "山泉", "茅台", "木雕", "APPLE", "智能家居",
    "新娘", "保险箱", "洗衣机", "钻石", "一站式", "绿源", "浅井", "双汇", "采暖", "土产", "自行车",
    "童", "钓", "散热", "冷冻", "纯净水", "安防", "钢丝", "绳", "开关", "公牛", "渔", "厨具",
    "乳品", "蚕丝", "顺丰", "电动", "工具", "蔬菜", "餐具", "餐", "菜", "奶", "保健",
    "订做", "瓷", "砖", "木门", "灶", "梦之蓝", "铝合金", "拖车", "光伏", "机械", "音响",
    "喜庆", "劳保", "塑料", "五金", "管带", "凉席", "办公", "杯", "雨伞", "宾馆", "酒店",
    "雨具", "毛巾", "家具", "咖啡", "奶茶", "害", "化肥", "配件", "卫生纸", "装饰", "长城", "佛教", "陵园",
    "干货", "绿城", "软管", "方圆", "中粮", "新能源", "净化器", "工业", "测绘", "花坛", "花盆", "花生", "电线", "萌宠", "小刀", "美白", "刀具",
    "石业", "威视", "安装", "布", "维修", "仓储", "果夫", "大米", "网络", "VR", "服务社", "汽车", "板栗", "俱乐部", "婆婆", "桂花",
    "妈妈", "闺蜜", "直通车", "闺蜜", "大队", "白铁", "毛线", "轮滑", "戴尔", "松鼠", "邮政", "康复", "家装",
    "旅游", "插座", "糖", "香箔", "华山路", "湖北", "木业", "情趣", "伴侣", "钢琴", "辣椒", "地毯", "海康", "水产", "青岛",
    "顺发", "厂", "袜子", "学生", "良品", "包装", "网购", "工程", "田园", "线缆", "床上用品",
    "钟表", "家私", "瓜", "洋果行", "轮胎", "面膜", "矿山", "农", "苏泊尔", "绣", "铁", "纯水机", "鸟", "熊", "纸锦绣", "艺术", "核桃", "饰品",
    "豆腐", "橱柜", "衣", "果", "丸", "卡拉", "指定", "系列", "家禽", "电脑", "大曲", "工艺", "水族", "果果", "货架", "三轮车",
    "蒙牛", "发行", "影音", "车业", "红旗", "指纹锁", "政府", "农贸", "监控", "服务区", "玉器",
    "钢材", "二锅头", "大枣", "工艺品", "休闲", "POS", "乐器", "糖果", "整装", "电工", "帽业", "五粮液", "板材", "果行", "冰糕", "粉条", "电线",
    "调料", "麻将", "照明", "OPPO", "红牛", "门", "饲料", "书", "画", "食", "周边",
    "动漫", "海尔", "彩钢", "铝", "蔬", "婚", "孕", "婴", "五金机电", "建材", "电暖", "肉", "花业", "榨油机", "母婴", "电池", "电信", "移动"
]

# 商铺类别
key_cate = [
    "购物服务;专卖店;专营店",
    "购物服务;专卖店;专营店|生活服务;生活服务场所;生活服务场所",
    "购物服务;个人用品/化妆品店;其它个人用品店",
    "购物服务;便民商店/便利店;便民商店/便利店",
    "购物服务;便民商店/便利店;便民商店/便利店|购物服务;专卖店;专营店",
    "购物服务;便民商店/便利店;便民商店/便利店|购物服务;商场;商场",
    "购物服务;便民商店/便利店;便民商店/便利店|购物服务;超级市场;超市",
    "购物服务;便民商店/便利店;便民商店/便利店|餐饮服务;餐饮相关场所;餐饮相关",
    "购物服务;商场;商场",
    "购物服务;综合市场;果品市场",
    "购物服务;购物相关场所;购物相关场所",
    "购物服务;超级市场;屈臣氏",
    "购物服务;超级市场;超市",
    "购物服务;超级市场;华润",
    "购物服务;超级市场;上海华联",
    "购物服务;超级市场;华润|购物服务;便民商店/便利店;便民商店/便利店",
    "购物服务;超级市场;家乐福",
    "购物服务;购物相关场所;购物相关场所|生活服务;生活服务场所;生活服务场所",
    "购物服务;购物相关场所;购物相关场所|餐饮服务;餐饮相关场所;餐饮相关",
    "购物服务;购物相关场所;购物相关场所"
]

################### Tools #####################

# 过滤的主要逻辑，判断字符串里面是否有某个字段
def isFilter(x):
    for key in key_word:
        if key in str(x).upper():
            return 1
    return 0


# 计算经纬度和商铺的逻辑距离
def calcDistance(Lat_A, Lng_A, Lat_B, Lng_B, nameA, nameB):
    ra = 6378.140  # 赤道半径 (km)
    rb = 6356.755  # 极半径 (km)
    flatten = (ra - rb) / ra  # 地球扁率
    rad_lat_A = radians(Lat_A)
    rad_lng_A = radians(Lng_A)
    rad_lat_B = radians(Lat_B)
    rad_lng_B = radians(Lng_B)
    pA = atan(rb / ra * tan(rad_lat_A))
    pB = atan(rb / ra * tan(rad_lat_B))
    xx = acos(sin(pA) * sin(pB) + cos(pA) * cos(pB) * cos(rad_lng_A - rad_lng_B))
    c1 = (sin(xx) - xx) * (sin(pA) + sin(pB)) ** 2 / cos(xx / 2) ** 2
    c2 = (sin(xx) + xx) * (sin(pA) - sin(pB)) ** 2 / sin(xx / 2) ** 2
    dr = flatten / 8 * (c1 - c2)
    distance = ra * (xx + dr)
    ratio = difflib.SequenceMatcher(None, nameA, nameB).quick_ratio()
    print(ratio, distance)

    if distance + 1 / ratio < 2:
        return "同一家店"

    if ratio > 0.8:
        return "连锁店"

    return "不同"


# 根据key清洗数据，统计词频
def clean_data():
    data = pd.read_excel("../Geographical_Info_Extractor/data/geo_code_replenish_basic.xlsx")
    data["name"] = data["name"].apply(lambda x: str(x).translate(str.maketrans("", "", string.punctuation)))
    print(data.shape)
    print(data[data["type"].isin(key_cate)].shape)
    zeros = np.zeros(data.shape[0])
    data["flag"] = zeros
    data["flag"] = data["name"].apply(lambda x: isFilter(x))
    print(data[data["flag"] == 0].shape)

    # 分词之后统计高频词
    # words = []
    # for i in data[data["flag"]==0]["name"].tolist():
    #     words.extend(list(jieba.cut(i)))
    # aa = pd.DataFrame(words)
    # aa.columns = ["col"]
    # print(aa["col"].value_counts()[:100])
    # aa["col"].value_counts().plot()
    # plt.show()
    # aa["col"].value_counts().reset_index().to_csv("key.csv", index=False)


# 获取name字段里面的权重值
def getWeight(x):
    sorted_key = sorted(list(feature_base.keys()),key=lambda x:feature_base[x],reverse=True)
    for k in sorted_key:
        if x.find(k) > -1:
            return feature_base.get(k)
    return 0

################### Training Part #####################

# 数据筛选

def filter():
    poisearch = pd.read_excel("../Geographical_Info_Extractor/data/geo_code_info_all.xlsx")
    poisearch_2 = poisearch[poisearch["是否为覆盖门店"]=="N"]
    poisearch = poisearch[poisearch["是否为覆盖门店"]=="Y"]

    # clean data
    poisearch_2["name"] = poisearch_2["name"].apply(lambda x: str(x).translate(str.maketrans("", "", string.punctuation)))
    zeros = np.zeros(poisearch_2.shape[0])
    poisearch_2["flag"] = zeros
    poisearch_2["flag"] = poisearch_2["name"].apply(lambda x: isFilter(x))
    poisearch_2 = poisearch_2[poisearch_2["flag"] == 0]
    poisearch_2.drop("flag", axis=1, inplace=True)
    print(poisearch_2)

    poisearch = pd.concat([poisearch, poisearch_2])
    poisearch.to_excel("Filtered.xlsx", index=False)

# 特征工程

def feature_eng():
    poisearch = pd.read_excel("Filtered.xlsx")
    # 获取经度，纬度
    poisearch[["lon", "lat"]] = poisearch["location"].str.split(",", expand=True).astype(float)
    # 类别特征
    poisearch["type"].fillna("-1;-1;-1", inplace=True)
    poisearch["cate1"] = poisearch["type"].apply(lambda x: str(x).split(";")[0])
    poisearch["cate2"] = poisearch["type"].apply(lambda x: str(x).split(";")[1])
    poisearch["cate3"] = poisearch["type"].apply(lambda x: str(x).split(";")[2])
    # 去除特殊字符
    poisearch["name"] = poisearch["name"].apply(lambda x: str(x).translate(str.maketrans("", "", string.punctuation)))
    # 分词之后统计拿top特征
    poisearch["name_weight"] = poisearch["name"].apply(lambda x: getWeight(str(x)))
    # 无训练Y值则替换为-1
    poisearch["YTD"].fillna(-1, inplace=True)

    '''
    # 查看各种数据分布
    fig, ax = plt.subplots()
    ax.scatter(x=poisearch["cate2"], y=poisearch["YTD"])
    plt.ylabel("YTD", fontsize=13)
    plt.xlabel("cate2", fontsize=13)
    plt.show()
    poisearch["YTD"].sort_values().reset_index(drop=True).plot()
    plt.show()
    poisearch["YTD"].map(np.log1p).plot()
    plt.show()
    poisearch[["YTD"]].boxplot()
    plt.show()
    print(np.percentile(poisearch["YTD"], 99.5))
    print(poisearch[poisearch["YTD"] > np.percentile(poisearch["YTD"], 99.5)].shape)
    '''

    # 根据分布做下面的处理，清除异常数据，不好确保测试集里面是否有大数据，暂时不去除
    # poisearch = poisearch[poisearch["YTD"] < np.percentile(poisearch["YTD"], 99.5)]
    # 删除内容一样的特征
    one_value_cols_train = [col for col in poisearch.columns if poisearch[col].nunique() <= 1]
    # 删除超过90%是null的特征
    many_null_cols_train = [col for col in poisearch.columns if
                            poisearch[col].isnull().sum() / poisearch.shape[0] > 0.9]
    # 删除长尾特征
    big_top_value_cols_train = [col for col in poisearch.columns if
                                poisearch[col].value_counts(dropna=False, normalize=True).values[0] > 0.9]
    # 最终需要删除的字段
    cols_to_drop = ["YTD", "name", "门店名称", "地址", "adcode", "type", "typecode", "id", "address", "location", "cate1", "business_area", "timestamp", "cost", "是否为覆盖门店"]

    # 对类别特征做encoder操作
    for f in poisearch.columns:
        if f not in cols_to_drop:
            if poisearch[f].dtype == "object":
                lbl = LabelEncoder()
                lbl.fit(list(poisearch[f].values))
                poisearch[f+"_lb"] = lbl.transform(list(poisearch[f].values))
                poisearch[f+"_lb"] = poisearch[f+"_lb"].astype(int)
                print(f, " #### ")
            else:
                print(f, " ===> ", poisearch[f].dtype)

    use_feature = [i for i in poisearch.columns if (i not in cols_to_drop) and ("_lb" not in i)]
    corrmat = poisearch[use_feature].corr()
    plt.subplots(figsize=(12, 9))
    sns.heatmap(corrmat, vmax=0.9, square=True)
    plt.show()
    print(poisearch[use_feature].shape)
    poisearch.to_excel("Featured.xlsx", index=False)

# 特征工程

def train():
    poisearch = pd.read_excel("Featured_eng.xlsx")
    # 最终需要删除的字段
    cols_to_drop = ["YTD", "name", "门店名称", "地址", "adcode", "type", "typecode", "id", "address", "location", "cate1", "business_area", "timestamp", "cost", "是否为覆盖门店"]

    use_feature = []
    # 对特征做筛选
    for f in poisearch.columns:
        if f not in cols_to_drop:
            if f + "_lb" in poisearch.columns:
                print("XXX:",f," ===> ",poisearch[f].dtype)
            else:
                use_feature.append(f)
                print("&&&:",f," ===> ",poisearch[f].dtype)

    # 拆分训练和测试集
    train = poisearch[poisearch.YTD != -1]
    # 根据分布做log转换，消除一些gap
    y = np.log1p(train["YTD"])
    y[np.isinf(y)] = 0
    X = train.drop("YTD", axis=1)

    # 划分测试训练集
    X_train, X_test, y_train, y_test = train_test_split(X[use_feature], y, test_size=0.2, random_state=1008)

    k = 5
    knn = KNeighborsRegressor(k)
    knn.fit(X_train, y_train)
    pred2 = knn.predict(X_test)
    print("knn test mae:", mean_absolute_error(np.exp(y_test), np.exp(pred2)))
    print("knn train mae:",mean_absolute_error(np.exp(y_train), np.exp(knn.predict(X_train))))

    GBoost = GradientBoostingRegressor(n_estimators=200, learning_rate=0.05,
                                       max_depth=10, max_features="sqrt",
                                       min_samples_leaf=15, min_samples_split=10,
                                       loss="huber", random_state=5)
    GBoost.fit(X_train, y_train)
    pred3 = GBoost.predict(X_test)
    print("GBoost test mae:",mean_absolute_error(np.expm1(y_test), np.expm1(pred3)))
    print("GBoost train mae:",mean_absolute_error(np.exp(y_train), np.exp(GBoost.predict(X_train))))
    xpred1 = GBoost.predict(poisearch[use_feature])

    lgr = lgb.LGBMRegressor(objective="regression", num_leaves=64,
                            learning_rate=0.05, n_estimators=200,
                            max_bin=55, bagging_fraction=0.8,
                            bagging_freq=5, feature_fraction=0.2319,
                            feature_fraction_seed=9, bagging_seed=9,
                            min_data_in_leaf=6, min_sum_hessian_in_leaf=11)
    lgr.fit(X_train, y_train)
    pred4 = lgr.predict(X_test)

    # 特征重要性
    fold_importance_df = pd.DataFrame()
    fold_importance_df["feature"] = X_train.columns
    fold_importance_df["importance"] = lgr.feature_importances_
    print(fold_importance_df.sort_values("importance", ascending=False))
    display_importances(fold_importance_df,'LGB')

    print("lgb test mae:",mean_absolute_error(np.expm1(y_test), np.expm1(pred4)))
    print("lgb train mae:",mean_absolute_error(np.exp(y_train), np.exp(lgr.predict(X_train))))
    xpred2 = lgr.predict(poisearch[use_feature])

    lr = LinearRegression()
    lr.fit(X_train, y_train)
    pred5 = lr.predict(X_test)
    print("lr test mae:",mean_absolute_error(np.expm1(y_test), np.expm1(pred5)))
    print("lr train mae:",mean_absolute_error(np.exp(y_train), np.exp(lr.predict(X_train))))

    lgr = xgb.XGBRegressor(colsample_bytree=0.4603, gamma=0.0468,
                           learning_rate=0.05, max_depth=10,
                           min_child_weight=1.7817, n_estimators=200,
                           reg_alpha=0.4640, reg_lambda=0.8571,
                           subsample=0.5213, silent=1,
                           random_state=7, nthread=-1)
    lgr.fit(X_train, y_train)
    pred6 = lgr.predict(X_test)

    # 特征重要性
    fold_importance_df = pd.DataFrame()
    fold_importance_df["feature"] = X_train.columns
    fold_importance_df["importance"] = lgr.feature_importances_
    print(fold_importance_df.sort_values("importance", ascending=False))
    display_importances(fold_importance_df,'XGB')

    print("xgb test mae:",mean_absolute_error(np.expm1(y_test), np.expm1(pred6)))
    print("xgb train mae:",mean_absolute_error(np.exp(y_train), np.exp(lgr.predict(X_train))))
    xpred3 = lgr.predict(poisearch[use_feature])

    pred = (pred4 * 0.25 + (pred6 * 0.5 + pred3 * 0.5) * 0.75)
    print(mean_absolute_error(np.expm1(y_test), np.expm1(pred)))
    xpred = (xpred2 * 0.25 + (xpred1 * 0.5 + xpred3 * 0.5) * 0.75)
    poisearch["pred_YTD"] = np.expm1(xpred)
    poisearch["pred_YTD"] = poisearch["pred_YTD"].apply(lambda x:x if x >= 0 else 0)
    poisearch.to_excel("Final.xlsx", index=False)



# 特征加预测
def process():
    poisearch = pd.read_excel("../Geographical_Info_Extractor/data/geo_code_info_all.xlsx")
    poisearch_2 = poisearch[poisearch["是否为覆盖门店"]=="N"]
    poisearch = poisearch[poisearch["是否为覆盖门店"]=="Y"]
    # clean data
    poisearch_2["name"] = poisearch_2["name"].apply(lambda x: str(x).translate(str.maketrans("", "", string.punctuation)))
    zeros = np.zeros(poisearch_2.shape[0])
    poisearch_2["flag"] = zeros
    poisearch_2["flag"] = poisearch_2["name"].apply(lambda x: isFilter(x))

    poisearch_2 = poisearch_2[poisearch_2["flag"] == 0]
    poisearch_2.drop("flag", axis=1, inplace=True)
    print(poisearch_2)

    poisearch = pd.concat([poisearch, poisearch_2])
    poisearch.to_excel("ttt.xlsx", index=False)
    
    # 获取经度，纬度
    poisearch[["lon", "lat"]] = poisearch["location"].str.split(", ", expand=True).astype(float)
    # 类别特征
    poisearch["type"].fillna("-1;-1;-1", inplace=True)
    poisearch["cate1"] = poisearch["type"].apply(lambda x: str(x).split(";")[0])
    poisearch["cate2"] = poisearch["type"].apply(lambda x: str(x).split(";")[1])
    poisearch["cate3"] = poisearch["type"].apply(lambda x: str(x).split(";")[2])
    # 去除特殊字符
    poisearch["name"] = poisearch["name"].apply(lambda x: str(x).translate(str.maketrans("", "", string.punctuation)))
    # 分词之后统计拿top特征
    poisearch["weight"] = poisearch["name"].apply(lambda x: getWeight(str(x)))
    poisearch["YTD"].fillna(-1, inplace=True)

    # 查看各种数据分布
    fig, ax = plt.subplots()
    ax.scatter(x=poisearch["cate2"], y=poisearch["YTD"])
    plt.ylabel("YTD", fontsize=13)
    plt.xlabel("cate2", fontsize=13)
    plt.show()
    poisearch["YTD"].sort_values().reset_index(drop=True).plot()
    plt.show()
    poisearch["YTD"].map(np.log1p).plot()
    plt.show()
    poisearch[["YTD"]].boxplot()
    plt.show()

    print(np.percentile(poisearch["YTD"], 99.5))
    print(poisearch[poisearch["YTD"] > np.percentile(poisearch["YTD"], 99.5)].shape)

    # 根据分布做下面的处理，清除异常数据，不好确保测试集里面是否有大数据，暂时不去除
    # poisearch = poisearch[poisearch["YTD"] < np.percentile(poisearch["YTD"], 99.5)]

    # 删除内容一样的特征
    one_value_cols_train = [col for col in poisearch.columns if poisearch[col].nunique() <= 1]
    # 删除超过90%是null的特征
    many_null_cols_train = [col for col in poisearch.columns if
                            poisearch[col].isnull().sum() / poisearch.shape[0] > 0.9]

    # 删除长尾特征
    big_top_value_cols_train = [col for col in poisearch.columns if
                                poisearch[col].value_counts(dropna=False, normalize=True).values[0] > 0.9]

    # 最终需要删除的字段
    cols_to_drop = ["YTD", "地址", "adcode", "type", "id", "typecode", "cate1", "location", "cost", "address", "timestamp", "门店名称", "name", "交通枢纽", "business_area", "是否为覆盖门店"]

    leftMain = poisearch[cols_to_drop]
    # 对类别特征做encoder操作
    for f in poisearch.columns:
        if poisearch[f].dtype == "object":
            lbl = LabelEncoder()
            lbl.fit(list(poisearch[f].values))
            poisearch[f] = lbl.transform(list(poisearch[f].values))
            poisearch[f] = poisearch[f].astype(int)

    use_feature = [i for i in poisearch.columns if i not in cols_to_drop]

    corrmat = poisearch.corr()
    plt.subplots(figsize=(12, 9))
    sns.heatmap(corrmat, vmax=0.9, square=True)
    plt.show()
    print(poisearch.shape)
    # 拆分训练和测试集
    train = poisearch[poisearch.YTD != -1]
    # 根据分布做log转换，消除一些gap
    y = np.log1p(train["YTD"])
    y[np.isinf(y)] = 0
    X = train.drop("YTD", axis=1)

    # 划分测试训练集
    X_train, X_test, y_train, y_test = train_test_split(X[use_feature], y, test_size=0.2, random_state=1108)

    k = 5
    knn = KNeighborsRegressor(k)
    knn.fit(X_train, y_train)
    pred2 = knn.predict(X_test)
    print("knn test mae:", mean_absolute_error(np.exp(y_test), np.exp(pred2)))
    print("knn train mae:",mean_absolute_error(np.exp(y_train), np.exp(knn.predict(X_train))))

    GBoost = GradientBoostingRegressor(n_estimators=3000, learning_rate=0.05,
                                       max_depth=4, max_features="sqrt",
                                       min_samples_leaf=15, min_samples_split=10,
                                       loss="huber", random_state=5)
    GBoost.fit(X_train, y_train)
    pred3 = GBoost.predict(X_test)
    print("GBoost test mae:",mean_absolute_error(np.expm1(y_test), np.expm1(pred3)))
    print("GBoost train mae:",mean_absolute_error(np.exp(y_train), np.exp(GBoost.predict(X_train))))
    xpred1 = GBoost.predict(poisearch[use_feature])

    lgr = lgb.LGBMRegressor(objective="regression", num_leaves=32,
                            learning_rate=0.05, n_estimators=720,
                            max_bin=55, bagging_fraction=0.8,
                            bagging_freq=5, feature_fraction=0.2319,
                            feature_fraction_seed=9, bagging_seed=9,
                            min_data_in_leaf=6, min_sum_hessian_in_leaf=11)
    lgr.fit(X_train, y_train)
    pred4 = lgr.predict(X_test)
    # 特征重要性
    # fold_importance_df = pd.DataFrame()
    # fold_importance_df["feature"] = X.columns
    # fold_importance_df["importance"] = lgr.feature_importances_
    # print(fold_importance_df.sort_values("importance", ascending=False))
    # display_importances(fold_importance_df)
    print("lgb test mae:",mean_absolute_error(np.expm1(y_test), np.expm1(pred4)))
    print("lgb train mae:",mean_absolute_error(np.exp(y_train), np.exp(lgr.predict(X_train))))
    xpred2 = lgr.predict(poisearch[use_feature])

    lr = LinearRegression()
    lr.fit(X_train, y_train)
    pred5 = lr.predict(X_test)
    print("lr test mae:",mean_absolute_error(np.expm1(y_test), np.expm1(pred5)))
    print("lr train mae:",mean_absolute_error(np.exp(y_train), np.exp(lr.predict(X_train))))

    lgr = xgb.XGBRegressor(colsample_bytree=0.4603, gamma=0.0468,
                           learning_rate=0.05, max_depth=3,
                           min_child_weight=1.7817, n_estimators=2200,
                           reg_alpha=0.4640, reg_lambda=0.8571,
                           subsample=0.5213, silent=1,
                           random_state=7, nthread=-1)
    lgr.fit(X_train, y_train)
    pred6 = lgr.predict(X_test)
    print("xgb test mae:",mean_absolute_error(np.expm1(y_test), np.expm1(pred6)))
    print("xgb train mae:",mean_absolute_error(np.exp(y_train), np.exp(lgr.predict(X_train))))
    xpred3 = lgr.predict(poisearch[use_feature])

    pred = (pred4 * 0.85 + (pred6 * 0.5 + pred3 * 0.5) * 0.15)
    print(mean_absolute_error(np.expm1(y_test), np.expm1(pred)))
    xpred = (xpred2 * 0.85 + (xpred1 * 0.5 + xpred3 * 0.5) * 0.15)
    poisearch["pred_YTD"] = np.expm1(xpred)
    poisearch["YTD"] = np.expm1(y)
    poisearch = pd.concat([leftMain, poisearch], axis=1)
    poisearch.to_excel("final2.xlsx", index=False)


def display_importances(feature_importance_df_,model_name=''):
    cols = feature_importance_df_[["feature", "importance"]].groupby("feature").mean().sort_values(by="importance",
                                                                                                   ascending=False)[
           :50].index
    best_features = feature_importance_df_.loc[feature_importance_df_.feature.isin(cols)]

    plt.figure(figsize=(8, 10))
    sns.barplot(x="importance", y="feature", data=best_features.sort_values(by="importance", ascending=False))
    plt.title(model_name+" Features (avg over folds)")
    plt.tight_layout()
    plt.savefig(model_name+"_importances.png")


if __name__ == "__main__":

    # filter()
    # copy YTD to Filtered.xlsx
    # feature_eng()
    # saving related pics
    train()