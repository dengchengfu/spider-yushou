#导入库
import random
import time
import os
import re
import requests
from lxml import etree
import pandas as pd 
import pymysql
import time
import re
from urllib.parse import urlparse,parse_qs,urlencode,urlunparse
import mysql


def get_info(url):

    yushou_cookie = """"""
    headers = {
        "cookie": yushou_cookie,
        "referer": "https://detail.tmall.com/",
        "sec-ch-ua":"""" Not A;Brand";v="99", "Chromium";v="90", "Google Chrome";v="90" """,
        "sec-ch-ua-mobile": "?0",
        "sec-fetch-dest": "script",
        "sec-fetch-mode": "no-cors",
        "sec-fetch-site": "cross-site",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"
    }
    
    r=requests.get(url,headers=headers)
    return r.text

#导出文档
def download_csv(data):
    # 获取采集时间
    open_time = time.localtime(time.time())
    open_time = time.strftime("%Y-%m-%d", open_time) 
    
    #获取文件路径
    curt_path = os.path.abspath("__file__")
    
    #创建文件夹
    folderpath = os.path.dirname(curt_path) + "/" + "预售链接数据源"
    if not os.path.exists(folderpath):
        os.mkdir(folderpath)
        

    #获取文件路径
    #注意是os.path.abspath()!!! os.path.dirname() 返回的是所在文件夹的路径!!
    filname = os.path.abspath(folderpath) + "/" + "预定件数_件单价" + "_" + open_time + ".csv"
    if os.path.exists(filname):
        os.remove(filname)
        
    #导出
    head=['id','时间', '预定件数', '件单价']
    details=pd.DataFrame(columns=head,data=data)
    print(details.info())
    #解决中文乱码
    details.to_csv(filname,encoding = "utf_8_sig")
    print(filname)


#-------------------执行部分-----------------------
my_sql = MySQL()
item_dict = my_sql.fetch_Item()
id_list = list(item_dict.keys())
item_links = list(item_dict.values())

yushou_data = []
error_id = []
count = 0
print("一共多少个id: ", len(id_list))
download_time = time.localtime(time.time())
download_time = time.strftime("%Y-%m-%d", download_time)

start_url = "https://mdskip.taobao.com/core/initItemDetail.htm?isUseInventoryCenter=false&cartEnable=false&service3C=false&isApparel=false&isSecKill=false&tmallBuySupport=true&isAreaSell=true&tryBeforeBuy=false&offlineShop=false&itemId={}&showShopProm=false&isPurchaseMallPage=false&itemGmtModified=1621866591000&isRegionLevel=true&household=false&sellerPreview=false&queryMemberRight=true&addressLevel=3&isForbidBuyItem=false&callback=setMdskip&timestamp={}&isg=eBO5ym9qjT3qEss5BO5Zourza77tNIRb4sPzaNbMiIncC63RT-9tiCxQDCVcnd-RR8XAtDYB4Zk2uL29-etfw_HmndCP97RaxxkDB&isg2=BBISzMflpga2_drDab3qDf8oY970Ixa9h4frJdxrm0Xu77LpxLMzzRlOX0tTn45V&cat_id=2&ref=https%3A%2F%2Flist.tmall.com%2F"
urls = [start_url.format(i,str(round(time.time() * 1000))) for i in id_list]


for url in urls:
    count += 1
    _id = parse_qs(url.split("?")[1])["itemId"][0]
    #随机停顿
    time.sleep(random.uniform(0.2,1))
    try:
        doc = get_info(url)
        yushou_order = int(re.search('(.*)groupUC(.*)maxAmount',doc).group(2).split('"')[2])
        yushou_price = float(re.search('(.*)price(.*)promType',doc).group(2).split('"')[2])
        print(count,_id,item_dict[_id])
        print((_id,download_time,yushou_order,yushou_price))
        yushou_data.append((_id,download_time,yushou_order,yushou_price))
    except Exception as e:
        print("*"*50)
        print("{}: 第{}个预定件数未成功".format(str(_id),str(count)),e)
        error_id.append((count,_id,item_dict[_id]))
        print("*"*50)

print("-"*50)
print("开始存入mysql数据库:")
table = "yushou_details_" + time.strftime("%Y%m%d", time.localtime(time.time()))
count_num = []
#my_sql.create_yushouTable(table)
for data in yushou_data:
    print(data,end='\r')
    insert_num = my_sql.insert_yushou(table,data)
    count_num.append(insert_num)

insert_num = sum(count_num)
update_num = len(yushou_data) - sum(count_num)

print("mysql数据表{}:".format(table))
print("插入{}个链接".format(insert_num),"更新{}个链接".format(update_num))


download_csv(yushou_data)



