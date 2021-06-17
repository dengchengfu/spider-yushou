
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

#导出文档
def download_csv(data,name):
    # 获取采集时间
    open_time = time.localtime(time.time())
    open_time = time.strftime("%Y-%m-%d", open_time) 
    
    #获取文件路径
    curt_path = os.path.abspath("__file__")
    
    #创建二级文件夹
    folderpath = os.path.dirname(curt_path) + "/" + "竞店数据源"
    if not os.path.exists(folderpath):
        os.mkdir(folderpath)
    sub_folder = folderpath + "/" + "竞店" + open_time
    if not os.path.exists(sub_folder):
        os.mkdir(sub_folder)
    
    #注意是os.path.abspath()!!! os.path.dirname() 返回的是所在文件夹的路径!!
    filname = os.path.abspath(sub_folder) + "/" + name + "_" + open_time + ".csv"
    if os.path.exists(filname):
        os.remove(filname)

    #导出
    head=['id','预售商品名称','预售商品链接','预售店铺名']
    details=pd.DataFrame(columns=head,data=data)
    print(details.info())
    #解决中文乱码
    details.to_csv(filname,encoding = "utf_8_sig")
    print(filname)
    print("竞店 " + name + " 下载完毕")
    


def get_html(url):
    headers = {
    "cookie": cookie,
    "pragma": "no-cache",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36",
    "x-requested-with": "XMLHttpRequest"
    }

    r=requests.get(url,headers=headers)
    html = r.text
    html_dom = etree.HTML(html)
    return html_dom



def get_store(url,name):
    html_dom = get_html(url)
    elements = html_dom.xpath("""/html/body/div/div[3]/div/dl/dd[2]/a""")
    pages =  html_dom.xpath("""/html/body/div/div[2]/p/b[1]""")[0].text.split("/")[1]

    print("开始爬取竞店: " + name + ", 一共{}页".format(str(pages)))
    print("第1页",url)
    
    #首页抓取
    for i in elements:
        link = "https:" + i.attrib["href"].split('"')[1].split('\\')[0]
        _id = parse_qs(link.split("?")[1])["id"][0]
        item_info.append((_id,i.text,link,name))
    
    #循环进入下一页
    for i in range(1,int(pages)):
        time.sleep(random.uniform(0.3,1))
        test_url = url
        test_url = test_url.replace("pageNo=1","pageNo="+str(i+1))
        
        print("第{}页".format(str(i+1)),test_url)
        html_dom = get_html(test_url)
        elements = html_dom.xpath("""/html/body/div/div[3]/div/dl/dd[2]/a""")
        for i in elements:
            link = "https:" + i.attrib["href"].split('"')[1].split('\\')[0]
            _id = parse_qs(link.split("?")[1])["id"][0]
            item_info.append((_id,i.text,link,name))


#-------------------执行部分-----------------------

data = pd.read_excel("竞店店铺id-all.xlsx")
names = data["店铺名"].tolist()
store_list = data["实际链接"].tolist()
store_dict = dict(zip(names,store_list))

crawl_time = time.localtime(time.time())
crawl_time = time.strftime("%Y-%m-%d", crawl_time) 

print("总共需取爬取的竞店数: ",len(store_list))
print("-"*50)

my_sql = mysql.MySQL()


#遍历爬取竞店url
for i in range(len(store_list)):
    item_info = []
    count = 0
    try:
        get_store(store_list[i],names[i])
        download_csv(item_info,names[i])
        print(names[i],"写入数据库mysql")
        for j in item_info:
            #插入数据表ItemID
            insert_num = my_sql.insert_ItemID("ItemID",j)
            if insert_num == 1:
                count+=1
                
        print("{} {} 新增{}个链接".format(names[i],crawl_time, str(count)))
        print("-"*50)
    except Exception as e:
        print(names[i],"发生异常")
        print(e)

print("关闭mysql数据库")
print("全部竞店下载完毕, 开始愉快的预售链接提取叭!!!")
