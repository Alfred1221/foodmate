import requests
from bs4 import BeautifulSoup
import os,re,time,datetime,threading
import pymysql
import paramiko
def upload_file(file):
    transport = paramiko.Transport(('106.12.95.104', 22))
    transport.connect(username='root', password='shian12345')
    sftp = paramiko.SFTPClient.from_transport(transport)
    # 将location.py 上传至服务器 /tmp/test.py
    sftp.put(file, '/root/PDF/'+file)
    transport.close()

def runSql(conn, sql):
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        conn.commit()
    except pymysql.Error as e:
        print(str(e))
        conn.rollback()


def getHTMLText(url):
    try:
        #获取服务器的响应内容，并设置最大请求时间为6秒
        res = requests.get(url, timeout = 6)
        #判断返回状态码是否为200
        res.raise_for_status()
        #设置真正的编码
        #res.encoding = res.apparent_encoding
        return res
    except:
        return '产生异常'

def runQuery(conn, sql):
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        ret = cursor.fetchall()
        return ret
    except pymysql.Error as e:
        print(str(e))
        return None

def download(item, item_dict):
    with sem:
        attr_list = []
        page_id = item[0]
        item_dict[page_id] = attr_list
        pdf_url = item[1]
        print("Download Start for page: "+ pdf_url)
        attr_list.append(pdf_url)
        res = getHTMLText(pdf_url)
        if isinstance(res, requests.Response):
            file_name = res.url.split("/")[-1]
            attr_list.append(file_name)
            attr_list.append(str(len(res.content)//1000)+'k')
            with open(file_name, 'wb') as f:
               f.write(res.content)
            upload_file(file_name)
            os.remove(file_name)
            print("Download Success for page: "+ pdf_url)
        else:
            attr_list.append("文件不存在")
            attr_list.append('0k')
            print("Download failed for page:"+pdf_url)

print("Start Time:")
print(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
conn = pymysql.connect(host='192.168.0.101', port=3306, user='yuan', passwd='yuan', db='mysql')
query = "select parentid, download_url from food_safety where download_url<>'' and file_name is NULL"
result = runQuery(conn, query)
item_dict = {}
thread_list = []
os.chdir("PDF/")
sem=threading.Semaphore(10)
for item in result[:20000]:
    t = threading.Thread(target=download, args=(item, item_dict))
    thread_list.append(t)
    t.start()

for thread in thread_list:
    thread.join()
print(len(item_dict))
for key in item_dict.keys():
    attrs = item_dict[key]
    if len(attrs)==3:
        sql = "update food_safety set file_name='"+attrs[1]+"', file_size='"+attrs[2]+"' where parentid = "+str(key)
        runSql(conn,sql)
conn.close()

print("End Time:")
print(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))