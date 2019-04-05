import requests
from bs4 import BeautifulSoup
import os,re,time,datetime,threading
import pymysql

def getHTMLText(url):
    try:
        #获取服务器的响应内容，并设置最大请求时间为6秒
        res = requests.get(url, timeout = 6)
        #判断返回状态码是否为200
        res.raise_for_status()
        #设置真正的编码
        res.encoding = res.apparent_encoding
        return res
    except:
        return '产生异常'

def runSql(conn, sql):
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        conn.commit()
        ret = cursor.fetchall()
        return ret
    except pymysql.Error as e:
        print(str(e))
        conn.rollback()
        return None

def get_page_set(target_url):
    page_set = set()
    #国内标准page 1 到 10
    for i in range(1,166):
        print("page "+str(i)+" start.")
        url = target_url + "/index-"+str(i)+".html"
        page_content = getHTMLText(url)

        #解析HTML代码
        if isinstance(page_content, requests.Response):
            soup = BeautifulSoup(page_content.text, 'html.parser')
            #模糊搜索HTML代码的所有<a>标签
            a_labels = soup.find_all('a')
            #获取所有<a>标签中的href对应的值，即超链接
            for a in a_labels:
                sub_url = a.get('href')
                if sub_url and re.match("http://down.foodmate.net/standard/sort/\d+/\d+\.html", sub_url):
                    page_set.add(sub_url)
        else:
            print("Invalid Page: "+ url)
    return page_set

def load_page(page, conn):
    parent_id = page.split("/")[-1].split(".")[0]
    page_content = getHTMLText(page)
    if isinstance(page_content, requests.Response):
        soup = BeautifulSoup(page_content.text, 'html.parser')
    else:
        print("Invalid Page: " + page)

    #找到基本属性
    table = soup.find(class_='xztable')
    #fields = table.find_all(attrs={'title'})
    print(page)
    attr_list = []
    for td in table.find_all(name='td'):
        if td.find_all(src=re.compile('xxyx')):
            attr_list.append("现行有效")
        elif td.find_all(src=re.compile('yjfz')):
            attr_list.append("已经废止")
        elif td.find_all(src=re.compile('jjss')):
            attr_list.append("即将实施")
        elif td.find_all(src=re.compile('wz.gif')):
            attr_list.append("未知")
        else:
            attr_list.append(str(td.string))
    if len(attr_list) == 6:
        type_name=attr_list[0]
        status = attr_list[2]
        department = attr_list[4]
        if '-' in attr_list[1]:
            publish_date = attr_list[1]
        else:
            publish_date = '9999-12-31'
        if '-' in attr_list[3]:
            inplement_date = attr_list[3]
        else:
            inplement_date = '9999-12-31'
        if '-' in attr_list[5]:
            abolish_date = attr_list[5]
        else:
            abolish_date = '9999-12-31'
    tags=set()
    for tag in soup.find_all(href=re.compile('http://down.foodmate.net/standard/hangye.php\?')):
        tags.add(tag.string)
    if len(tags)>0:
        tags = ",".join(list(tags))
    else:
        tags = ''
    pdf = soup.find(class_='telecom')
    if pdf:
        pdfurl = pdf.get('href')
    else:
        pdfurl = ''
        # res = getHTMLText(pdfurl)
        # if isinstance(res, requests.Response):
        #     file_name = res.url.split("/")[-1]
        #     output.write(file_name)
            #output.write(str(len(res.content)))
            #with open(file_name, 'wb') as f:
            #    f.write(res.content)
    sql = "insert into food_safety (parentid, type_name, status, department, industry_type, publish_date, " \
          "inplement_date, abolish_date, url, download_url) value ("+parent_id+", '"+type_name+"', '"+status+"', '"+department+"', '"+tags+"', '"+publish_date+"', '"+inplement_date+"', '"+abolish_date+"', '"+page+"', '"+pdfurl+"')"
    runSql(conn,sql)

def start(target_url):

    # Initialize DB
    #conn = pymysql.connect(host='106.12.74.85', port=3306, user='spider', passwd='spider', db='spider')
    conn = pymysql.connect(host='192.168.0.101', port=3306, user='yuan', passwd='yuan', db='mysql')
    # Out put file initialize
    page_set = get_page_set(target_url)
    #创建线程
    threads = []
    for page in page_set:
        t = threading.Thread(target=load_page,args=(page, conn))
        threads.append(t)
    for t in threads:
        t.start()
        t.join()
    conn.close()  # 断开连接
    return



target_list = {"http://down.foodmate.net/standard/sort/3/":"国家标准",
"http://down.foodmate.net/standard/sort/4/":"进出口行业标准",
"http://down.foodmate.net/standard/sort/5/":"农业标准",
"http://down.foodmate.net/standard/sort/7/":"水产标准",
"http://down.foodmate.net/standard/sort/6/":"商业标准",
"http://down.foodmate.net/standard/sort/8/":"轻工标准",
"http://down.foodmate.net/standard/sort/15/":"地方标准",
"http://down.foodmate.net/standard/sort/16/":"卫生标准",
"http://down.foodmate.net/standard/sort/17/":"化工标准",
"http://down.foodmate.net/standard/sort/14/":"医药标准",
"http://down.foodmate.net/standard/sort/18/":"烟草标准",
"http://down.foodmate.net/standard/sort/46/":"认证认可标准",
"http://down.foodmate.net/standard/sort/19/":"食品安全企业标准",
"http://down.foodmate.net/standard/sort/9/":"其它国内标准",
"http://down.foodmate.net/standard/sort/12/":"团体标准"}

print("Start Time:")
print(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
os.chdir('PDF')
start("http://down.foodmate.net/standard/sort/2/")
print("End Time:")
print(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
