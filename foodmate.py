import requests
from bs4 import BeautifulSoup
import os,re

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

def start():
    page_set = set()
    #国内标准page 1 到 10
    for i in range(1,2):
        url = "http://down.foodmate.net/standard/sort/1/index-"+str(i)+".html"
        page_content = getHTMLText(url)

        #解析HTML代码
        soup = BeautifulSoup(page_content.text, 'html.parser')

        #模糊搜索HTML代码的所有<a>标签
        a_labels = soup.find_all('a')

        #获取所有<a>标签中的href对应的值，即超链接
        for a in a_labels:
            sub_url = a.get('href')
            if sub_url and re.match("http://down.foodmate.net/standard/sort/\d+/\d+\.html", sub_url):
                page_set.add(sub_url)
    for page in page_set:
        page_content = getHTMLText(page)
        soup = BeautifulSoup(page_content.text, 'html.parser')

        #找到基本属性
        table = soup.find(class_='xztable')
        #fields = table.find_all(attrs={'title'})
        print(page)
        for td in table.find_all(name='td'):
            if td.find_all(src=re.compile('xxyx')):
                print("现行有效")
            elif td.find_all(src=re.compile('yjfz')):
                print("已经废止")
            else:
                print(td.string)
        tags=set()
        for tag in soup.find_all(href=re.compile('http://down.foodmate.net/standard/hangye.php\?')):
            tags.add(tag.string)
        if len(tags)>0:
            print(tags)
        pdf = soup.find(class_='telecom')
        if pdf:
            pdfurl = pdf.get('href')
            #print(pdfurl)
            res = getHTMLText(pdfurl)
            if isinstance(res, requests.Response):
                file_name = res.url.split("/")[-1]
                print(file_name)
                print(len(res.content))
                with open(file_name, 'wb') as f:
                    f.write(res.content)
        print()
    return

#目标网页
os.chdir('PDF')
start()