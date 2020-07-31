#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup # For HTML parsing

import time # To prevent overwhelming the server between connections
from collections import Counter # Keep track of our term counts
import pandas as pd

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from user_agent import generate_user_agent

from selenium.webdriver import ActionChains

from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from xml.etree import ElementTree as et

import requests
import json
import re

from selenium.webdriver.common.proxy import Proxy
from selenium.webdriver.common.proxy import ProxyType
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

import pytesseract
from PIL import Image

import pytz
import csv
import pymysql
from pymongo import MongoClient
import redis
import random
#直接获取网页信息
s = requests.sessions()
r = s.get(url)
#写入json格式
j = json.dumps(r.text) #s是string，dump（）直接接文件路径
#读取json格式
j = json.loads(r.text)
#声明代理
proxy = Proxy({'proxyType': ProxyType.MANUAL,'httpProxy': 'http://user:password@proxy.com:8080'})
#配置对象 DesiredCapabilities
dc=DesiredCapabilities.PhantomJS.copy()
#把代理 ip 加入配置对象
proxy.add_to_capabilities(dc)
#selenium打开PhantomJS
dr=webdriver.PhantomJS(desired_capabilities=dc)

#选择Chromeoption
option = webdriver.ChromeOptions()
#生成伪装表头
agent = generate_user_agent(device_type="desktop", os=('win'))
#使用表头
headers = { 'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en,zh-CN;q=0.9,zh;q=0.8',
            'user-agent':agent
            #最好手动复制自chrome表头修改，如果有cookie更佳
          }
option.add_argument('headers={}'.format(headers))
#打开隐身模式，最大化窗口
option.add_argument('--incognito --start-maximized')
#使用VPN
option.add_argument('--proxy-server=http://user:password@proxy.com:8080')
#selenium打开Chrome
driver = webdriver.Chrome(executable_path=r'D:\mypython\Weclouddata\TA\chromedriver.exe', options=option)

url = 'https://www.accuweather.com/en/ca/mississauga/l5b/{}-weather/55071?year={}&view=list'

months = {1: 'january',
          2: 'february',
          3: 'march',
          4: 'april',
          5: 'may',
          6: 'june',
          7: 'july',
          8: 'august',
          9: 'september',
          10: 'october',
          11: 'november',
          12: 'december'}

year = range(2009, 2010)

for y in year:
    for i in range(1, 13):
        # 停留两秒后打开网页
        time.sleep(2)
        driver.get(url.format(months[i], y))
        # 在停留两秒后后退
        time.sleep(2)
        driver.back()
        # 在停留两秒后前进
        time.sleep(2)
        driver.forward()
        # 在停留两秒后刷新
        time.sleep(2)
        driver.refresh()
        # 在停留两秒后运行JS脚本
        time.sleep(2)
        js = "var q=document.documentElement.scrollTop=10000" #JS="window.scrollTo(10000,document.body.scrollHeight)"
        driver.execute_script(js)
        # 截屏现在的页面
        driver.save_screenshot("save_1.png")
        # 从文本从获取 XML，查找获得的第一个
        root = et.fromstring(xml_string)
        find_node = root.find("name")
        print(find_node, find_node.tag, find_node.text)


        # 模拟鼠标操作实现下拉
        ac = driver.find_element_by_xpath("//ul[@infinite-scroll-disabled]/li[last()]")
        # 定位鼠标到指定元素
        ActionChains(driver).move_to_element(ac).perform()

        #鼠标操作：
        # 鼠标单击
        click(on_element=None)
        # 鼠标单击后不松开
        click_and_hold(on_element=None)
        # 右键单击
        context_click(on_element=None)
        # 双击
        double_click(on_element=None)
        # 拖动到某个元素
        drag_and_drop(source, target)
        # 拖拽到某个坐标然后松开
        drag_and_drop_by_offset(source, xoffset, yoffset)
        # 释放鼠标左键
        release(on_element=None)
        # 鼠标移动到某个坐标
        move_by_offset(xoffset, yoffset)
        #鼠标移动到某元素
        move_to_element(to_element)
        #移动到距某元素左上方多少坐标距离的位置
        move_to_element_with_offset(to_element, xoffset, yoffset)
        #向某个表格里面写入表单
        elem = driver.find_elements_by_id()
        ActionChains(driver).key_down(Keys.DOWN, elem).key_up(Keys.DOWN, elem).perform()
        # 向某个位置粘贴
        ActionChains(driver).key_down(Keys.CONTROL).send_keys('v').key_up(Keys.CONTROL).perform()
        #键盘操作:
        #按下某个键盘上的键
        key_down(value, element=None)
        #松开某个键
        key_up(value, element=None)
        # 发送
        send_keys(*args)
        # 执行已经添加到生成器的操作
        perform()

        #打开另外一个窗口
        JS = 'window.open("https://www.sogou.com");
        driver.execute_script(JS)
        # css名查找搜索框,并搜索selenium
        driver.find_element_by_css_selector("#kw").send_keys("selenium")
        driver.find_element_by_css_selector("#su").click()
        # 打开新闻
        time.sleep(2)
        driver.find_element_by_link_text("新闻").click()

        # 打开浏览器,设置隐式等待,隐性等待设置一次对整个程序运行过程全部起作用,只要设置一次即可,不需要在每次都书写一下。
        driver.implicitly_wait(30)
        # 使用显式等待
        try:
            WebDriverWait(driver, 20, 0.5).until(EC.presence_of_element_located((By.LINK_TEXT, u'首页')))
        finally:
            print(driver.find_element_by_link_text('首页').get_attribute('href'))
        # 获取所有句柄
        all_handles = driver.window_handles
        # 跳转到最后一个句柄,也就是刚刚打开的页面
        driver.switch_to_window(all_handles[-1])






        date_s = []
        high = []
        low = []
        precip = []
        HVhigh = []
        HVlow = []
        snow = []

        for i in range(1, 32):
            try:
                #摘取网页中匹配正则表达式的信息（先抓大）
                content = re.findall('<br />(.*?)<',driver,re.S)
                #进一步摘取content中的有用信息（再抓小）
                tbody = re.findall('正文(.*?)</tbody>',content,re.S)

                #下载图片
                pic=requests.get('pic_url').content
                with open('abc.png','wb') as f:
                    f.write(pic)
                #识别图片中的文字
                pic=Image.open('abc.png')
                code= pytesseract.image_to_string(pic)


                d = driver.find_element_by_xpath(
                    f'/html/body/div/div[5]/div/div[1]/div/div[2]/div[{i}]/div[1]/div/div/div[1]/p[2]').text
                date_s.append(d + '/' + str(y))
                h = driver.find_element_by_xpath(
                    f'/html/body/div/div[5]/div/div[1]/div/div[2]/div[{i}]/div[1]/div/div/div[2]/span[1]').text
                high.append(h[:-1])
                l = driver.find_element_by_xpath(
                    f'/html/body/div/div[5]/div/div[1]/div/div[2]/div[{i}]/div[1]/div/div/div[2]/span[2]').text
                low.append(l.split()[1][:-1])
                p = driver.find_element_by_xpath(
                    f'/html/body/div/div[5]/div/div[1]/div/div[2]/div[{i}]/div[1]/div/div/div[3]/p[2]').text
                precip.append(p.split()[0])
                hvh = driver.find_element_by_xpath(
                    f'/html/body/div/div[5]/div/div[1]/div/div[2]/div[{i}]/div[2]/div/div[2]/p[1]').text
                HVhigh.append(hvh)
                hvl = driver.find_element_by_xpath(
                    f'/html/body/div/div[5]/div/div[1]/div/div[2]/div[{i}]/div[2]/div/div[2]/p[2]').text
                HVlow.append(hvl.split()[3][:-1])
                s = driver.find_element_by_xpath(
                    f'/html/body/div/div[5]/div/div[1]/div/div[2]/div[{i}]/div[2]/div/div[1]/p[3]').text
                if s == '':
                    snow.append('0')
                else:
                    snow.append(s.split()[1])
            except NoSuchElementException:
                snow.append('0')
            continue
        HV_high = []
        HV_low = []
        for hvh in HVhigh:
            try:
                HV_high.append(hvh.split()[3][:-1])

            except:
                pass
            continue
        for hvl in HVlow:
            try:
                HV_low.append(hvl.split()[3][:-1])

            except:
                pass
            continue
        dict = {'date': date_s, 'high(°C)': high, 'low(°C)': low, 'precip(mm)': precip, 'snow(mm)': snow,
                'HV_high(°C)': HV_high, 'HV_low(°C)': HV_low}


        df = pd.DataFrame(dict)

        df['date'] = pd.to_datetime(df['date'])

        # 转换时间到本地时间
        df[i] = df[i].dt.tz_localize(tz=pytz.timezone('Canada/Eastern'))

        df.set_index('date', inplace=True)

        df.to_csv(r'D:\mypython\Weclouddata\Project 1\{}_{}.csv'.format(y, i))

        # 把数据写入csv
        with open(r'D:\mypython\Weclouddata\Project 1\Toronto_weather.csv', 'a', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=csv_columns, extrasaction='ignore')
            if i == 1:
                w.writeheader()
            else:
                pass
            w.writerow(d)

        # 连接mysql数据库
        conn = pymysql.connect(host='127.0.0.1', user='root', password='password', database='test', port=3306, charset='utf8')
        cur = conn.cursor()

        # 使用 execute()方法执行 SQL 查询
        # 使用 fetchone() 方法获取单条数据.
        version = "SELECT VERSION()"
        cur.execute(version)
        data = cur.fetchone()
        # 现在获取所有数据
        select = "SELECT * FROM user_info"
        exe_select = cur.execute(select)
        all_data = cur.fetchall()
        # 向下移动一行,绝对值从数据输出的上一行开始
        # mode 默认值为相对
        cur.scroll(1, mode='absolute')
        data1 = cur.fetchone()
        print(data1)
        # 向上移动两行
        # 因为游标现在相当在输出所有数据的下一行
        cur.scroll(-2, mode='relative')
        data2 = cur.fetchone()
        print(data2)
        # 插入数据
        insert = "INSERT INTO user_info VALUES('li_liu',2,123456789);"
        count = cur.execute(insert)
        conn.commit()
        # 关闭游标
        cur.close()
        # 关闭数据库连接
        conn.close()

        #连接mongoDB
        client = MongoClient('mongodb://username:password@IP:port')
        database = client['test']
        collection = database['spider']
        #插入数据
        mongodata = {'key1':value1,'key2':value2}
        collection.insert(mongodata)
        #查找数据
        result = collection.find_one()
        result = collection.find()
        result = collection.find({'key1':value1})
        result = collection.find({'key1':value1},{_id:0,'key2':1,'key3':1})
        #逻辑查询
        result = collection.find({'key1':{'$gt':29,'$lt':40}}) #$gt大于 $lt小于 $gte大于等于 $lte小于等于 $eq等于 $ne不等于
        #排序
        result = collection.find({'key1':{'$gt':29,'$lt':40}}).sort('age',1) #1升序, -1降序
        #更新数据
        collection.update_one({'key1':value1},{'$set':{'key2'=value3}}) #改一个
        collection.update_many({'key1':value1},{'$set':{'key2'=value3}}) #改所有
        #删除数据
        collection.delete_one({'age':20})  # 改一个
        collection.delete_many() # 改所有
        #去重
        collection.distinct({'age':20})



        #连接redis
        client = redis.StrictRedis(host='0.0.0.0',port=2739,password='12345')
        #列表插入数据
        client.lpush('list','123') #右侧是rpush
        #列表查看长度
        client.llen('list')
        #列表读取值
        client.lpop('list') #右侧读取是rpop
        # 集合插入数据
        client.sadd('list', '123','456','789')
        # 集合查看长度
        client.scard('list')
        # 集合读取值
        client.spop('list')



        time.sleep(10 + 10 * random.random())  # sleep(10) stops the notebook for 10 seconds. I use sleep here to wait for loading the page.
        driver.quit()
