#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
import seaborn as sns



import folium

%matplotlib inline

#在地图上标记显示坐标位置
spot = folium.Map(
    location=[43.670, -79.390],
    zoom_start=11
)
folium.Marker([latitude, longitude]).add_to(spot)

#读取csv
df = pd.read_csv(r'D:\mypython\Weclouddata\Project 1\Toronto_weather.csv')
#直接读取网页中的表
df = pd.read_html('https://finviz.com/screener.ashx')
#读取json
df = pd.read_json()
#读取pickle
df = pd.read_pickle()
#读取excel
df = pd.read_excel()
#读取SQL
df = pd.read_sql_table(table_name, con[, schema, …])
df = pd.read_sql_query(sql, con[, index_col, …])
df = pd.read_sql(sql, con[, index_col, …])

