#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import pymssql
import json
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

# 目的：获取包裹跟踪信息
# URL：http://track.api.sprintpack.com.cn/api/CNTracking
# Token：U2loZW5nOlNpaGVuZzg4OA==
# Parameter：ProductBarcode

def get_engine():
	db_info = {
		'host': '192.168.1.252',
		'port': '5678',
		'user': 'tom.dong',
		'password': 'oig123456',
		'database': 'wangzf'
	}
	conn_info = 'mssql+pymssql://%(user)s:%(password)s@%(host)s:%(port)s/%(database)s' % db_info
	engine = create_engine(conn_info, encoding = 'utf-8')
	return engine


def delete_no_use_data(conn, cur):
	sql = '''DELETE FROM dbo.ousutong_order_ascan
			 WHERE Datetime IS NULL '''
	cur.execute(sql)
	conn.commit()
	print('=========== 已删除无上网时间的数据 ============')


def delete_repeat_data(conn, cur):
	sql = '''DELETE FROM dbo.ousutong_order_ascan 
			 WHERE id NOT IN (
			 	SELECT MIN(A.id)
			 	FROM dbo.ousutong_order_ascan A
			 	GROUP BY 
			       [TrackingBarcode]
			       ,[OriginCountry]
			       ,[DestinationCountry]
			       ,[Parcelstatus]
			       ,[Status]
			       ,[Location]
			       ,[Datetime]
			       ,[StatusCode]
			 ) '''
	cur.execute(sql)
	conn.commit()
	print('=========== 已删除重复数据 ============')


def get_trackingNo(cur):
	sql = '''SELECT DISTINCT B.TrackNo
			 FROM (
			  	SELECT DISTINCT A.TrackNo TrackNo
			  	FROM dbo.last_three_month_cn_trackNo_api A
			  	WHERE 
			  		A.name LIKE '%欧速通%' AND 
			  		A.printed = 'y'
			  	UNION
			  	SELECT DISTINCT A.跟踪单号 TrackNo
			  	FROM Skyeye.dbo.Tail_Leg_Ebay_Account A
			  	WHERE 
			  		A.发货仓 = 'CN' AND 
			  		A.最终使用服务方式 LIKE '%欧速通%' AND
			  		A.打印状态 = 'y'
			  	UNION
			  	SELECT DISTINCT A.trackNo TrackNo
				FROM Skyeye.dbo.Tail_Leg_Wish_CN_Account_DATA A
				WHERE 
					A.whCode = 'CN' AND 
					A.PostageService LIKE '%欧速通%'
			 ) B
			 WHERE 
			  	B.TrackNo IS NOT NULL AND 
			  	B.TrackNo != '' AND 
			  	B.TrackNo != ' ' AND
			  	B.TrackNo NOT IN (SELECT DISTINCT TrackingBarcode
			    				  FROM dbo.ousutong_order_ascan) '''
	cur.execute(sql)
	result = cur.fetchall()
	return result

# def get_trace_df(js):
# 	TrackingBarcode = js[0]['TrackingBarcode']
# 	OriginCountry = js[0]['OriginCountry']
# 	DestinationCountry = js[0]['DestinationCountry']
# 	Parcelstatus = js[0]['Parcelstatus']
# 	DeliveryStatus = js[0]['DeliveryStatus']
# 	if DeliveryStatus!= []:
# 		trace_df = pd.DataFrame(DeliveryStatus)
# 		trace_df['insertDate'] = datetime.now()
# 		trace_df['TrackingBarcode'] = TrackingBarcode
# 		trace_df['OriginCountry'] = OriginCountry
# 		trace_df['DestinationCountry'] = DestinationCountry
# 		trace_df['Parcelstatus'] = Parcelstatus
# 	return trace_df


def write_to_db(df, engine):
	try:
		pd.io.sql.to_sql(df,
						 name = 'ousutong_order_ascan',
						 con = engine,
						 if_exists = 'append',
						 index = False)
		print('数据成功写入DataBase!')
	except:
		print('数据写入DataBase时出现错误！')

def concat_df(cur, engine):
	trackNo = get_trackingNo(cur)
	trackNo = list(trackNo)
	print('############################################################')
	print('#                      查询%s个跟踪单号：             #' % len(trackNo))
	print('############################################################')
	for i in trackNo:
		print('================== 查询的跟踪单号为: %s =================' % i[0])
		token = 'U2loZW5nOlNpaGVuZzg4OA=='
		headers = {
			'Content-Type': 'text/json; charset=utf-8',
			'Authorization': 'basic ' + token
		}
		url = "http://track.api.sprintpack.com.cn/api/CNTracking"
		params = {
			'ProductBarcode': i[0]
		}
		req = requests.get(url = url, params = params, headers = headers)
		if req.status_code == 200:
			print('数据请求成功...')
			req_js = json.loads(req.text)
			print('数据解析成功...')
			print('请求到的数据：')
			print(req_js)
		else:
			continue		
		# df = get_trace_df(req_js)
		TrackingBarcode = req_js[0]['TrackingBarcode']
		OriginCountry = req_js[0]['OriginCountry']
		DestinationCountry = req_js[0]['DestinationCountry']
		Parcelstatus = req_js[0]['Parcelstatus']
		DeliveryStatus = req_js[0]['DeliveryStatus']
		if DeliveryStatus!= []:
			df = pd.DataFrame(DeliveryStatus)
			df['insertDate'] = datetime.now()
			df['TrackingBarcode'] = TrackingBarcode
			df['OriginCountry'] = OriginCountry
			df['DestinationCountry'] = DestinationCountry
			df['Parcelstatus'] = Parcelstatus
			df = df[df.StatusCode.isin(['R02'])]
			print('Result DataFrame:')
			print(df)
		else:
			continue
		write_to_db(df, engine)


def main():
	conn = pymssql.connect(host = "192.168.1.252",
						   port = "5678",
						   user = "tom.dong",
						   password = "oig123456",
						   database = "wangzf",
						   charset = "utf8")
	cur = conn.cursor()
	delete_repeat_data(conn, cur)
	delete_no_use_data(conn, cur)
	engine = get_engine()
	concat_df(cur, engine)
	cur.close()
	conn.close()

if __name__ == "__main__":
	main()
