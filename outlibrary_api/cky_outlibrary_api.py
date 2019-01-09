#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'wangzhefeng'


import requests
import json
import pymssql
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import demjson
import time
from json.decoder import JSONDecodeError
import Logger

def delete_repeat(conn, cur):
	'''
	* 删除目标表 wangzf.dbo.CKY_outLibrary_order 中的重复数据
	'''
	sql = '''DELETE FROM dbo.CKY_outLibrary_order
			 WHERE id NOT IN (
			 	SELECT MIN(id)
			 	FROM dbo.CKY_outLibrary_order A
			 	GROUP BY [Ck1PackageId]
			       ,[HandleStatus]
			       ,[IsTracking]
			       ,[TrackingNumber]
			       ,[ShippingProvider]
			       ,[Ck1OrderShipOuted]
			       ,[Ck1OrderCreated]
			       ,[UnShippedReasonText]
			       ,[Weight]
			       ,[PlateformOrderId]
			       ,[ProductCode]
			       ,[ProductName]
			       ,[SalesPlatform]
			       ,[SellPriceCurrency]
			       ,[SellPrice]
			       ,[Country]
			       ,[Province]
			       ,[City]
			       ,[Street1]
			       ,[Street2]
			       ,[Postcode]
			       ,[Contact]
			       ,[Phone]
			       ,[Email]) '''
	cur.execute(sql)
	conn.commit()


def preAction(conn, cur):
	'''
	* 更新表：dbo.ck1_delete_data_date 中需要下载的订单号
	'''
	sql = """declare 
				@sdate datetime set @sdate = CONVERT(VARCHAR(10), GETDATE() - 8, 21)
			 declare 
			 	@edate datetime set @edate = CONVERT(VARCHAR(10), GETDATE(), 21)
			 
			 -- 正常订单
			 IF OBJECT_ID('TEMPDB..#res') IS NOT NULL BEGIN DROP TABLE #res END
			 SELECT 
			 	DISTINCT 
			 	A.parcel_id 卖家订单号
			 	,DATEADD(HH, -8, A.printDate) 打印时间
			 	,A.my_PostageService 派送方式
			 	,DATEADD(HH, -8, B.Ck1OrderShipOuted) 出库时间
			 	,A.printed 打印状态
			 INTO #res
			 FROM [192.168.1.220].CenterDB.dbo.parcelPrint A
			 LEFT JOIN dbo.CKY_outLibrary_order B
			 	ON A.parcel_id = B.PlateformOrderId
			 WHERE 
			 	A.printDate >= CONVERT(VARCHAR(10), @sdate, 21)
			 	AND A.printDate < CONVERT(VARCHAR(10), @edate, 21)
			 	AND A.wh_code = 'UKCKY'
			 
			 --汇总
			 IF OBJECT_ID('TEMPDB..#A') IS NOT NULL BEGIN DROP TABLE #A END
			 select 
			 	convert(varchar(10), A.打印时间, 21) 打印时间,
			 	A.派送方式,
			 	sum(case when A.打印状态 = 'y' AND datediff(dd, A.打印时间, A.出库时间) = 0 then 1 else 0 end) 当天出库,
			 	sum(case when A.打印状态 = 'y' AND datediff(dd, A.打印时间, A.出库时间) = 1 then 1 else 0 end) 第二天出库,
			 	sum(case when A.打印状态 = 'y' AND datediff(dd, A.打印时间, A.出库时间) = 2 then 1 else 0 end) 第三天出库,
			 	sum(case when (A.打印状态 = 'y' AND datediff(dd, A.打印时间, A.出库时间) < 0) 
			 		OR (A.打印状态 = 'y' AND A.出库时间 IS NULL)
			 		OR (A.打印状态 = 'y' AND A.出库时间 = '')
			 		OR (A.打印状态 = 'y' AND A.出库时间 <= '1901-01-01 00:00:00.000') then 1 else 0 end) 至今未出库,
			 	SUM(CASE WHEN A.打印状态 != 'y' THEN 1 ELSE 0 END) 取消订单数
			 INTO #A
			 FROM #res A
			 group by convert(varchar(10),A.打印时间,21), A.派送方式
			 
			 -- 去除旧数据
			 TRUNCATE TABLE dbo.ck1_delete_data_date
			 INSERT INTO dbo.ck1_delete_data_date
			 SELECT 
			 	DISTINCT 
			 	CONVERT(VARCHAR(10), 打印时间, 21) 打印日期
			 FROM #A A
			 WHERE A.至今未出库 != 0
			 UNION
			 SELECT CONVERT(VARCHAR(10), GETDATE() -1, 21) 打印日期 """
	cur.execute(sql)
	conn.commit()


def delete_no_use(conn, cur):
	'''
	* 删除目标表wangzf.dbo.CKY_outLibrary_order中未出库的数据
	'''
	sql = """DELETE FROM dbo.CKY_outLibrary_order
			 WHERE CONVERT(VARCHAR(10), Ck1OrderCreated, 21) IN (SELECT 打印日期 FROM dbo.ck1_delete_data_date) """
	cur.execute(sql)
	conn.commit()


def get_token():
	'''
	* Usage: 获取出口易API的Token, 出口易的Token是不断变化的
	* Return: 出口易token
	'''
	conn_MMS = pymssql.connect(host = "192.168.1.220",
	                       	   user = "tom.dong",
	                       	   password = "oig123456",
	                       	   database = 'MMS',
	                       	   charset ='utf8')
	cur_MMS = conn_MMS.cursor()
	sql = '''SELECT AccessToken
			 FROM dbo.chukou1_token'''
	cur_MMS.execute(sql)
	token = cur_MMS.fetchall()[0][0]
	cur_MMS.close()
	conn_MMS.close()
	return token


def get_UsedPrintedDate(cur):
	'''
	* Usage: 查询存在未出库订单所在日期
	'''
	sql = '''SELECT 打印日期
			 FROM dbo.ck1_delete_data_date'''
	cur.execute(sql)
	date = cur.fetchall()   
	return date


def get_parse(date, token, pageIndex):
	'''
	* 请求某天，某页的数据(每一页数据有200条数据)
	'''
	FromDate = '%sT00:00:00Z' % str((date).strftime('%Y-%m-%d'))
	ToDate = '%sT00:00:00Z' % str((date + timedelta(days = 1)).strftime('%Y-%m-%d'))
	params = {
		"FromDate": FromDate,
		"ToDate": ToDate,
		"PageSize": 200,
		"PageIndex": pageIndex,
		"TypeOfSearchDate": None,
		"HandleStatus": None
	}
	headers = {
		"Authorization": "Bearer " + token,
		"Content-Type": "application/json; charset=utf-8"
	}
	url = "https://openapi.chukou1.cn/v1/outboundOrders"
	response = requests.get(url = url, params = params, headers = headers)
	time.sleep(0.1)
	if response.status_code == 502:
		response = requests.get(url = url, params = params, headers = headers)
	response_str = response.text
	response_list = demjson.encode(response_str)
	response_dict = demjson.decode(response_list)
	if response_dict != []:
		data_list = json.loads(response_dict)
		if data_list != []:
			return data_list


def get_parse_per_date(logcky, date, token):
	'''
	* 请求每一个date的数据(翻页)
	'''
	data_list_per_date = []                                           # 某天data(list)
	for i in range(201)[1:]:                                          # 200页
		data_list_per_page = get_parse(date, token, i)                # 某天，某一页的数据
		logcky.info('%s 第%s页 数据: %s' % (date, i, data_list_per_page))
		if data_list_per_page != None:
			for j in range(len(data_list_per_page)):                  # 200条
				if data_list_per_page[j] != None:                     # 某天，某页，每条数据
					data_list_per_date.append(data_list_per_page[j])
					logcky.info('%s 第%s页 第%s条 数据: %s' % (date, i, j, data_list_per_page[j]))
	logcky.info('数据: %s' % data_list_per_date)
	return data_list_per_date


def get_outBoundOrders(data_list):
	'''
	* parameter:某天的所有数据列表
	'''
	Ck1PackageId = []
	HandleStatus = []
	IsTracking = []
	TrackingNumber = []
	ShippingProvider = []
	Ck1OrderShipOuted = []
	Ck1OrderCreated = []
	UnShippedReasonText = []
	Weight = []
	PlateformOrderId = []
	ProductCode = []
	ProductName = []
	SalesPlatform = []
	SellPriceCurrency = []
	SellPrice = []
	Country = []
	Province = []
	City = []
	Street1 = []
	Street2 = []
	Postcode = []
	Contact = []
	Phone = []
	Email = []
	if data_list != None:
		for j in range(len(data_list)):
			if data_list[j] != None:
				Ck1PackageId.append(data_list[j]['Ck1PackageId'])
				HandleStatus.append(data_list[j]['HandleStatus'])
				IsTracking.append(data_list[j]['IsTracking'])
				if 'TrackingNumber' not in data_list[j]:
					data_list[j]['TrackingNumber'] = ""
				TrackingNumber.append(data_list[j]['TrackingNumber'])
				ShippingProvider.append(data_list[j]['ShippingProvider'])
				if 'Ck1OrderShipOuted' not in data_list[j]:
					data_list[j]['Ck1OrderShipOuted'] = ""
					Ck1OrderShipOuted.append(data_list[j]['Ck1OrderShipOuted'])
				elif int(data_list[j]['Ck1OrderShipOuted'][6:19]) < 0:
					Ck1OrderShipOuted.append("")
				else:
					str1 = data_list[j]['Ck1OrderShipOuted']
					str2 = datetime.fromtimestamp(int(str1[6:19]) / 1000).strftime('%Y-%m-%d %H:%M:%S')
					Ck1OrderShipOuted.append(str2)
				str3 = data_list[j]['Ck1OrderCreated']
				str4 = datetime.fromtimestamp(int(str3[6:19]) / 1000).strftime('%Y-%m-%d %H:%M:%S')
				Ck1OrderCreated.append(str4)
				UnShippedReasonText.append(data_list[j]['UnShippedReasonText'])
				Weight.append(data_list[j]['Weight'])
				PlateformOrderId.append(data_list[j]['PlateformOrderId'])
				ProductCode.append(data_list[j]['ProductCode'])
				if 'ProductName' not in data_list[j]:
					data_list[j]['ProductName'] = ""
				ProductName.append(data_list[j]['ProductName'])
				SalesPlatform.append(data_list[j]['SalesPlatform'])
				SellPriceCurrency.append(data_list[j]['SellPriceCurrency'])
				SellPrice.append(data_list[j]['SellPrice'])
				Country.append(data_list[j]['ShipToAddress']['Country'])
				Province.append(data_list[j]['ShipToAddress']['Province'])
				City.append(data_list[j]['ShipToAddress']['City'])
				Street1.append(data_list[j]['ShipToAddress']['Street1'])
				Street2.append(data_list[j]['ShipToAddress']['Street2'])
				Postcode.append(data_list[j]['ShipToAddress']['Postcode'])
				Contact.append(data_list[j]['ShipToAddress']['Contact'])
				Phone.append(data_list[j]['ShipToAddress']['Phone'])
				Email.append(data_list[j]['ShipToAddress']['Email'])
	data_dict = {
		'Ck1PackageId': Ck1PackageId,
		'HandleStatus': HandleStatus,
		'IsTracking': IsTracking,
		'TrackingNumber': TrackingNumber,
		'ShippingProvider': ShippingProvider,
		'Ck1OrderShipOuted': Ck1OrderShipOuted,
		'Ck1OrderCreated': Ck1OrderCreated,
		'UnShippedReasonText': UnShippedReasonText,
		'Weight': Weight,
		'PlateformOrderId': PlateformOrderId,
		'ProductCode': ProductCode,
		'ProductName': ProductName,
		'SalesPlatform': SalesPlatform,
		'SellPriceCurrency': SellPriceCurrency,
		'SellPrice': SellPrice,
		'Country': Country,
		'Province': Province,
		'City': City,
		'Street1': Street1,
		'Street2': Street2,
		'Postcode': Postcode,
		'Contact': Contact,
		'Phone': Phone,
		'Email': Email
	}
	df = pd.DataFrame(data_dict)
	return df


def get_engine():
	'''
	* Usage: 创建数据库引擎
	'''
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

def write_to_db(logcky, df, engine):
	pd.io.sql.to_sql(df,
					 name = 'CKY_outLibrary_order',
					 con = engine,
					 if_exists = 'append',
					 index = False)
	logcky.info("数据已成功写入wangzf.dbo.CKY_outLibrary_order!")

def main():
	start = time.time()
	path = "D:/logfiles/outlibrary/cky/logcky%s.log" % datetime.now().strftime('%Y-%m-%d')
	logcky = Logger.Logger(path = path)
	'''
	* connect database [192.168.1.252,5678].wangzf
	'''
	conn = pymssql.connect(host = "192.168.1.252",
						   port = "5678",
						   user = "tom.dong",
						   password = "oig123456",
						   database = "wangzf",
						   charset = "utf8")
	cur = conn.cursor()
	'''
	* 处理数据库中的数据，为下载数据做准备
	'''
	preAction(conn, cur)
	delete_no_use(conn, cur)
	'''
	* 获取当天需要下载的订单的卖家订单号
	'''
	date = get_UsedPrintedDate(cur)
	engine = get_engine()
	token = get_token()
	for i in date:
		logcky.info('当前下载日期: %s' % i[0])
		data = get_parse_per_date(logcky, i[0], token)
		logcky.info('%s 的未解析数据: %s' % (i, data))
		df = get_outBoundOrders(data)
		logcky.info('%s 数据表: %s' % (i, df))
		write_to_db(logcky, df, engine)
	'''
	* disconnect the database
	'''
	cur.close()
	conn.close()
	logcky.info("已关闭数据库!")

	cost = time.time() - start
	logcky.info("运行时间: %s" % cost)


if __name__ == '__main__':
	main()




