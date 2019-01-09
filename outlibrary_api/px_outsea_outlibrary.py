#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pymssql
import requests
import json
import numpy as np
import pandas as pd
import time
from datetime import datetime
from sqlalchemy import create_engine
import Logger

def get_engine():
	'''
	* Usage: 创建数据库引擎, 用作将数据写入数据库中目标表 wangzf.dbo.[4px_outLibrary_order], wangzf.dbo.[4px_getOrderCarrier] 
	* return: engine
	'''
	db_info = {
		"host": "192.168.1.252",
		"port": "5678",
		"user": "tom.dong",
		"password": "oig123456",
		"database": "wangzf"
	}
	conn_info = 'mssql+pymssql://%(user)s:%(password)s@%(host)s:%(port)s/%(database)s' % db_info
	engine = create_engine(conn_info, encoding = 'utf-8')
	return engine

def delete_repeat(conn, cur):
	'''
	* 对目标表 wangzf.dbo.[4px_outLibrary_order] 进行去重处理
	'''
	sql = '''DELETE FROM dbo.[4px_outLibrary_order]
			 WHERE id NOT IN (
			 SELECT MIN(A.id)
			 FROM dbo.[4px_outLibrary_order] A
			 GROUP BY
			 	  [orderCode]
			 	  ,[referenceCode]
			 	  ,[warehouseCode]
			 	  ,[carrierCode]
			 	  ,[createTime]
			 	  ,[isOda]
			 	  ,[shippingNumber]
			 	  ,[shippingTime]
			 	  ,[status]
			 	  ,[weight]
			 	  ,[shipWeightPredict]
			 	  ,[shipWeightActual]
			 	  ,[interceptStatus]
			 	  ,[ramStatus]
			 	  ,[orderSign]
			 	  ,[countryCode]
			 	  ,[state]
			 	  ,[city]
			 	  ,[quantity]) '''
	cur.execute(sql)
	conn.commit()

def preAction(conn, cur):
	'''
	* Usage: 
	'''
	sql = """DECLARE 
			 @sdate DATETIME 
			 SET 
			 @sdate = CONVERT(VARCHAR(10), DATEADD(DD, -8, GETDATE()), 21)
			 DECLARE 
			 @edate DATETIME 
			 SET 
			 @edate = CONVERT(VARCHAR(10), GETDATE(), 21)
			 
			 -- 正常订单
			 IF OBJECT_ID('TEMPDB..#res') IS NOT NULL BEGIN DROP TABLE #res END
			 SELECT 
			 	DISTINCT
			 	CASE WHEN A.parcel_id LIKE '%..' THEN LEFT(A.parcel_id, len(A.parcel_id) - 2)
			 		 ELSE A.parcel_id
			 	END AS 卖家订单号
			 	,DATEADD(HH, -8, A.printDate) 打印时间
			 	,A.my_PostageService 派送方式
			 	,DATEADD(HH, -8, B.shippingTime) 出库时间
			 	,A.printed 打印状态
			 INTO #res
			 FROM [192.168.1.220].CenterDB.dbo.parcelPrint A
			 LEFT JOIN dbo.[4PX_outLibrary_order] B
			 	ON A.parcel_id = B.referenceCode
			 WHERE 
			 	A.printDate >= CONVERT(VARCHAR(10), @sdate, 21)
			 	AND A.printDate < CONVERT(VARCHAR(10), @edate, 21)
			 	AND A.wh_code IN ('UK4PX', 'DE4PX')

			 TRUNCATE TABLE dbo.[4px_used_referenceCode] 
			 INSERT INTO dbo.[4px_used_referenceCode]
			 SELECT DISTINCT *
			 FROM (
			 	SELECT 
			 		DISTINCT A.referenceCode referenceCode
			 	FROM dbo.[4PX_outLibrary_order] A
			 	WHERE
			 		A.shippingTime IS NULL 
			 		OR A.shippingTime = '' 
			 		OR A.shippingTime = '1900-01-01 00:00:00.000'
			 	UNION 
			 	SELECT 
			 		DISTINCT B.卖家订单号 referenceCode
			 	FROM #res B
			 	WHERE 
			 		B.出库时间 <= '1900-01-01 00:00:00.000' 
			 		OR B.出库时间 IS NULL
			 ) C """
	cur.execute(sql)
	conn.commit()

def delete_no_use(conn, cur):
	'''
	* Usage: 删除目标表 wangzf.dbo.[4PX_outLibrary_order]中没有出库时间的订单数据
	'''
	sql = """DELETE FROM dbo.[4PX_outLibrary_order]
			 WHERE referenceCode IN (
			 	SELECT A.referenceCode
			 	FROM dbo.[4px_used_referenceCode] A
			 ) """
	cur.execute(sql)
	conn.commit()

def get_used_reference_code(cur):
	sql = '''SELECT DISTINCT A.referenceCode
			 FROM dbo.[4px_used_referenceCode] A '''
	cur.execute(sql)
	result = cur.fetchall()
	return result

def get_wh_code(cur):
	sql =  '''SELECT DISTINCT A.warehouseCode
			  FROM wangzf.dbo.[4px_outLibrary_order] A'''
	cur.execute(sql)
	result = cur.fetchall()
	return result


def write_to_outlibrary(log4px, engine, df):
	pd.io.sql.to_sql(df,
					 name = '4px_outLibrary_order',
			  		 con = engine,
			  		 if_exists = "append",
			  		 index = False)
	log4px.info('数据已成功写入数据库4px_outLibrary_order!')

def write_to_carrier(log4px, engine, df):
	pd.io.sql.to_sql(df,
					 name = '4px_getOrderCarrier',
					 con = engine,
					 if_exists = "append",
					 index = False)
	log4px.info('数据已成功写入数据库4px_getOrderCarrier')

params = {
	'format': 'json',
	'token': '7f256bfd95aa4ad507fff6d71bdba1f8',
	'customerId': '698283',
	'language': 'zh_CN',
	'_method': 'post',
	'version': '3.0.0'
}

headers = {
	'Content-Type': 'application/json'
}

def getOrderCarrier(log4px, engine, whCode):
	url_carrier = "http://openapi.4px.com/api/service/woms/order/getOrderCarrier"
	d = np.unique(whCode)
	for code in d:
		data = json.dumps({
			"warehouseCode": "%s"
		}) % code
		response = requests.post(url = url_carrier, params = params, data = data, headers = headers, timeout = 10)
		if response.status_code == 200:
			results = json.loads(response.text)
			data = results['data']
		else:
			log4px.error("requests error")
	df_carrier = pd.DataFrame(data)
	# write data to database
	write_to_carrier(log4px, engine, df_carrier)


def getDeliveryOrder(log4px, engine, d):
	url_delivery = "http://openapi.4px.com/api/service/woms/order/getDeliveryOrder"
	for code in d:
		log4px.info("查询的卖家订单号为: %s" % code[0])
		data = json.dumps({
			"referenceCode": "%s"
		}) % code[0]
		response = requests.post(url = url_delivery, params = params, data = data, headers = headers, timeout = 10)
		time.sleep(0.3)
		if response.text != None:
			try:
				dict_data = json.loads(response.text)['data']
				log4px.info('请求得到的数据为: %s' % str(dict_data).encode(encoding = "UTF-8"))
			except json.decoder.JSONDecodeError as e:
				continue
			if dict_data is not None:
				orderCode = [dict_data['orderCode']]
				referenceCode = [dict_data['referenceCode']]
				warehouseCode = [dict_data['warehouseCode']]
				carrierCode = [dict_data['carrierCode']]
				createTime = [dict_data['createTime']]
				isOda = [dict_data['isOda']]
				shippingNumber = [dict_data['shippingNumber']]
				shippingTime = [dict_data['shippingTime']]
				status = [dict_data['status']]
				weight = [dict_data['weight']]
				shipWeightPredict = [dict_data['shipWeightPredict']]
				shipWeightActual = [dict_data['shipWeightActual']]
				interceptStatus = [dict_data['interceptStatus']]
				ramStatus = [dict_data['ramStatus']]
				orderSign = [dict_data['orderSign']]
				countryCode = [dict_data['objConsigneeReponseVo']['countryCode']]
				state = [dict_data['objConsigneeReponseVo']['state']]
				city = [dict_data['objConsigneeReponseVo']['city']]
				quantity = [dict_data['lsOrderDetails'][0]['quantity']]
			else:
				continue
		df_dict = {
			'orderCode': orderCode,
			'referenceCode': referenceCode,
			'warehouseCode': warehouseCode,
			'carrierCode': carrierCode,
			'createTime': createTime,
			'isOda': isOda,
			'shippingNumber': shippingNumber,
			'shippingTime': shippingTime,
			'status': status,
			'weight': weight,
			'shipWeightPredict': shipWeightPredict,
			'shipWeightActual': shipWeightActual,
			'interceptStatus': interceptStatus,
			'ramStatus': ramStatus,
			'orderSign': orderSign,
			'countryCode': countryCode,
			'state': state,
			'city': city,
			'quantity': quantity
		}
		df_delivery = pd.DataFrame(df_dict)
		# wirte data to database
		write_to_outlibrary(log4px, engine, df_delivery)


def delete_repeat_carrier(conn, cur): 
	sql = """DELETE FROM dbo.[4px_getOrderCarrier]
			 WHERE id NOT IN (
			 	SELECT MIN(A.id)
			 	FROM dbo.[4px_getOrderCarrier] A
			 	GROUP BY 
			       A.[warehouseCode]
			       ,A.[carrierCode]
			       ,A.[carrierName]
			       ,A.[carrierEName]
			       ,A.[productCode]
			 ) """
	cur.execute(sql)
	conn.commit()


def main():
	start = time.time()

	# 配置日志
	path = "D:/logfiles/outlibrary/4px/log4px%s.log" % datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
	log4px = Logger.Logger(path = path)

	# 连接数据库
	conn = pymssql.connect(host = "192.168.1.252",
						   port = "5678",
						   user = "tom.dong",
						   password = "oig123456",
						   database = "wangzf",
						   charset = "utf8")
	cur = conn.cursor()
	log4px.info("连接wangzf数据库成功！")

	# create the database engine
	engine = get_engine()

	# 数据库表去重，其他准备操作
	delete_repeat(conn, cur)
	preAction(conn, cur)
	delete_no_use(conn, cur)
	
	# 当天需要下载的parcel_id
	d = get_used_reference_code(cur)
	log4px.info("*" * 150)
	log4px.info("查询 %s 个订单" % len(d))
	log4px.info("*" * 150)
	
	# 下载订单出库数据
	getDeliveryOrder(log4px, engine, d)

	# 下载发货方式并对表dbo.[4px_getOrderCarrier]去重
	whCode = get_wh_code(cur)
	print(whCode)
	getOrderCarrier(log4px, engine, whCode)
	delete_repeat_carrier(conn, cur)
	
	# 关闭数据库连接
	cur.close()
	conn.close()
	log4px.info("已关闭数据库！")

	# 计算程序运行时间
	cost = time.time() - start
	log4px.info("运行时间为 %s s" % cost)


if __name__ == "__main__":
	main()
