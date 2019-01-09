#!/usr/bin/env python
# -*- coding: utf-8 -*-


from datetime import datetime, timedelta
import hashlib
import requests
import json
import pymssql
import pandas as pd
from sqlalchemy import create_engine
import time
import Logger


# 基础参数===================================================================
# action:      方法名
# app_key:     万邑连账户
# client_id:   应用id
# timestamp:   时间
# version:     版本
# sign:        客户签名
# client_sign: 应用签名
# sign_method: 签名方式
# format:      格式
# platform:    应用Code
# language:    语言
# data:        业务参数
# ===========================================================================


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


def delete_repeat(conn, cur):
	sql  = '''DELETE FROM dbo.WINIT_outLibrary_order
			  WHERE ID NOT IN (
			  	SELECT MIN(A.ID)
			  	FROM dbo.WINIT_outLibrary_order A
			  	GROUP BY [sellerOrderNo], [countryName], [trackingNo], [weight], [statusName], 
			  		[eBaySellerID], [itemqty], [estimateFees], [currency], [winitTrackingNo], 
			  		[warehouseName], [winit_trackingno], [deliverywayName], [volume], [dateOrdered], 
			  		[dateFinish], [name], [exwarehouseId], [isRepeat], [deliverywayId], [sellerWeight], 
			  		[status], [postal], [winitProductCode]) '''
	cur.execute(sql)
	conn.commit()


def preAction(conn, cur):
	sql = """declare @sdate datetime set @sdate = CONVERT(VARCHAR(10), DATEADD(DD, -8, GETDATE()), 21)
			 declare @edate datetime set @edate = CONVERT(VARCHAR(10), GETDATE(), 21)
			 -- 正常订单
			 IF OBJECT_ID('TEMPDB..#res') IS NOT NULL BEGIN DROP TABLE #res END
			 SELECT 
			 	DISTINCT 
			 	A.parcel_id 卖家订单号
			 	,CASE WHEN A.wh_code = 'USKYNWINIT' THEN DATEADD(HH, -11, A.printDate)
			 		  WHEN A.wh_code = 'USTXWINIT' THEN DATEADD(HH, -10, A.printDate)
			 		  WHEN A.wh_code = 'USWCWINIT' THEN DATEADD(HH, -9, A.printDate)
			 		  WHEN A.wh_code = 'AUWINIT' THEN DATEADD(HH, 3, A.printDate)
			 		  WHEN A.wh_code = 'UKWINIT' THEN DATEADD(HH, -8, A.printDate)
			 		  WHEN A.wh_code = 'DEWINIT' THEN DATEADD(HH, -7, A.printDate) 
			 	 END AS 打印时间
			 	,A.my_PostageService 派送方式
			 	,A.wh_code 仓库名称
			 	,CASE WHEN A.wh_code = 'USKYNWINIT' THEN DATEADD(HH, -11, B.dateFinish)
			 		  WHEN A.wh_code = 'USTXWINIT' THEN DATEADD(HH, -10, B.dateFinish)
			 		  WHEN A.wh_code = 'USWCWINIT' THEN DATEADD(HH, -9, B.dateFinish)
			 		  WHEN A.wh_code = 'AUWINIT' THEN DATEADD(HH, 3, B.dateFinish)
			 		  WHEN A.wh_code = 'UKWINIT' THEN DATEADD(HH, -8, B.dateFinish)
			 		  WHEN A.wh_code = 'DEWINIT' THEN DATEADD(HH, -7, B.dateFinish) 
			 	 END AS 出库时间                                                     -- 时差调整
			 	,A.printed 打印状态
			 INTO #res
			 FROM [192.168.1.220].CenterDB.dbo.parcelPrint A
			 LEFT JOIN dbo.WINIT_outlibrary_order B
			 	ON A.parcel_id = B.sellerOrderNo
			 WHERE 
			 	CONVERT(VARCHAR(10), A.printDate, 21) >= CONVERT(VARCHAR(10), @sdate, 21)
			 	AND CONVERT(VARCHAR(10), A.printDate, 21) < CONVERT(VARCHAR(10), @edate, 21)
			 	AND A.wh_code LIKE '%WINIT'
			 
			 TRUNCATE TABLE dbo.winit_used_sellerOrderNo
			 INSERT INTO dbo.winit_used_sellerOrderNo
			 SELECT *
			 FROM (
			 	SELECT 
			 		DISTINCT
			 		A.sellerOrderNo,
			 		A.warehouseName
			 	FROM dbo.WINIT_outLibrary_order A
			 	WHERE 
			 		A.dateFinish IS NULL 
			 		OR A.dateFinish = '' 
			 		OR A.dateFinish = '1900-01-01 00:00:00.000'
			 	UNION 
			 	SELECT 
			 		卖家订单号 sellerOrderNo,
			 		仓库名称 warehouseName
			 	FROM #res
			 	WHERE 
			 		出库时间 IS NULL
			 		OR 出库时间 = '' 
			 		OR 出库时间 = '1900-01-01 00:00:00.000'
			 ) B """
	cur.execute(sql)
	conn.commit()


def delete_no_use(conn, cur):
	sql = """DELETE FROM dbo.WINIT_outLibrary_order
			 WHERE sellerOrderNo IN (
			 	SELECT DISTINCT A.sellerOrderNo
			 	FROM dbo.winit_used_sellerOrderNo A
			 ) """
	cur.execute(sql)
	conn.commit()


def get_uk_de_sellerOrderNo(cur):
	sql = '''SELECT DISTINCT A.sellerOrderNo
			 FROM dbo.winit_used_sellerOrderNo A
			 WHERE 
			 	(A.warehouseName NOT IN ('USTXWINIT', 'USWCWINIT', 'USKYNWINIT', 'AUWINIT', 
			  						      'USTX Warehouse', 'USWC Warehouse', 'USKYN Warehouse', 'AU Warehouse') 
			 	OR A.warehouseName IS NULL)
			 	AND A.sellerOrderNo NOT IN (SELECT DISTINCT sellerOrderNo
			 								FROM dbo.WINIT_outLibrary_order) 
			 	AND A.sellerOrderNo NOT IN ('ZZZZMI3P49GSO171UY', 'ZZZZPICH4YJSS09JH00') '''
	cur.execute(sql)
	result = cur.fetchall()
	return result


def get_au_us_sellerOrderNo(cur):
	sql = '''SELECT DISTINCT A.sellerOrderNo
			 FROM dbo.winit_used_sellerOrderNo A
			 WHERE A.warehouseName IN ('USTXWINIT', 'USWCWINIT', 'USKYNWINIT', 'AUWINIT', 
			  					      'USTX Warehouse', 'USWC Warehouse', 'USKYN Warehouse', 'AU Warehouse') 
			  	   AND A.sellerOrderNo NOT IN (SELECT DISTINCT sellerOrderNo
			 								   FROM dbo.WINIT_outLibrary_order) '''
	cur.execute(sql)
	result = cur.fetchall()
	return result


def get_sellerOrderNo(cur):
	sql = '''declare @sdate datetime set @sdate = '2018-03-19'
			 declare @edate datetime set @edate = CONVERT(VARCHAR(10), GETDATE(), 21)
			 SELECT 
			  	DISTINCT A.parcel_id 卖家订单号
			 FROM [192.168.1.220].CenterDB.dbo.parcelPrint A
			 WHERE 
			  	CONVERT(VARCHAR(10), A.printDate, 21) >= CONVERT(VARCHAR(10), @sdate, 21)
			  	AND CONVERT(VARCHAR(10), A.printDate, 21) < CONVERT(VARCHAR(10), @edate, 21)
			  	AND A.wh_code IN ('USTXWINIT', 'USWCWINIT', 'USKYNWINIT', 'AUWINIT', 'DEWINIT', 'UKWINIT')
		  '''
	cur.execute(sql)
	result = cur.fetchall()
	return result


def md5(str):
	m = hashlib.md5()
	m.update(str.encode('utf-8'))
	return m.hexdigest().upper()


def get_OutLibrary(SellOrder, dateOrderedStartDate, dateOrderedEndDate):
	token = "20BC45092988B9AA6D740514060530C0"
	actionValue = "queryOutboundOrderList"
	app_keyValue = "ken@oigbuy.com"
	dataValue = '{"sellerOrderNo":"' + SellOrder + \
				'","dateOrderedStartDate":"' + dateOrderedStartDate + \
				'","dateOrderedEndDate":"' + dateOrderedEndDate + \
				'","PageSize":' + str(1) + \
				',"PageNum":' + str(1) + '}'
	formatValue = "json"
	platformValue = "SELLERERP"
	sign_methodValue = "md5"
	timestampValue = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	versionValue = "1.0"
	# sign
	sign_str = token + \
			   "action" + actionValue + \
			   "app_key" + app_keyValue + \
			   "data" + dataValue + \
			   "format" + formatValue + \
			   "platform" + platformValue + \
			   "sign_method" + sign_methodValue + \
			   "timestamp" + timestampValue + \
			   "version" + versionValue + \
			   token
	sign = md5(sign_str)
	# url
	url_outsea = "http://api.winit.com.cn/ADInterface/api"
	# headers
	headers = {"Authorization": "Bearer 20BC45092988B9AA6D740514060530C0",
			   "Content-Type": "application/json"}
	# params
	params = json.dumps({
			"action": actionValue,
			"app_key": app_keyValue,
			"timestamp": timestampValue,
			"version": versionValue,
			"sign": sign,
			"sign_method": sign_methodValue,
			"format": formatValue,
			"platform":platformValue,
			"language":"zh_CN",
			"data": {
				"sellerOrderNo": SellOrder,
				"dateOrderedStartDate": dateOrderedStartDate,
				"dateOrderedEndDate": dateOrderedEndDate,
				"PageSize": 1,
				"PageNum": 1
			}
		})
	# request
	response = requests.post(url = url_outsea, data = params, headers = headers, timeout = 10)
	js = json.loads(response.text)
	return js


def parse_json(logwinit, obOrderNum, dateOrderedStartDate, dateOrderedEndDate, engine):
	for i in obOrderNum:
		logwinit.info('查询的ParcelID为 %s' % i)
		response = get_OutLibrary(i[0].strip().replace('\n', ''), dateOrderedStartDate, dateOrderedEndDate)
		data = response['data']
		logwinit.info('查询得到的数据为: %s' % str(data).encode(encoding = "UTF-8"))
		if data['list'] != []:
			# print(data['list'])
			for j in range(len(data['list'])):
				Item = data['list'][j]
				# =========================================
				sellerOrderNo = [Item['sellerOrderNo']]
				countryName = [Item['countryName']]
				trackingNo = [Item['trackingNo']]
				weight = [Item['weight']]
				statusName = [Item['statusName']]
				eBaySellerID = [Item['eBaySellerID']]
				itemqty = [Item['itemqty']]
				estimateFees = [Item['estimateFees']]
				currency = [Item['currency']]
				winitTrackingNo = [Item['winitTrackingNo']]
				warehouseName = [Item['warehouseName']]
				winit_trackingno = [Item['winit_trackingno']]
				deliverywayName = [Item['deliverywayName']]
				volume = [Item['volume']]
				dateOrdered = [Item['dateOrdered']]
				dateFinish = [Item['dateFinish']]
				name = [Item['name']]
				exwarehouseId = [Item['exwarehouseId']]
				isRepeat = [Item['isRepeat']]
				deliverywayId = [Item['deliverywayId']]
				sellerWeight = [Item['sellerWeight']]
				status = [Item['status']]
				postal = [Item['postal']]
				winitProductCode = [Item['winitProductCode']]
				documentNo = [Item['documentNo']]
				sku = [Item['sku']]

				df_dict = {
					'sellerOrderNo': sellerOrderNo,
					'countryName': countryName,
					'trackingNo': trackingNo,
					'weight': weight,
					'statusName': statusName,
					'eBaySellerID': eBaySellerID,
					'itemqty': itemqty,
					'estimateFees': estimateFees,
					'currency': currency,
					'winitTrackingNo': winitTrackingNo,
					'warehouseName': warehouseName,
					'winit_trackingno': winit_trackingno,
					'deliverywayName': deliverywayName,
					'volume': volume,
					'dateOrdered': dateOrdered,
					'dateFinish': dateFinish,
					'name': name,
					'exwarehouseId': exwarehouseId,
					'isRepeat': isRepeat,
					'deliverywayId': deliverywayId,
					'sellerWeight': sellerWeight,
					'status': status,
					'postal': postal,
					'winitProductCode': winitProductCode,
					'documentNo': documentNo,
					'sku': sku
				}
				# ******************************************
				logwinit.info('*' * 200)
				logwinit.info(sku)
				logwinit.info('*' * 200)
				SKU = []
				QTY = []
				if sku[0] is not None:
					for i in sku:
						result1 = i.split(',')
						for j in result1:
							result2 = j.split('@@')
							SKU.append(result2[0])
							QTY.append(result2[1])
					df_sku = pd.DataFrame({'sku':SKU, 'qty': QTY})
					df_sku['sellerOrderNo'] = sellerOrderNo[0]
					df_sku['trackingNo'] = trackingNo[0]
					df_sku['documentNo'] = documentNo[0]
					df_sku['warehouseName'] = warehouseName[0]
					df_sku['deliverywayName'] = deliverywayName[0]
					df_sku['dateOrdered'] = dateOrdered[0]
					df_sku['dateFinish'] = dateFinish[0]
					# with pd.option_context('display.max_rows', None, 'display.max_columns', None):
					# 	print(df_sku)
					# ZZZZPICH4YJSS09JH00
					write_to_outLibrary_sku(logwinit, df_sku, engine)
				else:
					continue
				# ******************************************
				df = pd.DataFrame(df_dict)
				df = df.drop('sku', axis = 1, inplace = False)
				write_to_outLibrary(logwinit, df, engine)
		else:
			sellerOrderNo = [i]
			countryName = [None]
			trackingNo = [None]
			weight = [None]
			statusName = [None]
			eBaySellerID = [None]
			itemqty = [None]
			estimateFees = [None]
			currency = [None]
			winitTrackingNo = [None]
			warehouseName = [None]
			winit_trackingno = [None]
			deliverywayName = [None]
			volume = [None]
			dateOrdered = [None]
			dateFinish = [None]
			name = [None]
			exwarehouseId = [None]
			isRepeat = [None]
			deliverywayId = [None]
			sellerWeight = [None]
			status = [None]
			postal = [None]
			winitProductCode = [None]
			documentNo = [None]
			sku = [None]
		# =============================================
			df_dict = {
				'sellerOrderNo': sellerOrderNo,
				'countryName': countryName,
				'trackingNo': trackingNo,
				'weight': weight,
				'statusName': statusName,
				'eBaySellerID': eBaySellerID,
				'itemqty': itemqty,
				'estimateFees': estimateFees,
				'currency': currency,
				'winitTrackingNo': winitTrackingNo,
				'warehouseName': warehouseName,
				'winit_trackingno': winit_trackingno,
				'deliverywayName': deliverywayName,
				'volume': volume,
				'dateOrdered': dateOrdered,
				'dateFinish': dateFinish,
				'name': name,
				'exwarehouseId': exwarehouseId,
				'isRepeat': isRepeat,
				'deliverywayId': deliverywayId,
				'sellerWeight': sellerWeight,
				'status': status,
				'postal': postal,
				'winitProductCode': winitProductCode,
				'documentNo': documentNo,
				'sku': sku
			}
			# ******************************************
			logwinit.info('*' * 200)
			logwinit.info(sku)
			logwinit.info('*' * 200)
			SKU = []
			QTY = []
			if sku[0] is not None:
				for i in sku:
					result1 = i.split(',')
					for j in result1:
						result2 = j.split('@@')
						SKU.append(result2[0])
						QTY.append(result2[1])
				df_sku = pd.DataFrame({'sku':SKU, 'qty': QTY})
				df_sku['sellerOrderNo'] = sellerOrderNo[0]
				df_sku['trackingNo'] = trackingNo[0]
				df_sku['documentNo'] = documentNo[0]
				df_sku['warehouseName'] = warehouseName[0]
				df_sku['deliverywayName'] = deliverywayName[0]
				df_sku['dateOrdered'] = dateOrdered[0]
				df_sku['dateFinish'] = dateFinish[0]
				write_to_outLibrary_sku(logwinit, df_sku, engine)
			else:
				continue
			# ******************************************
			df = pd.DataFrame(df_dict)
			df = df.drop('sku', axis = 1, inplace = False)
			write_to_outLibrary(logwinit, df, engine)

def write_to_outLibrary(logwinit, df, engine):
	pd.io.sql.to_sql(df,
					 name = 'WINIT_outLibrary_order',
					 con = engine,
					 if_exists = 'append',
					 index = False)
	logwinit.info('============ 当前查询数据已成功写入数据库表：WINIT_outLibrary_order!')

def write_to_outLibrary_sku(logwinit, df, engine):
	pd.io.sql.to_sql(df,
					 name = 'WINIT_outLibrary_order_sku',
					 con = engine,
					 if_exists = 'append',
					 index = False)
	logwinit.info('当前查询数据已成功写入数据库表：WINIT_outLibrary_order_sku!')


def main():
	start = time.time()
	
	path = "D:/logfiles/outlibrary/winit/logwinit%s.log" % datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
	logwinit = Logger.Logger(path = path)
	
	# connect the database
	conn = pymssql.connect(host = "192.168.1.252",
						   port = "5678",
						   user = "tom.dong",
						   password = "oig123456",
						   database = "wangzf",
						   charset = "utf8")
	cur = conn.cursor()
	logwinit.info("连接数据库成功!")
	
	# delete data
	delete_repeat(conn, cur)
	logwinit.info("已对wangzf.dbo.WINIT_outLibrary_order_sku进行去重处理...")
	preAction(conn, cur)
	delete_no_use(conn, cur)
	logwinit.info("已删除wangzf.dbo.WINIT_outLibrary_order_sku中的未出库订单...")
	
	# datetime parameters
	dateOrderedStartDate = (datetime.now() - timedelta(weeks = 3)).strftime('%Y-%m-%d')
	dateOrderedEndDate = datetime.now().strftime('%Y-%m-%d')
	
	# db engine
	engine = get_engine()

	# 下载AU, US 仓库数据
	obOrderNum_au_us = get_au_us_sellerOrderNo(cur)
	logwinit.info('查询 %s 个AU,US仓库卖家订单号' % len(obOrderNum_au_us))
	parse_json(logwinit, obOrderNum_au_us, dateOrderedStartDate, dateOrderedEndDate, engine)
	logwinit.info('*' * 150)
	logwinit.info('AU,US仓库数据已成功写入数据库！')
	logwinit.info('*' * 150)
	time.sleep(10)

	# 下载UK, DE, 
	obOrderNum_uk_de = get_uk_de_sellerOrderNo(cur)
	logwinit.info('查询 %s 个UK,DE仓库卖家订单号' % len(obOrderNum_uk_de))
	parse_json(logwinit, obOrderNum_uk_de, dateOrderedStartDate, dateOrderedEndDate, engine)
	logwinit.info('*' * 150)
	logwinit.info('UK,DE仓库数据已成功写入数据库！')
	logwinit.info('*' * 150)

	# 关闭数据库链接，关闭游标
	cur.close()
	conn.close()
	logwinit.info("已关闭数据库!")
	cost = time.time() - start
	logwinit.info("程序运行时间：%s" % cost)


if __name__ == "__main__":
	main()