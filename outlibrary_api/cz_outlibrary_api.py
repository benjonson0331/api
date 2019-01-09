#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import json
import pandas as pd
import xml.dom.minidom
import pymssql
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import time
import Logger

# ==================================================================
# 测试库
# appToken: a996f0a95c9939413d965647958f44bf
# appKey: 71feab23d0c98b8b03f91a9f6f9f3510
# 正式库
# appToken: 67aef66dc9a942ca3de480fd28e46db4
# appKey: ab7b4f1b0d616ca8836fecd0589c672c
# ==================================================================

def delete_repeate(conn, cur):
	sql = '''DELETE FROM dbo.cz_outLibrary_order
			 WHERE id NOT IN (
			 	SELECT MIN(A.id)
			 	FROM dbo.cz_outLibrary_order A
			 	GROUP BY
			       [order_code]
			       ,[reference_no]
			       ,[platform_code]
			       ,[order_status]
			       ,[shipping_method]
			       ,[tracking_no]
			       ,[warehouse_code]
			       ,[order_weight]
			       ,[order_desc]
			       ,[date_create]
			       ,[date_release]
			       ,[date_shipping]
			       ,[date_modify]
			       ,[consignee_country_code]
			       ,[consignee_country_name]
			       ,[consignee_state]
			       ,[consignee_city]) '''
	cur.execute(sql)
	conn.commit()


def preAction(conn, cur):
	sql = """
		  IF OBJECT_ID('TEMPDB..#res') IS NOT NULL BEGIN DROP TABLE #res END
		  SELECT 
		  	DISTINCT 
		  	A.parcel_id,
		  	DATEADD(HH, -7, A.printDate) printDate,
		  	A.my_PostageService,
		  	DATEADD(HH, -7, CASE WHEN B.date_shipping = 'null' THEN '1900-01-01 00:00:00' 
		  						 ELSE convert(datetime, B.date_shipping)
		  					END) date_shipping
		  	,A.printed
		  INTO #res
		  FROM [192.168.1.220].CenterDB.dbo.parcelPrint A
		  LEFT JOIN dbo.CZ_outLibrary_order B
		  	ON A.parcel_id = B.reference_no
		  WHERE 
		  	A.printDate >= CONVERT(VARCHAR(10), DATEADD(DD, -8, GETDATE()), 21)
		  	AND A.printDate < CONVERT(VARCHAR(10), GETDATE(), 21)
		  	AND A.wh_code = 'CZRYKD'

		  TRUNCATE TABLE dbo.cz_used_referenceCode
		  INSERT INTO dbo.cz_used_referenceCode
		  SELECT DISTINCT A.parcel_id
		  FROM (
		  	SELECT 
		  	DISTINCT 
		  	parcel_id
		  FROM #RES A
		  WHERE 
		  	A.printed = 'y' AND
		  	(A.date_shipping IS NULL OR 
		  	A.date_shipping = '' OR 
		  	A.date_shipping <= '1901-01-01 00:00:00.000' OR 
		  	DATEDIFF(DD, A.printDate, A.date_shipping) < 0)
		  UNION 
		  SELECT DISTINCT reference_no parcel_id
		  FROM dbo.CZ_outLibrary_order A
		  WHERE 
		  	A.date_shipping IS NULL OR 
		  	A.date_shipping = '' OR 
		  	A.date_shipping <= '1901-01-01 00:00:00.000'
		  ) A
		   """
	cur.execute(sql)
	conn.commit()


def delete_no_use(conn, cur):
	sql = """DELETE FROM dbo.CZ_outLibrary_order
			 WHERE reference_no IN (SELECT DISTINCT parcel_id FROM dbo.cz_used_referenceCode) """
	cur.execute(sql)
	conn.commit()


def get_reference_code(cur):
	sql = """
		  SELECT DISTINCT parcel_id
		  FROM dbo.cz_used_referenceCode """
	cur.execute(sql)
	ref_code = cur.fetchall()	
	return ref_code


def getOrderByRefCode(logcz, refer_code):
	try:
		for code in refer_code:
			# data = get_parse(logcz, code)
			param = '''<?xml version="1.0" encoding="UTF-8"?>
			  <SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="http://www.example.org/Ec/">
			    	<SOAP-ENV:Body>
			    	    <ns1:callService>
			    	        <paramsJson>{"reference_no":"%s"}</paramsJson>
			    	        <appToken>67aef66dc9a942ca3de480fd28e46db4</appToken>
			    	        <appKey>ab7b4f1b0d616ca8836fecd0589c672c</appKey>
			    	        <service>getOrderByRefCode</service>
			    	    </ns1:callService>
			    	</SOAP-ENV:Body>
			  </SOAP-ENV:Envelope>''' % code[0]
			logcz.info('查询的卖家订单号: %s' % code[0])
			url = "http://121.40.249.183/default/svc/web-service"
			headers = {
				'Content-Type': 'text/xml'
			}
			r = requests.post(url = url, data = param, headers = headers)
			result = r.text
			xml_parse = xml.dom.minidom.parseString(result)
			xml_doc = xml_parse.documentElement
			response = xml_doc.getElementsByTagName("response")
			if response != []:
				response = response[0]
				response_str = response.childNodes[0].data
				response_dict = json.loads(response_str)
				data = response_dict['data']
				logcz.info('下载的原始数据: %s' % str(data).encode(encoding = "UTF-8"))
			else:
				continue
			if data is not None:
				order_code = [data['order_code']]
				reference_no = [data['reference_no']]
				platform_code = [data['platform']]
				order_status = [data['order_status']]
				shipping_method = [data['shipping_method']]
				tracking_no = [data['tracking_no']]
				warehouse_code = [data['warehouse_code']]
				order_weight = [data['order_weight']]
				order_desc = [data['order_desc']]
				date_create = [data['date_create']]
				if date_create == 'null':
					date_create = ['1900-01-01 00:00:00']
				else:
					date_create = [data['date_create']]
				date_release = [data['date_release']]
				if date_release == 'null':
					date_release = ['1900-01-01 00:00:00']
				else:
					date_release = [data['date_release']]
				date_shipping = [data['date_shipping']]
				if date_shipping == 'null':
					date_shipping = ['1900-01-01 00:00:00']
				else:
					date_shipping = [data['date_shipping']]
				date_modify = [data['date_modify']]
				if date_modify == 'null':
					date_modify = ['1900-01-01 00:00:00']
				else:
					date_modify = [data['date_modify']]
				consignee_country_code = [data['consignee_country_code']]
				consignee_country_name = [data['consignee_country_name']]
				consignee_state = [data['consignee_state']]
				consignee_city = [data['consignee_city']]
				insertDate = [datetime.now()]
				data_dict = {
					'order_code': order_code,
					'reference_no': reference_no,
					'platform_code': platform_code,
					'order_status': order_status,
					'shipping_method': shipping_method,
					'tracking_no': tracking_no,
					'warehouse_code': warehouse_code,
					'order_weight': order_weight,
					'order_desc': order_desc,
					'date_create': date_create,
					'date_release': date_release,
					'date_shipping': date_shipping,
					'date_modify': date_modify,
					'consignee_country_code': consignee_country_code,
					'consignee_country_name': consignee_country_name,
					'consignee_state': consignee_state,
					'consignee_city': consignee_city,
					'insertDate': insertDate
				}
				df = pd.DataFrame(data_dict)
				try:
					engine = get_engine()
					write_db(engine, df)
					logcz.info('数据正确写入数据库...')
				except NotImplementedError as e:
					logcz.error('Error:无数据写入数据库...')	
			else:
				continue
	except TypeError as e:
		logcz.error("Error:存在未出库订单.")

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


def write_db(engine, df):
	pd.io.sql.to_sql(df, 
					 name = 'cz_outLibrary_order',
	                 con = engine,
				 	 if_exists = 'append',
				     index = False)


def main():
	start = time.time()
	path = "D:/logfiles/outlibrary/cz/logcz%s.log" % datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
	logcz = Logger.Logger(path = path)
	# -------------------------------------------------
	conn = pymssql.connect(host = "192.168.1.252",
						   port = "5678",
						   user = "tom.dong",
						   password = "oig123456",
						   database = "wangzf",
						   charset = "utf8")
	cur = conn.cursor()
	logcz.info("数据库wangzf连接成功...")
	preAction(conn, cur)
	delete_no_use(conn, cur)
	delete_repeate(conn, cur)
	refcode = get_reference_code(cur)
	logcz.info("*" * 150)
	logcz.info('查询订单数 %s:' % str(len(refcode)))
	logcz.info("*" * 150)
	getOrderByRefCode(logcz, refer_code = refcode)
	cur.close()
	conn.close()
	logcz.info("关闭数据库连接...")
	# -------------------------------------------------
	end = time.time()
	cost_time = end - start
	logcz.info("运行时间：%s" % cost_time)


if __name__ == "__main__":
	main()