#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pymssql
import requests
import json
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import Logger
import time


def get_engine():
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
	sql = """DELETE FROM dbo.fpx_dhl_outsea_order_ascan_dscan 
			 WHERE id NOT IN (
							  SELECT MIN(A.id)
							  FROM dbo.fpx_dhl_outsea_order_ascan_dscan A
							  GROUP BY 
							       [occurAddress]
							       ,[occurDate]
							       ,[trackCode]
							       ,[trackContent]
							       ,[trackNo]) """
	try:
		cur.execute(sql)
		conn.commit()
	except:
		conn.rollback()


def get_trackNo(cur):
	sql = '''SELECT DISTINCT 跟踪单号
			 FROM Skyeye.dbo.Tail_Leg_Ebay_Account A
			 WHERE  
			 	A.发货仓 = 'UK4PX' AND
			 	A.最终使用服务方式 = 'LDHL' AND 
			 	A.跟踪单号 IS NOT NULL AND
			 	A.跟踪单号 != '' AND
			 	A.跟踪单号 != ' ' AND
			 	A.跟踪单号 NOT IN (SELECT DISTINCT A.trackNo
			 					   FROM dbo.fpx_dhl_outsea_order_ascan_dscan A)
			 union
			 select distinct a.trackNo
			 from Skyeye.dbo.Tail_Leg_Wish_Outsea_Account a
			 where 
			 	a.whCode = 'UK4PX' and 
			 	a.PostageService = 'LDHL' and 
			 	a.trackNo is not null and 
			 	a.trackNo != '' and
			 	a.trackNo != ' ' and
			 	a.trackNo not in (select distinct a.trackNo
			 					  from dbo.fpx_dhl_outsea_order_ascan_dscan a)
			 union 
			 select distinct a.trackNo
			 from dbo.fpx_ascan_dscan a
			 where a.ascan is null or a.dscan is null '''
	cur.execute(sql)
	result = cur.fetchall()
	return result

def delete_no_use_data(conn, cur):
	sql =  """ delete from wangzf.dbo.fpx_ascan_dscan
			   where ascan is null or dscan is null """
	cur.execute(sql)
	conn.commit()

def parse_json(js, code):
	data = js['data']
	df = pd.DataFrame(data)
	df['trackNo'] = code
	df['insertDate'] = datetime.now()
	return df

def write_to_db(log4px_dhl, df, engine):
	pd.io.sql.to_sql(df,
					 name = 'fpx_dhl_outsea_order_ascan_dscan',
					 con = engine,
					 if_exists = "append",
					 index = False)
	log4px_dhl.info("数据已成功写入DataBase！")


def GetTrackList(log4px_dhl, d, engine):
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
	url = "http://openapi.4px.com/api/service/woms/receiving/getTrackList"
	for code in d:
		log4px_dhl.info("查询的跟踪单号为:%s" % code[0])
		data = json.dumps({
			"receivingCode": "%s"
		}) % code[0]
		response = requests.post(url = url, params = params, data = data, headers = headers)
		if response.status_code == 200:
			js = json.loads(response.text)
			log4px_dhl.info("请求得到的数据为: %s" % js)
			if js['data'] is not None:
				df = parse_json(js, code[0])
				# df = df[df['trackContent'].isin(["Shipment delivered.", "Shipment information received"])]
				write_to_db(log4px_dhl, df, engine)
			else:
				continue
		else:
			continue

def main():
	start = time.time()
	path = "D:/logfiles/oversea_tracking/4px_dhl/log4px_dhl%s.log" % datetime.now().strftime('%Y-%m-%d')
	log4px_dhl = Logger.Logger(path = path)
	conn = pymssql.connect(host = "192.168.1.252",
						   port = "5678",
						   user = "tom.dong",
						   password = "oig123456",
						   database = "wangzf",
						   charset = "utf8")
	cur = conn.cursor()
	log4px_dhl.info("连接数据库成功！")
	delete_repeat(conn, cur)
	log4px_dhl.info("去重操作完成！")
	d = get_trackNo(cur)
	# delete_no_use_data()
	engine = get_engine()
	log4px_dhl.info('*' * 150)
	log4px_dhl.info('查询%s条DHL跟踪单号' % len(d))
	log4px_dhl.info('*' * 150)
	GetTrackList(log4px_dhl, d, engine)
	cur.close()
	conn.close()
	log4px_dhl.info("关闭数据库！")
	cost = time.time() - start
	log4px_dhl.info("程序运行时间为：%s" % cost)

if __name__ == "__main__":
	main()
