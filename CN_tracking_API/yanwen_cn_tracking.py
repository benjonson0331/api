#!/usr/bin/env python
# -*- coding: utf-8 -*-


import requests
import pymssql
from sqlalchemy import create_engine
import json
import pandas as pd
from datetime import datetime

def get_engine():
	db_info = {
		"host": "192.168.1.252",
		"port": "5678",
		"user": "tom.dong",
		"password": "oig123456",
		"database": "wangzf"
	}  
	conn_info = "mssql+pymssql://%(user)s:%(password)s@%(host)s:%(port)s/%(database)s" % db_info
	engine = create_engine(conn_info, encoding = "utf-8")
	return engine

def write_to_yanwen(df, engine):
	pd.io.sql.to_sql(df,
					 name = "yanwen_order_ascan_dscan",
					 con = engine, 
					 if_exists = "append",
					 index = False)

def delete_repeat(conn, cur):
	sql = """DELETE FROM dbo.yanwen_order_ascan_dscan
			 WHERE id NOT IN (
			 	SELECT MIN(A.id)
			 	FROM dbo.yanwen_order_ascan_dscan A
			 	GROUP BY A.location, A.message, A.region, A.send_date, A.timestamp, A.tracking_number) """
	cur.execute(sql)
	conn.commit()
	print('=========== 已删除重复数据 ============')

def delete_no_use_data(conn, cur):
	sql = """delete from wangzf.dbo.cn_ascan_dscan
	  		 where 
	  		 	a.postService = 'yanwen' and 
	  		 	(a.ascan is null or a.dscan is null) and
	  		 	a.trackNo not in ( 
	  		 		SELECT DISTINCT C.TrackNo
	  		 		FROM (
	  		             SELECT DISTINCT TrackNo
	  		             FROM dbo.last_three_month_cn_trackNo_api A
	  		             WHERE 
	  		                 A.name LIKE '%燕文%' AND 
	  		                 A.name LIKE '%平邮%' AND
	  		                 A.printed = 'y' 
	  		             UNION
	  		             SELECT DISTINCT A.跟踪单号 TrackNo
	  		             FROM Skyeye.dbo.Tail_Leg_Ebay_Account A
	  		             WHERE 
	  		                 A.发货仓 = 'CN' AND 
	  		                 A.打印状态 = 'y' AND
	  		                 A.最终使用服务方式 LIKE '%燕文%' AND 
	  		                 A.最终使用服务方式 LIKE '%平邮%'
	  		 		 ) C
	  		 		WHERE 
	  		 			C.TrackNo != '' 
	  		 			AND C.TrackNo != ' ' 
	  		 			AND C.TrackNo IS NOT NULL 
	  		 			AND PATINDEX('%[^A-Z^a-z^0-9]%', C.TrackNo) = 0
	  		 ) """
	cur.execute(sql)
	conn.commit()
	print('=========== 已删除无上网时间的数据 ============')

def get_trackingNo(cur):
	sql = '''SELECT DISTINCT A.trackNo TrackNo
			 FROM dbo.cn_ascan_dscan A
			 INNER JOIN (
			 	SELECT DISTINCT TrackNo
			 	FROM dbo.last_three_month_cn_trackNo_api A
			 	WHERE 
			 		A.name LIKE '%燕文%' AND 
			 		A.name NOT LIKE '%平邮%' AND
			 		A.printed = 'y' 
			 	UNION
			 	SELECT DISTINCT A.跟踪单号 TrackNo
			 	FROM Skyeye.dbo.Tail_Leg_Ebay_Account A
			 	WHERE 
			 		A.发货仓 = 'CN' AND 
			 		A.打印状态 = 'y' AND
			 		A.最终使用服务方式 LIKE '%燕文%' AND 
			 		A.最终使用服务方式 NOT LIKE '%平邮%'
			 	UNION
			 	SELECT DISTINCT TrackNo
			 	FROM Skyeye.dbo.Tail_Leg_Wish_CN_Account_Data A
			 	WHERE A.FilterFlag >= 100 AND
			 		A.PostageService LIKE '%燕文%' AND
			 		A.PostageService NOT LIKE '%平邮%'
			 ) B ON A.TrackNo = B.TrackNo
			 WHERE 
			 	A.postService = 'yanwen' AND 
			 	(A.ascan is null or A.dscan is null)
			 UNION
			 SELECT DISTINCT C.TrackNo
			 FROM (
			 	SELECT DISTINCT TrackNo
			 	FROM dbo.last_three_month_cn_trackNo_api A
			 	WHERE 
			 		A.name LIKE '%燕文%' AND
			 		A.printed = 'y' 
			 	UNION
			 	SELECT DISTINCT A.跟踪单号 TrackNo
			 	FROM Skyeye.dbo.Tail_Leg_Ebay_Account A
			 	WHERE 
			 		A.发货仓 = 'CN' AND 
			 		A.打印状态 = 'y' AND
			 		A.最终使用服务方式 LIKE '%燕文%'
			 	UNION 
			 	SELECT DISTINCT TrackNo
			 	FROM Skyeye.dbo.Tail_Leg_Wish_CN_Account_Data A
			 	WHERE 
			 		A.FilterFlag >= 100 AND
			 		A.PostageService LIKE '%燕文%'
			 ) C
			 WHERE 
			 	C.TrackNo NOT IN (SELECT DISTINCT tracking_number
			 					  FROM dbo.yanwen_order_ascan_dscan)
			 	AND C.TrackNo != '' 
			 	AND C.TrackNo != ' ' 
			 	AND C.TrackNo IS NOT NULL 
			 	AND PATINDEX('%[^A-Z^a-z^0-9]%', C.TrackNo) = 0 '''
	cur.execute(sql)
	result = cur.fetchall()
	return result

def parse_data(cur, engine):
	trackingNo = get_trackingNo(cur)
	print("======================================================")
	print("== 查询%s条跟踪单号" % len(trackingNo))
	print("======================================================")
	# trackingNo = ["UC063173435MY"]
	for i in trackingNo:
		print('====== 查询的跟踪单号为:%s =======' % i[0].strip())
		url = "http://api.track.yw56.com.cn/v1/tracking/%s" % i[0].strip()
		response = requests.get(url = url)
		if response.status_code == 200:
			js = json.loads(response.text)
			print('请求得到的数据：')
			print(js)
			tracking_number = js['tracking_number']
			region = js['region']
			send_date = js['send_date']
			details_orig = js['details_orig']
			df = pd.DataFrame(details_orig)
			df['insertDate'] = datetime.now()
			df['tracking_number'] = tracking_number
			df['region'] = region
			df['send_date'] = send_date
			print('================= Data Frame: =================')
			print(df)
			try:
				write_to_yanwen(df, engine)
				print("-------------------------------")
				print('数据正确写入数据库')
			except:
				print("=======================")
				print("Error:无数据写入数据库.")
				print("=======================")
		else:
			continue

def main():
	conn = pymssql.connect(host = "192.168.1.252",
						   port = "5678",
						   user = "tom.dong",
						   password = "oig123456",
						   database = "wangzf",
						   charset = "utf8")
	cur = conn.cursor()
	engine = get_engine()
	delete_repeat(conn, cur)
	parse_data(cur, engine)
	delete_no_use_data(conn, cur)
	cur.close()
	conn.close()

if __name__ == "__main__":
	main()


