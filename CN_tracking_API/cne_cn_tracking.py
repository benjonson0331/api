#!/usr/bin/env python
# -*- coding: utf-8 -*-


# --------------------------------------------------------------
# def md5(str):
# 	m = hashlib.md5()
# 	m.update(str.encode('utf-8'))
# 	return m.hexdigest().upper()
# RequestName_value = "EmsApiTrack"
# icID_value = "16926"
# token = "rAOkxwCs125lPIJ"
# TimeStamp_value = int(round(datetime.now().timestamp() * 1000))
# sign_str = icID_value + str(TimeStamp_value) + token
# MD5_value = md5(sign_str)
# data = json.dumps({
# 	"RequestName": RequestName_value,
# 	"icID": icID_value,	
# 	"TimeStamp": TimeStamp_value,
# 	"MD5": MD5_value
# })
# --------------------------------------------------------------

import json
import pymssql
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

def get_engine():
	db_info = {
		'host': '192.168.1.252',
		'port': '5678',
		'user': 'tom.dong',
		'password': 'oig123456',
		'database': 'wangzf'
	}
	conn_info = 'mssql+pymssql://%(user)s:%(password)s@%(host)s:%(port)s/%(database)s' % db_info
	engine = create_engine(conn_info, encoding ='utf-8')
	return engine


def delete_repeat_data(conn, cur):
	sql = '''DELETE FROM dbo.cne_order_ascan_dscan
			 WHERE id NOT IN (
			 	SELECT MIN(A.id)
			 	FROM dbo.cne_order_ascan_dscan A
			 	GROUP BY [trackingNbr], [EmsKind], [Number_tt], [pickupDate]
			 	   , [From_p], [Destination], [referNbr], [Receiver], [RPhone]
			 	   , [irid], [totalPieces], [totalWeigt], [status], [deliveryDate]
			 	   , [signature], [date], [place], [details]
			 ) '''
	cur.execute(sql)
	conn.commit()
	print('已删除重复数据...')


def get_trackNo(cur):
	sql = '''SELECT DISTINCT C.TrackNo TrackNo
			 FROM (
			   	SELECT DISTINCT TrackNo
			   	FROM dbo.last_three_month_cn_trackNo_api A
			   	WHERE 
			   		A.name LIKE '%CNE%' AND
			   		A.printed = 'y' 
			   	UNION
			   	SELECT DISTINCT A.跟踪单号 TrackNo
			   	FROM Skyeye.dbo.Tail_Leg_Ebay_Account A
			   	WHERE 
			   		A.发货仓 = 'CN' AND 
			   		A.打印状态 = 'y' AND
			   		A.最终使用服务方式 LIKE '%CNE%'
			   	UNION 
				SELECT DISTINCT A.trackNo TrackNo
				FROM Skyeye.dbo.Tail_Leg_Wish_CN_Account_Data A
				WHERE A.PostageService LIKE '%CNE%'
			 ) C
			 WHERE C.TrackNo NOT IN (SELECT DISTINCT trackingNbr
			    					 FROM dbo.cne_order_ascan_dscan)
			       AND C.TrackNo != ''
			       AND C.TrackNo != ' ' 
			       AND C.TrackNo IS NOT NULL 
			 UNION
			 SELECT DISTINCT a.trackNo TrackNo
			 FROM dbo.cn_ascan_dscan A
			 WHERE 
			 	A.postService = 'cne' AND 
			 	(A.ascan IS NULL OR A.dscan IS NULL) '''
	cur.execute(sql)
	result = cur.fetchall()
	return result

def delete_no_use_data(conn, cur):
	sql = '''delete from dbo.cn_ascan_dscan
			 where 
			 	postService = 'cne' and 
				(ascan is null or dscan is null) '''
	cur.execute(sql)
	conn.commit()
	print('=========== 已删除无ascan,dscan的数据 ============')

def write_db(df, engine):
	pd.io.sql.to_sql(df,
					 name = "cne_order_ascan_dscan",
					 con = engine,
					 if_exists = "append",
					 index = False)

def parse_data(trackNo, engine):
	for i in trackNo:
		print("============ 查询的跟踪单号是%s =============" % i)
		url = "http://trackapi.cnexps.com/cgi-bin/GInfo.dll?EmsApiTrack"
		params = {
			"cno": i,
			"ntype": "10101"
			# "cp": "65001"
		}
		response = requests.get(url = url, params = params)
		if response.status_code == 200:
			data = json.loads(response.text)
			print("============ 请求得到的数据为: ==============")
			print(data)
		else:
			continue
		ReturnValue = data["ReturnValue"]
		if ReturnValue == "100":
			Response_Info = data["Response_Info"]
			trackingNbr = Response_Info["trackingNbr"]
			EmsKind = Response_Info["EmsKind"]
			Number_tt = Response_Info["Number_tt"]
			pickupDate = Response_Info["pickupDate"]
			From = Response_Info["From"]
			Destination = Response_Info["Destination"]
			if "referNbr" not in Response_Info.keys():
				referNbr = None
			else:	
				referNbr = Response_Info["referNbr"]
			Receiver = Response_Info["Receiver"]
			RPhone = Response_Info["RPhone"]
			irid = Response_Info["irid"]
			totalPieces = Response_Info["totalPieces"]
			totalWeigt = Response_Info["totalWeigt"]
			status = Response_Info["status"]
			deliveryDate = Response_Info["deliveryDate"]
			signature = Response_Info["signature"]
			# -------------------
			trackingEventList = data["trackingEventList"]
			track_df = pd.DataFrame(trackingEventList)
			# -------------------
			track_df['insertDate'] = datetime.now()
			track_df['trackingNbr'] = trackingNbr
			track_df['EmsKind'] = EmsKind
			track_df['Number_tt'] = Number_tt
			track_df['pickupDate'] = pickupDate
			track_df['From_p'] = From
			track_df['Destination'] = Destination
			track_df['referNbr'] = referNbr
			track_df['Receiver'] = Receiver
			track_df['RPhone'] = RPhone
			track_df['irid'] = irid
			track_df['totalPieces'] = totalPieces
			track_df['totalWeigt'] = totalWeigt
			track_df['status'] = status
			track_df['deliveryDate'] = deliveryDate
			track_df['signature'] = signature
			print("========================== 解析得到的数据表为: ==========================")
			print(track_df)
			try:
				write_db(track_df, engine)
				print("-------------------------------")
				print('数据正确写入数据库')
			except NotImplementedError as e:
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
	delete_repeat_data(conn, cur)
	trackingNo = get_trackNo(cur)
	engine = get_engine()
	print('============ 查询%s个跟踪单号 ==============' % len(trackingNo))
	parse_data(trackingNo, engine)
	# delete_no_use_data()
	cur.close()
	conn.close()

if __name__ == "__main__":
	main()