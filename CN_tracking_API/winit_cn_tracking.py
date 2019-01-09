#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime
import hashlib
import requests
import json
import pymssql
import pandas as pd
from sqlalchemy import create_engine

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



def delete_repeat_data(conn, cur):
	sql = '''DELETE FROM wangzf.dbo.winit_cn_order_ascan_dscan
			 WHERE id NOT IN (
			 	SELECT MIN(B.id)
			 	FROM wangzf.dbo.winit_cn_order_ascan_dscan B
			 	GROUP BY 
			       [date]
			       ,[eventCode]
			       ,[eventDescription]
			       ,[eventStatus]
			       ,[lastEvent]
			       ,[location]
			       ,[operator]
			       ,[trackingType]
			       ,[type]
			       ,[orderNo]
			       ,[trackingNo]
			       ,[origin]
			       ,[destination]
			       ,[pickupMode]
			       ,[status]
			       ,[vendorName]
			       ,[occurTime]
			       ,[logisticsStatus]
			       ,[logisticsMess]
			       ,[airLines]
			       ,[flight]
			       ,[expressCompany]
			       ,[carrier]
			       ,[carrierCode]
			       ,[standardCarrier]
			       ,[trackingUrl]
			       ,[isTracked]
			 ) '''
	cur.execute(sql)
	conn.commit()
	print('============ 已删除重复数据 ==============')


def delete_no_useData(conn, cur):
	sql = '''DELETE FROM wangzf.dbo.winit_cn_order_ascan_dscan
			 WHERE date IS NULL '''
	cur.execute(sql)
	conn.commit()
	print('============ 已删除无上网时间数据 ==============')


# 查询无上网和妥投时间的跟踪单号
def get_trackingNo(cur):
	sql = '''SELECT DISTINCT B.TrackNo
			 FROM (
			  	SELECT DISTINCT A.TrackNo TrackNo
			  	FROM dbo.last_three_month_cn_trackNo_api A
			  	WHERE 
			   		(A.name LIKE '%万邑邮选%' OR A.name LIKE '%万邑通%') AND
			   		A.printed = 'y'
			  	UNION
			  	SELECT DISTINCT A.跟踪单号 TrackNo
			  	FROM Skyeye.dbo.Tail_Leg_Ebay_Account A
			  	WHERE 
			  		A.发货仓 = 'CN' AND
			  		A.打印状态 = 'y' AND 
			  		(A.最终使用服务方式 LIKE '%万邑邮选%' OR A.最终使用服务方式 LIKE '%万邑通%')
			  ) B
			 WHERE 
			  	B.TrackNo IS NOT NULL AND 
			  	B.TrackNo != '' AND 
			  	B.TrackNo != ' ' AND
			  	PATINDEX('%[^A-Z^a-z^0-9]%', B.TrackNo) = 0 AND
			  	B.TrackNo NOT IN (SELECT DISTINCT trackingNo
			    				  FROM dbo.winit_cn_order_ascan_dscan) '''
	cur.execute(sql)
	result = cur.fetchall()
	return result

# ======================================================================================================================
def md5(str):
	m = hashlib.md5()
	m.update(str.encode('utf-8'))
	return m.hexdigest().upper()


def get_trackTrace(trackingNOs):
	token = "20BC45092988B9AA6D740514060530C0"
	actionValue = "tracking.getOrderTracking"
	app_keyValue = "ken@oigbuy.com"
	dataValue = '{"trackingNOs":"' + trackingNOs + '"}'
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
	# print(sign_str)
	sign = md5(sign_str)
	# url
	url_outsea = "http://openapi.winit.com.cn/openapi/service"
	# headers
	headers = {"Authorization": "Bearer " + token,
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
				"trackingNOs": trackingNOs
			}
		})
	# request
	response = requests.post(url = url_outsea, data = params, headers = headers)
	print("数据请求成功...")
	js = json.loads(response.text)
	print("数据解析成功...")
	return js

# ========================================================================================
def get_trace_df(js_data):
	data = js_data['data']
	if data != []:
		orderNo = data[0]['orderNo']
		trackingNo = data[0]['trackingNo']
		origin = data[0]['origin']
		destination = data[0]['destination']
		pickupMode = data[0]['pickupMode']
		status = data[0]['status']
		vendorName = data[0]['vendorName']
		occurTime = data[0]['occurTime']
		logisticsStatus = data[0]['logisticsStatus']
		logisticsMess = data[0]['logisticsMess']
		airLines = data[0]['airLines']
		flight = data[0]['flight']
		expressCompany= data[0]['expressCompany']
		carrier = data[0]['carrier']
		carrierCode = data[0]['carrierCode']
		standardCarrier = data[0]['standardCarrier']
		trackingUrl = data[0]['trackingUrl']
		isTracked = data[0]['isTracked']
		# ------------------------------------
		trace = data[0]['trace']
		trace_df = pd.DataFrame(trace)
		# ------------------------------------
		trace_df['insertDate'] = datetime.now()
		trace_df['orderNo'] = orderNo
		trace_df['trackingNo'] = trackingNo
		trace_df['origin'] = origin
		trace_df['destination'] = destination
		trace_df['pickupMode'] = pickupMode
		trace_df['status'] = status
		trace_df['vendorName'] = vendorName
		trace_df['occurTime'] = occurTime
		trace_df['logisticsStatus'] = logisticsStatus
		trace_df['logisticsMess'] = logisticsMess
		trace_df['airLines'] = airLines
		trace_df['flight'] = flight
		trace_df['expressCompany'] = expressCompany
		trace_df['carrier'] = carrier
		trace_df['carrierCode'] = carrierCode
		trace_df['standardCarrier'] = standardCarrier
		trace_df['trackingUrl'] = trackingUrl
		trace_df['isTracked'] = isTracked
		return trace_df


def write_to_db(df, engine):
	try:
		pd.io.sql.to_sql(df,
	                     name = 'winit_cn_order_ascan_dscan',
	                     con = engine,
	                     if_exists = 'append',
	                     index = False)
		print('数据成功写入DataBase!')
	except:
		print('数据写入DataBase时出现错误！')

def get_write_df(conn, cur, engine):
	delete_repeat_data(conn, cur)
	delete_no_useData(conn, cur)
	trackNo = get_trackingNo(cur)
	print('############################################################')
	print('============ 查询%s个跟踪单号：=============' % len(trackNo))
	print('############################################################')
	trackNo = list(trackNo)
	for i in trackNo:
		print('================ 查询的跟踪单号为: %s ==================' % i[0])
		js = get_trackTrace(i[0])
		print('------------ 请求到的数据：-------------')
		print(js)
		df = get_trace_df(js)
		df = df[df.eventCode.isin(['AS'])]
		print('============= Result DF:=============')
		print(df)
		write_to_db(df, engine)



# =====================================================================================
# 主函数
def main():
	conn = pymssql.connect(host = '192.168.1.252',
						   port = '5678',
						   user = 'tom.dong',
						   password = 'oig123456',
						   database = 'wangzf',
						   charset = 'utf8')
	cur = conn.cursor()
	engine = get_engine()
	get_write_df(conn, cur, engine)
	cur.close()
	conn.close()
		
# ======================================================================================
if __name__ == "__main__":
	main()