#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime
import hashlib
import requests
import json
import pymssql
import pandas as pd
from sqlalchemy import create_engine
import Logger
import time

# 基础参数===============================================
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
# =======================================================

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
	sql = """DELETE FROM dbo.winit_outsea_order_ascan_dscan
     		 WHERE id NOT IN (
     		 	SELECT MIN(A.id)
     		 	FROM dbo.winit_outsea_order_ascan_dscan A
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
     		       ,[isTracked]) """
	cur.execute(sql)
	conn.commit()

def get_trackingNo(cur):
	sql = '''IF OBJECT_ID('TEMPDB..#winit_ebay') IS NOT NULL BEGIN DROP TABLE #winit_ebay END
			 SELECT DISTINCT 跟踪单号
			 INTO #winit_ebay
			 FROM Skyeye.dbo.Tail_Leg_Ebay_Account A
			 WHERE 
			 	A.打印状态 = 'y' AND
			 	A.发货仓 IN ('AUWINIT', 'UKWINIT', 'DEWINIT', 'USWCWINIT', 'USTXWINIT', 'USKYNWINIT') AND
			 	A.最终使用服务方式 NOT LIKE '%Untracked%' AND 
			 	A.妥投时间 IS NULL AND 
			 	A.跟踪单号 IS NOT NULL AND
			 	A.最终使用服务方式 LIKE '%DHL%'
			 if OBJECT_ID('tempdb..#winit_wish') is not null begin drop table #winit_wish end
			 select distinct a.TrackNo
			 INTO #winit_wish
			 from Skyeye.dbo.Tail_Leg_Wish_Outsea_Account a
			 where
			 	a.PrintedStatus = 'y' AND
			 	a.whCode in ('DEWINIT', 'UKWINIT', 'USTXWINIT', 'USWCWINIT', 'AUWINIT', 'USKYNWINIT') AND
			 	a.PostageService NOT LIKE '%Untracked%' AND 
			 	a.DSCAN IS NULL AND
			 	a.TrackNo IS NOT NULL AND
			 	a.PostageService LIKE '%DHL%'
			 select *
			 from #winit_ebay
			 union
			 SELECT *
			 FROM #winit_wish '''
	cur.execute(sql)
	result = cur.fetchall()
	return result


# def delete_no_use_data():
# 	conn = pymssql.connect(host = "192.168.1.252",
# 						   port = "5678",
# 						   user = "tom.dong",
# 						   password = "oig123456",
# 						   database = "wangzf",
# 						   charset = "utf8")
# 	cur = conn.cursor()
# 	sql = """delete from dbo.winit_ascan_dscan
# 			 where ascan is null or dscan is null """
# 	cur.execute(sql)
# 	conn.commit()
# 	cur.close()
# 	conn.close()
# 	print("===== 已删除上网时间或妥投时间为空的数据 =====")


def md5(str):
	m = hashlib.md5()
	m.update(str.encode('utf-8'))
	return m.hexdigest().upper()


def get_trackTrace(logwinit, trackingNOs):
	token = "20BC45092988B9AA6D740514060530C0"
	actionValue = "tracking.getOrderVerdorTracking"
	app_keyValue = "ken@oigbuy.com"
	dataValue = '{"trackingnos":"' + trackingNOs + '"}'
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
	url_outsea = "http://openapi.winit.com.cn/openapi/service"
	# headers
	headers = {
		"Authorization": "Bearer " + token,
		"Content-Type": "application/json"
		}
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
				"trackingnos": trackingNOs
			}
		})
	# request
	response = requests.post(url = url_outsea, data = params, headers = headers)
	logwinit.info("数据请求成功...")
	js = json.loads(response.text)
	logwinit.info("数据解析成功...")
	return js


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
		for i in trace_df.index:
			if len(trace_df.eventDescription[i]) >= 60:
				trace_df.eventDescription[i] = trace_df.eventDescription[i][0:60]
	return trace_df


def write_to_db(logwinit, df, engine):
	try:
		pd.io.sql.to_sql(df,
	                     name = 'winit_outsea_order_ascan_dscan',
	                     con = engine,
	                     if_exists = 'append',
	                     index = False)
		logwinit.info('数据成功写入DataBase!')
	except:
		logwinit.info('数据写入DataBase时出现错误！')


def concat_df(logwinit, cur, engine):
	trackNo = get_trackingNo(cur)
	trackNo = list(trackNo)
	logwinit.info('*' * 150)
	logwinit.info('查询 %s 个跟踪单号：' % len(trackNo))
	logwinit.info('*' * 150)
	for i in trackNo:
		logwinit.info('查询的跟踪单号为: %s' % i[0])
		js = get_trackTrace(logwinit, i[0])
		logwinit.info('请求到的数据: %s' % js)
		try:
			trace_df = get_trace_df(js)
			write_to_db(logwinit, trace_df, engine)
		except: 
			continue

def main():
	start = time.time()
	path = "D:/logfiles/oversea_tracking/winit_dhl/logwinit_dhl%s.log" % datetime.now().strftime('%Y-%m-%d')
	logwinit = Logger.Logger(path = path)
	conn = pymssql.connect(host = "192.168.1.252",
						   port = "5678",
						   user = "tom.dong",
						   password = "oig123456",
						   database = "wangzf",
						   charset = "utf8")
	cur = conn.cursor()
	logwinit.info("连接数据库成功！")
	engine = get_engine()
	# delete_repeat(conn, cur)
	concat_df(logwinit, cur, engine)
	# delete_no_use_data()
	cur.close()
	conn.close()

	# 关闭数据库
	logwinit.info("关闭数据库！")
	cost = time.time() - start
	logwinit.info("程序运行时间为：%s" % cost)


if __name__ == "__main__":
	main()