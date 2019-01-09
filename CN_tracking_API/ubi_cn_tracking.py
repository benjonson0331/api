#!/usr/bin/env python
# -*- coding: utf-8 -*-


import requests 
import json
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import pymssql
from email.utils import formatdate
import hashlib
import hmac
import base64

# ===========================================================================
# Test env
url_test = "http://qa.etowertech.com"
Token_test = "test5AdbzO5OEeOpvgAVXUFE0A"
Key_test = "79db9e5OEOpvgAVXUFWSD"

# Production env
# Beijing, China
url_cn = "https://cn.etowertech.com"
# Sydney, Australia
url_au = "https://au.etowertech.com"
# Portland or USA
url_us = "https://us.etowertech.com"
#Token Secret
Token = "pclB54HlRuJsZEPTmC9vRe"
Secret = "bR_xHiTpWqzpRnkZTCjYYA"
# ==========================================================================
def hm_sha1(Secret, Message):
	secret =  Secret.encode(encoding = 'utf-8')
	message = Message.encode(encoding = 'utf-8')
	# h = hmac.new(secret)
	# h.update(message)
	# h_str = h.hexdigest()
	# h_str = base64.b64encode(h_str.encode(encoding = 'utf-8'))
	h = hmac.new(secret, message, digestmod = hashlib.sha1)
	h_str = h.hexdigest()
	h_str = base64.b64encode(h_str.encode(encoding = 'utf-8'))
	return h_str




def get_request():
	Host = "https://au.etowertech.com"

	Path = "/services/shipper/trackingEvents"
	Path_1 = "/services/integration/shipper/trackingEvents"

	Token = "pclu7cYfbveaaTbRbZYcxX"
	Secret = "SM-M3R2xB7zaQbeX-gUgTA"

	Token_1 = "pclB54HlRuJsZEPTmC9vRe"
	Secret_1 = "bR_xHiTpWqzpRnkZTCjYYA"

	X_WallTech_Date = formatdate(timeval = None, localtime = False, usegmt = True)
	Message = "POST" + "\n" + X_WallTech_Date + "\n" + Path_1
	sign = hm_sha1(Secret, Message)
	
	print(X_WallTech_Date)
	print(Message)
	print(sign)
	
	headers = {
		"Content-Type": "application/json",
		"Accept": "application/json",
		"User-Agent": "Mozilla 5.0",
		"Host": "au.etowertech.com",
		"X-WallTech-Date": X_WallTech_Date,
		"Authorization": "WallTech %s:%s" % (Token, sign)
	}

	params = json.dumps({
		"trackingNo": ["2MB617086801000931506"]
	})
	response = requests.post(url = Host + Path, params = params, headers = headers)
	print(response.status_code)
	print(response.url)
	print(Host + Path)
	print(response.text)







# def get_engine():
# 	db_info = {
# 		"host": "192.168.1.252",
# 		"port": "5678",
# 		"user": "tom.dong",
# 		"password": "oig123456",
# 		"database": "wangzf"
# 	}
# 	conn_info = "mssql+pymssql://%(user)s:%(password)s@%(host)s:%(port)s/%(database)s" % db_info
# 	engine = create_engine(conn_info, encoding = "utf-8")
# 	return engine

# def write_to_ubi(df):
# 	engine = get_engine()
# 	pd.io.sql.to_sql(df,
# 					 name = "ubi_order_ascan_dscan",
# 					 con = engine, 
# 					 if_exists = "append",
# 					 index = False)




# def delete_repeat():
# 	conn = pymssql.connect(host = "192.168.1.252",
# 						   port = "5678",
# 						   user = "tom.dong",
# 						   password = "oig123456",
# 						   database = "wangzf",
# 						   charset = "utf8")
# 	cur = conn.cursor()
# 	sql = """DELETE FROM dbo.ubi_order_ascan_dscan
# 			 WHERE id NOT IN (
# 			 	SELECT MIN(A.id)
# 			 	FROM dbo.ubi_order_ascan_dscan A
# 			 	GROUP BY ) """
# 	cur.execute(sql)
# 	conn.commit()
# 	cur.close()
# 	conn.close()
# 	print('=========== 已删除重复数据 ============')

# def delete_no_use_data():
# 	conn = pymssql.connect(host = "192.168.1.252",
# 						   port = "5678",
# 						   user = "tom.dong",
# 						   password = "oig123456",
# 						   database = "wangzf",
# 						   charset = "utf8")
# 	cur = conn.cursor()
# 	sql = """ """
# 	cur.execute(sql)
# 	conn.commit()
# 	cur.close()
# 	conn.close()
# 	print('=========== 已删除无上网时间的数据 ============')

# def get_trackingNo():
# 	conn = pymssql.connect(host = "192.168.1.252",
# 						   port = "5678",
# 						   user = "tom.dong",
# 						   password = "oig123456",
# 						   database = "wangzf",
# 						   charset = "utf8")
# 	cur = conn.cursor()
# 	sql = """ """
# 	cur.execute(sql)
# 	result = cur.fetchall()
# 	cur.close()
# 	conn.close()
# 	return result





def main():
	get_request()

if __name__ == "__main__":
	main()
