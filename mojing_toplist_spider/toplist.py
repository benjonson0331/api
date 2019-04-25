#!/usr/bin/env python
# -*- coding: utf-8 -*-


import os
import requests 
import json
import time
import pandas as pd
from pandas import Series, DataFrame


# conn = pymssql.connect(host = '192.168.1.252:5678', 
# 					   	 user = 'tom.dong',
# 					   	 password = 'oig123456',
# 					   	 database = 'dmj')
# cur = conn.cursor()
# cur.execute('''select 
#				 	no_plate 
#				 from USEGD_TrackNos 
# 				 where status is null or status<>'派送完成' order by printdate''')
# results = cur.fetchall()
# conn.commit()
# cur.close()
# conn.close()

def get_ChildrenList():
	url = "https://market1.moojing.com/api/cats_tree?plat=tmall"
	headers = {
		'Accept':'application/json, text/javascript, */*; q=0.01',
		'Connection':'keep-alive',
		'Cookie':'__root_domain_v=.moojing.com; fx=default; qqClosedpop=1511414318575; Hm_lvt_05ddfef16c79f92646ef46dd17e01386=1511408148; Hm_lpvt_05ddfef16c79f92646ef46dd17e01386=1511414399; from=; cookie2015=e31c62b245631bf3d8ff4eb06f8da88807374c03; qq=c; __utma=61458224.79860059.1511408148.1511408225.1511414296.2; __utmc=61458224; __utmz=61458224.1511414296.2.2.utmcsr=console.moojing.com|utmccn=(referral)|utmcmd=referral|utmcct=/login/; __utmv=61458224.|1=Member%20Type=visitor=1; _ga=GA1.2.79860059.1511408148; _gid=GA1.2.2024134925.1511408148; _qddaz=QD.vmz0gb.8c78f.jabxe0op; session=e559da78ae67091f_5a164215.C0kkgWHlz4oL4qUqPb7UX4OAUEM',
		'Host':'market1.moojing.com',
		'User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36'
	}
	response = requests.get(url, headers = headers)
	Category = response.json()
	childrenList = Category["result"]["childrenList"]
	return childrenList

Items = {
	"女装/女士精品": "16",
	"男装": "30",
	"住宅家具": "50008164",
	"手机": "1512",
	"女鞋": "50006843",
	"汽车/用品/配件/改装": "26",
	"箱包皮具/热销女包/男包": "50006842",
	"3C数码配件": "50008090",
	"大家电": "50022703",
	"流行男鞋": "50011740",
	"玩具/童车/益智/积木/模型": "25",
	"运动/瑜伽/健身/球迷用品": "50010728",
	"厨房电器": "50012082",
	"床上用品": "50008163",
	"户外/登山/野营/旅行用品": "50013886",
	"个人护理/保健/按摩器材": "50002768",
	"五金/工具": "50020485",
	"饰品/流行首饰/时尚饰品新": "50013864",
	"服饰配件/皮带/帽子/围巾": "50010404",
	"运动服/休闲服装": "50011699",
	"家居饰品": "50020808"
}
first_level_code = list(Items.values())
childrenList = get_ChildrenList()

first_category_id = []
first_category_name_en = []
first_name = []
first_is_parent = []
first_level = []
first_parent_id = []
first_num = []
first_chd = []
for i in first_level_code:
	first_category_id.append(childrenList[i]["category_id"])
	first_category_name_en.append(childrenList[i]["category_name_en"])
	first_name.append(childrenList[i]["name"])
	first_is_parent.append(childrenList[i]["is_parent"])
	first_level.append(childrenList[i]["level"])
	first_parent_id.append(childrenList[i]["parent_id"])
	first_num.append(childrenList[i]["num"])
	first_chd.append(childrenList[i]["chd"])

first_level_dict = {
	'first_category_id': first_category_id,
	'first_category_name_en': first_category_name_en,
	'first_name': first_name,
	'first_is_parent': first_is_parent,
	'first_level': first_level,
	'first_parent_id': first_parent_id,
	'first_num': first_num
}
#==========================================================
def get_code(level_code, level_chd):
	L = {}
	category_id = []
	category_name_en = []
	name = []
	is_parent = []
	level = []
	parent_id = []
	num = []
	chd = []
	for i in range(len(level_code)):
		for t in level_chd[i]:
			category_id.append(childrenList[t]["category_id"])
			category_name_en.append(childrenList[t]["category_name_en"])
			name.append(childrenList[t]["name"])
			is_parent.append(childrenList[t]["is_parent"])
			level.append(childrenList[t]["level"])
			parent_id.append(childrenList[t]["parent_id"])
			num.append(childrenList[t]["num"])
			chd.append(childrenList[t]["chd"])
	L = {'category_id': category_id, 
		 'category_name_en': category_name_en, 
		 'name': name, 
		 'is_parent': is_parent, 
		 'level': level, 
		 'parent_id': parent_id, 
		 'num': num, 
		 'chd': chd}
	return L

def get_data(category_id):
	headers = {
		'Accept':'*/*',
		'Accept-Encoding':'gzip, deflate, br',
		'Accept-Language':'zh-CN,zh;q=0.9',
		'Connection':'keep-alive',
		'Cookie':'__root_domain_v=.moojing.com; fx=default; qqClosedpop=1511414318575; Hm_lvt_05ddfef16c79f92646ef46dd17e01386=1511408148; Hm_lpvt_05ddfef16c79f92646ef46dd17e01386=1511414399; from=; qq=c; __utma=61458224.79860059.1511408148.1511408225.1511414296.2; __utmc=61458224; __utmz=61458224.1511414296.2.2.utmcsr=console.moojing.com|utmccn=(referral)|utmcmd=referral|utmcct=/login/; __utmv=61458224.|1=Member%20Type=visitor=1; _ga=GA1.2.79860059.1511408148; _qddaz=QD.vmz0gb.8c78f.jabxe0op; session=e559da78ae67091f_5a164215.C0kkgWHlz4oL4qUqPb7UX4OAUEM',
		'Host':'market1.moojing.com',
		'Origin':'http://console.moojing.com',
		'Referer':'http://console.moojing.com/static/sole_pages/toplist.html',
		'User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36'
	}
	data = {}
	for code in category_id:
		code_url = "https://market1.moojing.com/api/shuang11/platform/tmall/cats/" + code + "/brands_top?type=brand&start=2017-10&end=2016-02&lang=zh&page=1&page_size=5&q="
		level_response = requests.get(code_url, headers, timeout = 40)
		level_data = level_response.json()
		ID = [code] * len(level_data["result"])
		brand_name = []
		brand_id = []
		sale = []
		sold = []
		avg_price = []
		item_num = []
		for i in range(len(level_data["result"])):
			brand_name.append(level_data["result"][i]["brand_name"])
			brand_id.append(level_data["result"][i]["brand_id"])
			sale.append(level_data["result"][i]["sale"])
			sold.append(level_data["result"][i]["sold"])
			avg_price.append(level_data["result"][i]["avg_price"])
			item_num.append(level_data["result"][i]["item_num"])
		data[code] = {'ID': ID,
					  'brand_name': brand_name,
					  'brand_id': brand_id,
					  'sale': sale,
					  'sold': sold,
					  'avg_price': avg_price,
					  'item_num': item_num}
		time.sleep(3)
	return data                                           # dict 

def get_df(category_id, data):
	df = DataFrame()
	for i in category_id:
		df = pd.concat([df, DataFrame(data[i])])
	return df                                                 # DataFrame
print(u"========================================================================================================================")
first_category_id = first_category_id
# print(first_category_id)
first_chd = first_chd
# print(first_chd)
first_data = get_data(first_category_id)
# print(first_data)
first_df = get_df(first_category_id, first_data)
print(first_df)
first_identity_code = pd.DataFrame(first_level_dict)
print(first_identity_code)
df1_final = first_df.merge(first_identity_code, left_on = "ID", right_on = "first_category_id")
df1_final.drop("ID", axis = 1, inplace = True)
print(df1_final)
df1_final.to_csv("/home/wangzhefeng/df1_final.csv")
print(u"========================================================================================================================")
second_category_id = get_code(first_category_id, first_chd)['category_id']
# print(second_category_id)
second_chd = get_code(first_category_id, first_chd)['chd']
# print(second_chd)
second_data = get_data(second_category_id)
# print(second_data)
second_df = get_df(second_category_id, second_data)
print(second_df)
L2 = get_code(first_category_id, first_chd)
second_identity_code = pd.DataFrame(L2)
print(second_identity_code)
second_identity_code.rename(columns = {'category_id': 'second_category_id',
					  				   'category_name_en': 'second_category_name_en',
					  				   'name': 'second_name',
					  				   'is_parent': 'second_is_parent',
					  				   'level': 'second_level',
					  				   'parent_id': 'second_parent_id',
					  				   'num': 'second_num',
					  				   'chd': 'second_chd'}, 
					  		inplace = True)
df2 = second_df.merge(second_identity_code, left_on = "ID", right_on = "second_category_id")

df2.drop(["ID", "second_chd"], axis = 1, inplace = True)
df2_final = df2.merge(first_identity_code, left_on = "second_parent_id", right_on = "first_category_id")
print(df2_final)
df2_final.to_csv("/home/wangzhefeng/df2_final.csv")
print(u"========================================================================================================================")
third_category_id = get_code(second_category_id, second_chd)['category_id']
# print(third_category_id)
third_chd = get_code(second_category_id, second_chd)['chd']
# print(third_chd)
third_data = get_data(third_category_id)
# print(third_data)
third_df = get_df(third_category_id, third_data)
print(third_df)
L3 = get_code(second_category_id, second_chd)
third_identity_code = pd.DataFrame(L3)
print(third_identity_code)
third_identity_code.rename(columns = {'category_id': 'third_category_id',
									  'category_name_en': 'third_category_name_en',
									  'name': 'third_name',
									  'is_parent': 'third_is_parent',
									  'level': 'third_level',
									  'parent_id': 'third_parent_id',
									  'num': 'third_num',
									  'chd': 'third_chd'}, 
						   inplace = True)
df3 = third_df.merge(third_identity_code, left_on = "ID", right_on = "third_category_id")

df3.drop(["ID", "third_chd"], axis = 1, inplace = True)
df31 = df3.merge(second_identity_code, left_on = "third_parent_id", right_on = "second_category_id")
df3_final = df31.merge(first_identity_code, left_on = "second_parent_id", right_on = "first_category_id")
df3_final.drop("third_chd", axis = 1, inplace = True)
print(df3_final)
df3_final.to_csv("/home/wangzhefeng/df3_final.csv")
print(u"========================================================================================================================")
forth_category_id = get_code(third_category_id, third_chd)['category_id']
# print(forth_category_id)
forth_chd = get_code(third_category_id, third_chd)['chd']
# print(forth_chd)
forth_data = get_data(forth_category_id)
# print(forth_data)
forth_df = get_df(forth_category_id, forth_data)
print(forth_df)
L4 = get_code(third_category_id, third_chd)
forth_identity_code = pd.DataFrame(L4)
print(forth_identity_code)
forth_identity_code.rename(columns = {'category_id': 'forth_category_id',
									  'category_name_en': 'forth_category_name_en',
									  'name': 'forth_name',
									  'is_parent': 'forth_is_parent',
									  'level': 'forth_level',
									  'parent_id': 'forth_parent_id',
									  'num': 'forth_num',
									  'chd': 'forth_chd'}, 
						   inplace = True)
df4 = forth_df.merge(forth_identity_code, left_on = "ID", right_on = "forth_category_id")

df4.drop(["ID", "forth_chd"], axis = 1, inplace = True)
df41 = df4.merge(third_identity_code, left_on = "forth_parent_id", right_on = "third_category_id")
df42 = df41.merge(second_identity_code, left_on = "third_parent_id", right_on = "second_category_id")
df4_final = df42.merge(first_identity_code, left_on = "second_parent_id", right_on = "first_category_id")
df4_final.drop(["second", "third_chd"], axis = 1, inplace = True)
print(df4_final)
df4_final.to_csv("/home/wangzhefeng/df4_final.csv")

print(u"========================================================================================================================")
df_final = pd.concat([df1_final, df2_final, df3_final, df4_final])
print(df_final)
df_final.to_csv("/home/wangzhefeng/df_final.csv")


# def main():
# 	get_ChildrenList()

# if __name__ == "__main__":
# 	main()
print(u"========================================================================================================================")
# KEY = Category["result"]["childrenList"].keys()
# VALUE = list(Category["result"]["childrenList"].values())
# print(VALUE[1])
# print(VALUE[1]["category_id"])
# print(type(VALUE[1]["category_id"]))
# print(VALUE[1]["category_id"] in first_level_code)

