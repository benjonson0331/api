# -*- coding:utf-8 -*-

import time
import requests
import hmac
import hashlib
import csv
try:
    import simplejson as json
except (ImportError, SyntaxError):
    import json


class run1688_api():

    def __init__(self):

        self.urlhead_orderDetail = "https://gw.open.1688.com/openapi/param2/1/cn.alibaba.open/trade.order.orderDetail.get/8320473"
        self.urlPath_orderDetail = "param2/1/cn.alibaba.open/trade.order.orderDetail.get/8320473"

        self.urlhead_buyerView = "https://gw.open.1688.com/openapi/param2/1/com.alibaba.logistics/alibaba.trade.getLogisticsTraceInfo.buyerView/8320473"
        self.urlPath_buyerView = "param2/1/com.alibaba.logistics/alibaba.trade.getLogisticsTraceInfo.buyerView/8320473"

        self.secret = "S2tk5UFIgJ5f"

    def get_orderid(self,order_id):
        params_to_sign_orderDetail = {
            "orderId": order_id   # 连接数据库，将读取得到的数据遍历
        }

        params_to_sign_buyerView = {
            "orderId": order_id,
            "webSite": "1688"
        }

        data = {
            "orderId": order_id  # 连接数据库，将读取得到的数据遍历
        }
        return params_to_sign_orderDetail,params_to_sign_buyerView,data

    def mix_str(self,pstr):
        if isinstance(pstr, str):
            return pstr
        if not PY3 and isinstance(pstr, unicode):
            return pstr.encode('utf-8')
        else:
            return str(pstr)

    #  ** 获取 buyerView API 的签名:
    def sign_buyerView(self,urlPath, params, secret,id):

        if not self.urlPath_buyerView:
            raise_aop_error('sign error: urlPath missing')
        if not secret:
            raise_aop_error('sign error: secret missing')
        paramList = []
        id_1 = self.get_orderid(id)
        if id_1[1]:
            if not hasattr(id_1[1], "items"):
                self.raise_aop_error('sign error: params must be dict-like')
            paramList = [self.mix_str(k) + self.mix_str(v) for k, v in id_1[1].items()]
            paramList = sorted(paramList)
        msg = bytearray(self.urlPath_buyerView.encode('utf-8'))
        for param in paramList:
            msg.extend(bytes(param.encode('utf-8')))
        sha = hmac.new(bytes(self.secret.encode('utf-8')), None, hashlib.sha1)
        sha.update(msg)
        return sha.hexdigest().upper()

    # ** 获取 buyerView API 的信息：
    def run_buyerView(self, id):
        id_2 = self.get_orderid(id)
        sign = self.sign_buyerView(self.urlPath_buyerView, id_2[1], self.secret, id)
        param_sign = {
            "_aop_signature": sign,
            "webSite": "1688"
        }
        response = requests.post(url=self.urlhead_buyerView, params=param_sign, data=id_2[2])
        f = json.loads(response.text)
        if 'errorMessage' in list(f):    # 没有物流信息的判断（非未签收状态）
            logisticsBillNo = ''
            logisticsCompanyName = ''
            acceptTime = ''
            return logisticsBillNo, logisticsCompanyName, acceptTime, f["errorMessage"]


        elif 'error_message' in list(f):   # 没有物流信息的判断（非未签收状态）
            logisticsBillNo = ''
            logisticsCompanyName = ''
            acceptTime = ''
            weizhi = '无法识别物流情况。'
            return logisticsBillNo, logisticsCompanyName, acceptTime, weizhi

        else:                              # 这种情况的有物流信息的（已签收）
            p = f['logisticsTrace'][0]['logisticsSteps']
            p = p[len(p) - 1]
            acceptTime = p['acceptTime']
            remark = p['remark']
            logisticsBillNo = ''
            logisticsCompanyName = ''
            return  logisticsBillNo, logisticsCompanyName, acceptTime, remark

    # ** 获取 orderDetail API 的签名
    def sign_orderDetail(self,urlPath, params, secret, id):
        id_3 = self.get_orderid(id)
        if not self.urlPath_orderDetail:
            raise_aop_error('sign error: urlPath missing')
        if not secret:
            raise_aop_error('sign error: secret missing')
        paramList = []
        if id_3[0]:
            if not hasattr(id_3[0], "items"):
                self.raise_aop_error('sign error: params must be dict-like')
            paramList = [self.mix_str(k) + self.mix_str(v) for k, v in id_3[0].items()]
            paramList = sorted(paramList)
        msg = bytearray(self.urlPath_orderDetail.encode('utf-8'))
        for param in paramList:
            msg.extend(bytes(param.encode('utf-8')))
        sha = hmac.new(bytes(self.secret.encode('utf-8')), None, hashlib.sha1)
        sha.update(msg)
        return sha.hexdigest().upper()

    # ？？ 获取 orderDetail API 的信息，并判断 status状态 是否为 success ，如果为 success ，则调用 buyerView API 并 获取物流信息
    def run_api(self,id):
        try:
            id_4 = self.get_orderid(id)
            sign = self.sign_orderDetail(self.urlPath_orderDetail, id_4[0], self.secret, id)
            param_sign = {
                "_aop_signature": sign
            }
            time.sleep(0.1)
            respons = requests.post(url=self.urlhead_orderDetail, params=param_sign, data=id_4[2])
            f = json.loads(respons.text)
            #print(f)
            status = (f['result']['toReturn'][0]['status'])
            i = 1

            lists = ['该订单没有物流跟踪信息。', '根据订单ID获取订单时出错。', '无法识别物流情况。']
            buyerView_api_data = self.run_buyerView(id)
            if  buyerView_api_data[3] in lists:
                sellerCompanyName = (f['result']['toReturn'][0]['sellerCompanyName'])
                alipayTradeId = (f['result']['toReturn'][0]['alipayTradeId'])
                now_time = time.strftime('%Y-%m-%d', time.localtime(time.time()))
                print (id,status, alipayTradeId, sellerCompanyName, buyerView_api_data[2], buyerView_api_data[3], now_time,'      *********************该订单没有签收*********************')
                return id,status, alipayTradeId, sellerCompanyName, buyerView_api_data[2], buyerView_api_data[3], now_time

            else:
                sellerCompanyName = (f['result']['toReturn'][0]['sellerCompanyName'])
                alipayTradeId = (f['result']['toReturn'][0]['alipayTradeId'])
                now_time = str(time.strftime('%Y-%m-%d', time.localtime(time.time())))
                new_status = 'success'
                print (id, new_status,alipayTradeId, sellerCompanyName, buyerView_api_data[2], buyerView_api_data[3], now_time)
                return id, new_status,alipayTradeId, sellerCompanyName, buyerView_api_data[2], buyerView_api_data[3], now_time

        except requests.RequestException as e:
            if e.response is not None:
                response_data['response'] = e.response
                response_data['errors'] = [e.response.json()['errorMessage']]
            else:
                response_data['errors'] = [(str(e))]
#        except Exception as e:
#            print ("error")
#            pass

    def run_api_get_json(self,id):
        try:
            id_4 = self.get_orderid(id)
            sign = self.sign_orderDetail(self.urlPath_orderDetail, id_4[0], self.secret, id)
            param_sign = {
                "_aop_signature": sign
            }
            time.sleep(0.1)
            respons = requests.post(url=self.urlhead_orderDetail, params=param_sign, data=id_4[2])
            f = json.loads(respons.text)
            status = (f['result']['toReturn'][0]['status'])
            i = 1

            # json to csv

            # open a file for writing

            csvfile = csv.writer(open("/home/user/Documents/proj/1688API/a.csv", "w+"))

            data = f['result']['toReturn'][0]['orderEntries']
            csvfile.writerow(data[0].keys())  # header row
            for row in data:
                csvfile.writerow(row.values()) #values row


            # productName = f['result']['toReturn'][0]['orderEntries'][0]['productName'] 
            # price = f['result']['toReturn'][0]['orderEntries'][0]['price']
            # productPic = f['result']['toReturn'][0]['orderEntries'][0]['productPic']
            # quantity = f['result']['toReturn'][0]['orderEntries'][0]['quantity']

            # sellerCompanyName = f['result']['toReturn'][0]['sellerCompanyName']
            # alipayTradeId = f['result']['toReturn'][0]['alipayTradeId']
            # buyerFeedback = f['result']['toReturn'][0]['buyerFeedback']
            # sellerAlipayId = f['result']['toReturn'][0]['sellerAlipayId']
            # sellerPhone = f['result']['toReturn'][0]['sellerPhone']
            # sellerUserId = f['result']['toReturn'][0]['sellerUserId']
            # sellerMobile = f['result']['toReturn'][0]['sellerMobile']
            # buyerPhone = f['result']['toReturn'][0]['buyerPhone']

            # csvfile.writerow(["productName", productName])
            # csvfile.writerow(["price", price])
            # csvfile.writerow(["productPic", productPic])
            # csvfile.writerow(["quantity", quantity])
            # csvfile.writerow(["sellerCompanyName", sellerCompanyName])
            # csvfile.writerow(["sellerPhone", sellerPhone])
            
            # Write CSV Header, If you dont need that, remove this line
            #file.writerow(["productName", "price", "productPic", "quantity", "sellerCompanyName", "sellerPhone", "sellerMobile", "buyerPhone"])


        except requests.RequestException as e:
            if e.response is not None:
                response_data['response'] = e.response
                response_data['errors'] = [e.response.json()['errorMessage']]
            else:
                response_data['errors'] = [(str(e))]


if __name__ == '__main__':
 s = run1688_api()
 #s.run_api('179059889627412872')
 s.run_api_get_json('179059889627412872')



