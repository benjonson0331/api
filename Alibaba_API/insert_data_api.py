# -*- coding:utf-8 -*-


import pymssql
from Alibaba_api import run1688_api
import time


class get_api_data():

    # 连接数据库，并将 purchase_order_view 的 orderid 的提取出来
    def connectsql(self):
        conn = pymssql.connect(
            host = '192.168.1.252',
            port = '5678',
            user = 'tom.dong',
            password = 'oig123456',
            charset='utf8')
        cur = conn.cursor()
        return cur,conn


    # 将 下载到表里未签收的 orderid 删掉
    def delete_orderid(self):
        #**** 删除未签收的 orderid ****
        sql = "delete from [wangzf].[dbo].[ali_Order_Logistics_Trace] where remark in ('该订单没有物流跟踪信息。', '根据订单ID获取订单时出错。', '无法识别物流情况。')"
        c = self.connectsql()
        c[0].execute(sql)
        c[1].commit()
        print ("已删除未签收的 orderid ！")

    # 查找需要更新的 orderid（新增加的 orderid）
    def data_or_in_purchase(self):
        c = self.connectsql()
        #···判断 ali_Order_Logistics_Trace 的 orderid 是否存在于 purchase_order_view···
        sql = "select a.out_order_no from [odoo].[dbo].[purchase_order_view] a left join [purchase].[dbo].[purchase_alibaba_api] b on" \
              " a.out_order_no = b.订单号 where b.订单号 is null and a.out_order_no not in ('冲抵采销','转账','直付')"
        c[0].execute(sql)
        orderids = c[0].fetchall()
        id = []
        for orderid in orderids:
            id.append(orderid[0].strip().replace('\t',''))
        return id

    # ？？？ 将需要更新的 orderid 传入 run1688_api().run_api() 查询（··调用 阿里巴巴 API··）
    def getdata(self):
        self.delete_orderid()
        datas = []
        get_updatadata = self.data_or_in_purchase()
        for post_id in get_updatadata:
            time.sleep(0.1)
            data = run1688_api().run_api(post_id)
            datas.append(data)
        return datas

    #更新数据
    def updata_orderid(self):
        sql_data = self.connectsql()
        datas = self.getdata()
        for data in datas:
            # 将数据插入数据库
            if data:
                sql = "insert into [wangzf].[dbo].[ali_Order_Logistics_Trace] (orderid, status, logisticsBillNo, logisticsCompanyName, acceptTime, remark, now_time) values(%s,%s,%s,%s,%s,%s,%s)"
                sql_data[0].execute(sql, data)
        sql_data[1].commit()
        sql_data[1].close()


# p = get_api_data()
# p.updata_orderid()

