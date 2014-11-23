# -*- coding:utf-8 -*-
__author__ = 'tangmash'
import pymongo
import time
from datetime import *
import math
import re
import logging
import logging.handlers

class Checkdata():

    def __init__(self, data_check, db_check):
        self.dicError={}
        self.dicNone={}
        self.data_check = data_check
        self.db_check = db_check


    def checkdata(self):
        db = self.db_check
        data = self.data_check
        sign = True
        id = data['_id']
        print id
        #处理city相关字段
        if data['city_id'] is not None:
            print len(str(data["city_id"]))
            if len(str(data["city_id"])) != 6:
                sign = False
                self.addExceptionError(db, id, "city_id")
        else:
            sign = False
            self.addExceptionNone(db, id, "city_id")

        #处理ouBasicColor相关字段
        #basic_outer_color_id
        if data['basic_outer_color_id'] is not None:
            if data["basic_outer_color_id"] < 1 or data["basic_outer_color_id"] > 15:
                sign = False
                self.addExceptionError(db, id, "basic_outer_color_id")
        else:
            sign = False
            self.addExceptionNone(db, id, "basic_outer_color_id")

        #site_model_id 局部车型Id
        if data['local_model_id'] is None:
            sign = False
            self.addExceptionNone(db, id, "local_model_id")

        #dealer_site_id 局部车商Id
        if data['local_dealer_id'] is None:
            sign = False
            self.addExceptionNone(db, id, "local_dealer_id")

        #site 网站Id
        if data['website_id'] is None:
            sign = False
            self.addExceptionNone(db, id, "website_id")

        #mileage 公里数 mileage
        if data['mileage'] is not None:
            if data["mileage"] < 0 or data["mileage"] > 5000000:
                sign = False
                self.addExceptionError(db, id, "mileage")
        else:
            sign = False
            self.addExceptionNone(db, id, "mileage")

        #price 价格
        if data['price'] is not None:
            if data["price"] < 0 or data["price"] > 10000000:
                sign = False
                self.addExceptionError(db, id, "price")
        else:
            sign = False
            self.addExceptionNone(db, id, "price")

        #create_at 采集日期
        if data['collect_at'] is not None:
            if data['collect_at'].strftime('%Y') < "2014":
                sign = False
                self.addExceptionError(db, id, "collect_at")

            delta = datetime.now() - data['collect_at']
            if delta.days < 0:
                sign = False
                self.addExceptionError(db, id, "collect_at over now time")

            month = datetime.strftime(data['collect_at'],'%m')
            if int(month) < 1 or int(month) > 12:
                sign = False
                self.addExceptionError(db, id, "collect_at error month")
        else:
            sign = False
            self.addExceptionNone(db, id, "collect_at")

        #date.release_date 发布日起
        release_date = data['date']['release_date']
        if release_date is not None:
            if data['date']['release_date'].strftime('%Y') < "1980":
                sign = False
                self.addExceptionError(db, id, "date.release_date")

            delta = datetime.now() - release_date
            if delta.days < 0:
                sign = False
                self.addExceptionError(db, id, "date.release_date over now time")

            month = datetime.strftime(release_date,'%m')
            print month
            if int(month) < 1 or int(month) > 12:
                sign = False
                self.addExceptionError(db, id, "date.release_date error month")

            delta1 = data['collect_at'] - release_date
            if delta1.days < 0:
                sign = False
                self.addExceptionError(db, id, "date.release_date")
        else:
            sign = False
            self.addExceptionNone(db, id, "date.release_date")

        #date.register_date 初登日起
        register_date = data['date']['register_date']
        if register_date is not None:
            if register_date.strftime('%Y') < "1950":
                sign = False
                self.addExceptionError(db, id, "date.register_date")

            delta = datetime.now() - register_date
            if delta.days < 0:
                sign = False
                self.addExceptionError(db, id, "date.register_date")

            month = datetime.strftime(register_date,'%m')
            if int(month) < 1 and int(month) > 12:
                sign = False
                self.addExceptionError(db, id, "date.register_date")

            delta1 = data['collect_at'] - register_date
            if delta1.days < 0:
                sign = False
                self.addExceptionError(db, id, "date.register_date")

            delta2 = release_date - register_date
            if delta2.days < 0:
                sign = False
                self.addExceptionError(db, id, "date.register_date")
        else:
            sign = False
            self.addExceptionNone(db, id, "date.register_date")
        '''
        if sign:
            db.base_vehicle_source_zhuang.update({'_id':id},{'$set':{'flag.is_exception':False}})
        '''
        print id
        #return sign
        #print self.dicError,len(self.dicError)
        #print self.dicNone,len(self.dicNone)




    def addExceptionError(self, db, id, fieldName):
        fieldName = fieldName + "_Data_Error"
        db.base_vehicle_source_zhuang.update({'_id':id},{'$set':{'flag.is_exception':True}})
        self.logg("id: " + str(id) + " error: " + str(fieldName))
        print fieldName
        '''
        if self.dicError.has_key(fieldName):
            self.dicError[fieldName] += 1
        else:
            self.dicError[fieldName] = 1
        '''

    def addExceptionNone(self, db, id, fieldName):
        fieldName = fieldName + "_Data_None"
        db.base_vehicle_source_zhuang.update({'_id':id},{'$set':{'flag.is_exception':True}})
        self.logg("id: " + str(id) + " error: " + str(fieldName))
        print fieldName
        '''
        if self.dicNone.has_key(fieldName):
            self.dicNone[fieldName] += 1
        else:
            self.dicNone[fieldName] = 1
        '''



    #第一阶段去重
    def simVehi(self):
        db = self.db_check
        data = self.data_check
        id = data['_id']
        simVehiSrcId = str(data['city_id']) + "-"
        simVehiSrcId += str(data['color_id']) + "-"
        simVehiSrcId += data['site'] + "-"
        simVehiSrcId += data['site_model_id'] + "-"
        simVehiSrcId += data['site_dealer_id'] + "-"
        simVehiSrcId += datetime.strftime(data['date']['register_date'],'%Y') + "-" + \
                        datetime.strftime(data['date']['register_date'],'%m')

        db.base_vehicle_source.update({'_id':id},{'$set':{'dedup.vehicle_part_identity_id':simVehiSrcId, "dedup.part_deduplication_flag":1}})
        #print simVehiSrcId

    #第二阶段去重
    def vehicleDedup(self):
        iVehiDic = {}
        conn = pymongo.Connection()
        db = conn.kkcrawler
        iList = {}
        iCount = 0
        t_similarMatrix = self.genMatrix(0, 0)
        iItems = db.base_vehicle_source.find({'exceptionFlag':'normal', 'dedup.part_deduplication_flag':1, 'partProcessFlag':{'$in':[0,2]}}).limit(1000)
        while iItems:
            for iItem in iItems:
                simVehiSrcId = iItem['dedup.vehicle_part_identity_id']
                if iVehiDic.has_key(simVehiSrcId):
                    iList = None
                    iList.append(iItem)
                    iVehiDic[simVehiSrcId] = iList
                else:
                    iVehiDic[simVehiSrcId] = iList.append(iItem)
                    iList = None

            for k,v in iVehiDic.items():
                iSize = len(v)
                t_raptime = 0#重复次数
                t_distSum = 0#内部距离
                if iSize > 1:
                    iCount = iSize
                    t_similarMatrix = self.genMatrix(iSize,iSize)
                    if iSize > 1:
                        for i in range(0, iSize-1):
                            t1_mileage = v[i]['mileage']
                            t1_price = v[i]['price']
                            for j in range(i+1, iSize):
                                t2_mileage = v[j]['mileage']
                                t2_price = v[j]['price']
                                if self.isSameVehi(t1_mileage, t1_price, t2_mileage, t2_price):
                                    t_similarMatrix[i][j]=1
                                    t_similarMatrix[j][i]=1
                                else:
                                    t_similarMatrix[i][j]=0
                                    t_similarMatrix[j][i]=0

                    for i in range(0,5):
                        t_similarMatrix[i][i]=1

                    #传递性
                    for i in range(0,iCount-1):
                        for j in range(i+1, iCount):
                            if t_similarMatrix[i][j] == 1 and t_similarMatrix[j][j+1] == 1:
                                t_similarMatrix[i][j+1] = 1

                    #计算重复
                    for i in range(0,iCount-1):
                        for j in range(i+1, iCount):
                            if t_similarMatrix[i][j] == 1 and t_similarMatrix[j][i] == 1:
                                t_raptime +=0

                    #计算步数
                    for i in range(0,iCount-1):
                        t_info1 = v[i]
                        for j in range(i+1, iCount):
                            t_info2 = v[j]
                            t_diff1 = (abs(t_info1['price'] - t_info2['price'])*1)/min(t_info1['price'], t_info2['price'])*100000;
                            t_diff2 = abs(t_info1['mileage'] - t_info2['mileage'])
                            t_dist = math.sqrt(t_diff1*t_diff1+t_diff2*t_diff2)
                            t_distSum += t_dist

                    id = v[0]['_id']
                    db.base_vehicle_source.update({'_id':id},{'$push':{'dedup.local_class_span':t_distSum, "dedup.local_deduplication_count":t_raptime, "partProcessFlag":2, "partDupFlag":0}})

                    for i in range(0,iCount-1):
                        for j in range(i+1, iCount):
                            if t_similarMatrix[i][j] == 1 and t_similarMatrix[j][i] == 1:
                                db.base_vehicle_source.update({'_id':id},{'$push':{'dedup.local_class_span':t_distSum, "dedup.local_deduplication_count":t_raptime, "partProcessFlag":2, "partDupFlag":id}})

                else:
                    id = v[0]['_id']
                    db.base_vehicle_source.update({'_id':id},{'$push':{'dedup.local_class_span':1, "dedup.local_deduplication_count":1, "partProcessFlag":2, "partDupFlag":0}})

            iItems = db.base_vehicle_source.find({'exceptionFlag':'normal', 'dedup.part_deduplication_flag':1, 'partProcessFlag':{'$in':[0,2]}}).limit(1000)


    def isSameVehi(self, t1_mileage, t1_price, t2_mileage, t2_price):
        if abs(t1_mileage - t2_mileage):
            if (abs(t1_price - t2_price)*100/min(t1_price, t2_price)) <= 5:
                return True
        return False


    def genMatrix(self,rows,cols):
        matrix = [[0 for col in range(cols)] for row in range(rows)]
        for i in range(rows):
            for j in range(cols):
                matrix[i][j] = 0
        return matrix
    def logg(self ,strData):
        LOG_FILE = 'tst.log'
        handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes = 4096*4096, backupCount = 5) # 实例化handler
        fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
        formatter = logging.Formatter(fmt)   # 实例化formatter
        handler.setFormatter(formatter)      # 为handler添加formatter
        logger = logging.getLogger('tst')    # 获取名为tst的logger
        logger.addHandler(handler)           # 为logger添加handler
        logger.setLevel(logging.DEBUG)
        logger.info(strData)

