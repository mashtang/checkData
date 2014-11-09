# -*- coding:utf-8 -*-
__author__ = 'tangmash'
import pymongo
import time
from datetime import *
import math

class Checkdata():

    def __init__(self):
        self.dicError={}
        self.dicNone={}
        self.sign = True

    def checkdata(self):
        conn = pymongo.Connection()
        db = conn.kkcrawler
        data = db.carSrcContent.find_one({'exceptionFlag':''})
        id = data['_id']
        #cityId = data["cityId"]
        #处理city相关字段
        if data['city_id'] is not None:
            if data["city_id"] >= 1 and data["city_id"] <= 363:
                pass
            else:
                self.sign = False
                self.addExceptionError(db, id, "city_id")
        else:
            self.sign = False
            self.addExceptionNone(db, id, "city_name")

        #处理ouBasicColor相关字段
        if data['color_id'] is not None:
            if data["color_id"] >= 1 and data["color_id"] <= 15:
                pass
            else:
                self.sign = False
                self.addExceptionError(db, id, "city_id")
        else:
            self.sign = False
            self.addExceptionNone(db, id, "city_id")

        #site_model_id 局部车型Id
        if data['site_model_id'] is not None and data['site_model_id'] != "":
            pass
        else:
            self.sign = False
            self.addExceptionNone(db, id, "site_model_id")

        #dealer_site_id 局部车商Id
        if data['dealer_site_id'] is not None and data['site_model_id'] != "":
            pass
        else:
            self.sign = False
            self.addExceptionNone(db, id, "dealer_site_id")

        #site 网站Id
        if data['site'] is not None and data['site'] != "":
            pass
        else:
            self.sign = False
            self.addExceptionNone(db, id, "site")

        #mileage 公里数
        if data['mileage'] is not None:
            if data["mileage"] >= 0 and data["mileage"] <= 5000000:
                pass
            else:
                self.sign = False
                self.addExceptionError(db, id, "mileage")
        else:
            self.sign = False
            self.addExceptionNone(db, id, "mileage")

        #price 价格
        if data['price'] is not None:
            if data["price"] >= 0 and data["price"] <= 10000000:
                pass
            else:
                self.sign = False
                self.addExceptionError(db, id, "price")
        else:
            self.sign = False
            self.addExceptionNone(db, id, "price")

        #create_at 采集日期
        if data['create_at'] is not None:
            if data['create_at'].strftime('%Y') >= "2014":
                pass
            else:
                self.sign = False
                self.addExceptionError(db, id, "create_at")

            delta = datetime.now() - data['create_at']
            if delta.days < 0:
                self.sign = False
                self.addExceptionError(db, id, "create_at")

            month = datetime.strftime(data['create_at'],'%m')
            if month < 1 and month > 12:
                self.sign = False
                self.addExceptionError(db, id, "create_at")
        else:
            self.sign = False
            self.addExceptionNone(db, id, "create_at")

        #date.release_date 发布日起
        if data['date.release_date'] is not None:
            if data['date.release_date'].strftime('%Y') >= "1980":
                pass
            else:
                self.sign = False
                self.addExceptionError(db, id, "date.release_date")

            delta = datetime.now() - data['date.release_date']
            if delta.days < 0:
                self.sign = False
                self.addExceptionError(db, id, "date.release_date")

            month = datetime.strftime(data['date.release_date'],'%m')
            if month < 1 and month > 12:
                self.sign = False
                self.addExceptionError(db, id, "date.release_date")

            delta1 = data['create_at'] - data['date.release_date']
            if delta1.day < 0:
                self.sign = False
                self.addExceptionError(db, id, "date.release_date")
        else:
            self.sign = False
            self.addExceptionNone(db, id, "date.release_date")

        #date.registration_date 发布日起
        if data['date.registration_date'] is not None:
            if data['date.registration_date'].strftime('%Y') >= "1950":
                pass
            else:
                self.sign = False
                self.addExceptionError(db, id, "date.registration_date")

            delta = datetime.now() - data['date.registration_date']
            if delta.days < 0:
                self.sign = False
                self.addExceptionError(db, id, "date.registration_date")

            month = datetime.strftime(data['date.registration_date'],'%m')
            if month < 1 and month > 12:
                self.sign = False
                self.addExceptionError(db, id, "date.registration_date")

            delta1 = data['create_at'] - data['date.registration_date']
            if delta1.day < 0:
                self.sign = False
                self.addExceptionError(db, id, "date.registration_date")
            delta2 = data['date.release_date'] - data['date.registration_date']
            if delta2.day < 0:
                self.sign = False
                self.addExceptionError(db, id, "date.registration_date")
        else:
            self.sign = False
            self.addExceptionNone(db, id, "date.registration_date")

        if self.sign:
            db.carSrcContent.update({'_id':id},{'$push':{'exceptionFlag':'normal'}})

        print
        print self.dicError
        print self.dicNone

    def addExceptionError(self, db, id, fieldName):

        db.carSrcContent.update({'_id':id},{'$push':{'exceptionFlag':'exception'}})
        if self.dicError.has_key(fieldName):
            self.dicError[fieldName] += 1
        else:
            self.dicError[fieldName] = 1

    def addExceptionNone(self, db, id, fieldName):

        db.carSrcContent.update({'_id':id},{'$push':{'exceptionFlag':'exception'}})
        if self.dicNone.has_key(fieldName):
            self.dicNone[fieldName] += 1
        else:
            self.dicNone[fieldName] = 1

    #第一阶段去重
    def simVehi(self):
        conn = pymongo.Connection()
        db = conn.kkcrawler
        data = db.carSrcContent.find_one({'exceptionFlag':'normal'})
        id = data['_id']
        simVehiSrcId = str(data['city_id']) + "-"
        simVehiSrcId += str(data['color_id']) + "-"
        simVehiSrcId += data['site'] + "-"
        simVehiSrcId += data['site_model_id'] + "-"
        simVehiSrcId += data['site_dealer_id'] + "-"
        simVehiSrcId += datetime.strftime(data['date.register_date'],'%Y') + "-" + \
                        datetime.strftime(data['date.register_date'],'%m')

        db.carSrcContent.update({'_id':id},{'$push':{'dedup.vehicle_part_identity_id':simVehiSrcId, "dedup.part_deduplication_flag":1}})
        #print simVehiSrcId

    #第二阶段去重
    def vehicleDedup(self):
        iVehiDic = {}
        conn = pymongo.Connection()
        db = conn.kkcrawler
        iList = {}
        iCount = 0
        t_similarMatrix = self.genMatrix(0, 0)
        iItems = db.carSrcContent.find({'exceptionFlag':'normal', 'dedup.part_deduplication_flag':1, 'partProcessFlag':[0,2]}).limit(1000)
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
                            t_diff2 = abs(t_info1['mileage'] - t_info2['mileage']);
                            t_dist = math.sqrt(t_diff1*t_diff1+t_diff2*t_diff2);
                            t_distSum += t_dist

                    id = v[0]['_id']
                    db.carSrcContent.update({'_id':id},{'$push':{'dedup.local_class_span':t_distSum, "dedup.local_deduplication_count":t_raptime, "partProcessFlag":2, "partDupFlag":0}})

                    for i in range(0,iCount-1):
                        for j in range(i+1, iCount):
                            if t_similarMatrix[i][j] == 1 and t_similarMatrix[j][i] == 1:
                                db.carSrcContent.update({'_id':id},{'$push':{'dedup.local_class_span':t_distSum, "dedup.local_deduplication_count":t_raptime, "partProcessFlag":2, "partDupFlag":id}})

                else:
                    id = v[0]['_id']
                    db.carSrcContent.update({'_id':id},{'$push':{'dedup.local_class_span':1, "dedup.local_deduplication_count":1, "partProcessFlag":2, "partDupFlag":0}})

            iItems = db.carSrcContent.find({'exceptionFlag':'normal', 'dedup.part_deduplication_flag':1, 'partProcessFlag':[0,2]}).limit(1000)


    def isSameVehi(self, t1_mileage, t1_price, t2_mileage, t2_price):
        if abs(t1_mileage - t2_mileage):
            if (abs(t1_price - t2_price)*100/min(t1_price, t2_price)) <= 5:
                return True
        return False


    def test(self):
        conn = pymongo.Connection()
        db = conn.kkche
        data = db.kkche.find_one()
        #strTime = data['mydate']
        #%Y-%m-%d
        '''
        print data['mydate'].strftime('%d')
        if data['mydate'].strftime('%Y') == "2014":
            print "yes"
        else:
            print "no"
        d1 = datetime.strftime(data['mydate'],'%Y-%m-%d')
        d2 = datetime.strftime(datetime.now(),'%Y-%m-%d')
        print d1
        print d2
        print datetime.strftime(data['mydate'],'%m')
        delta = data['mydate'] - datetime.now()
        print delta.days

        a = 1
        iList = self.genMatrix(2,2)
        for i in range(2):
            for j in range(2):
                iList[i][j] = a
                a += 1
                print iList[i][j]
        '''

        dict = {"b":[{"q":15,"w":16},{"r":3,"p":89}], "a":"1","c":"3", "e":"5", "d":"4"}
        #print dict['e']
        #if dict.has_key("a"):print "gg"
        for k,v in dict.items():
            if k=="b":
                #print v[0]['q']
                pass

        #print abs(1-2)
        #print min(7,9)

        iList = self.genMatrix(5, 5)
        '''
        for i in range(0, 5):
            for j in range(0, 5):
                if i%2 == 1 and j%2 == 1:
                    iList[i][j]=1
                else:
                    iList[i][j]=0
        '''
        iList[0][0] = 1
        iList[0][1] = 1
        iList[0][2] = 0
        iList[0][3] = 1
        iList[0][4] = 0
        iList[1][0] = 1
        iList[1][1] = 1
        iList[1][2] = 1
        iList[1][3] = 0
        iList[1][4] = 0
        iList[2][0] = 1
        iList[2][1] = 1
        iList[2][2] = 1
        iList[2][3] = 1
        iList[2][4] = 0
        iList[3][0] = 1
        iList[3][1] = 1
        iList[3][2] = 0
        iList[3][3] = 1
        iList[3][4] = 0
        iList[4][0] = 1
        iList[4][1] = 1
        iList[4][2] = 0
        iList[4][3] = 1
        iList[4][4] = 1

        print iList

        for i in range(0,4):
            for j in range(i+1, 5):
                if iList[i][j] == 1 and iList[j][j+1] == 1:
                    iList[i][j+1] = 1

        print iList


    def genMatrix(self,rows,cols):
        matrix = [[0 for col in range(cols)] for row in range(rows)]
        for i in range(rows):
            for j in range(cols):
                matrix[i][j] = 0
        return matrix

