# -*- coding:utf-8 -*-
__author__ = 'tangmash'
import checkData
from multiprocessing import Process
import pymongo
import logging
import logging.handlers

def pachong():
    conn = pymongo.Connection()
    db = conn.kkcrawler
    while True:
        try:
            res = extract(db)
            if res is True:
                continue
            else:
                break
        except Exception as e:
            print e
        #extract(db)

def extract(db):
    data = db.base_vehicle_source_zhuang.find_and_modify({'flag.is_exception':None}, update={'$set':{'flag.is_exception':False}}, upsert=True)
    #data = db.base_vehicle_source_zhuang.find_one({'flag.is_exception':None})
    if data == None:
        return False
    try:
        '''
        if checkData.Checkdata(data, db).checkdata():
            db.base_vehicle_source_zhuang.update({'_id':data['_id']},{'$set':{'flag.is_exception':False}})
        '''
        checkData.Checkdata(data, db).checkdata()
        #logg("success: " + str(data['_id']))
        #db.base_vehicle_source_zhuang.update({'flag.is_exception':False},{'$set':{'flag.is_exception':None}})
        #db.base_vehicle_source_zhuang.update({'flag.is_exception':True},{'$set':{'flag.is_exception':None}})
        return True

    except Exception as e:
        id = data['_id']
        #logg("fail: " + str(id))
        #db.base_vehicle_source_zhuang.find_and_modify({'_id':id},update={'$set':{'flag.is_exception':None}},upsert=True)
        print e,id
        return True

def logg(strData):
    LOG_FILE = 'tst.log'
    handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes = 4096*4096, backupCount = 5) # 实例化handler
    fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
    formatter = logging.Formatter(fmt)   # 实例化formatter
    handler.setFormatter(formatter)      # 为handler添加formatter
    logger = logging.getLogger('tst')    # 获取名为tst的logger
    logger.addHandler(handler)           # 为logger添加handler
    logger.setLevel(logging.DEBUG)
    logger.info(strData)

if __name__ == '__main__':
    processes = []
    for i in range(10):
        p = Process(target=pachong, args=())
        p.start()
        processes.append(p)
    for p in processes:
        p.join()

    '''
    initId = ''
    processes = []
    obj = a.find_one({'_id':{'$gt':initId}})
    while obj:
        if len(processes)<10:
            p = Process(target=oneJob, args=(obj))
            p.start()
            processes.append(p)
            initId = obj['_id']
            obj = a.find_one({'_id':{'$gt':initId}})

        for p in processes:
            if p.complete:
                processes.pop(p)
    '''


