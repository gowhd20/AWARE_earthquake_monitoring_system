import socket
import sys
import pdb
import threading
#import json
import time
#import base64
import os
#import mysql.connector as connector
#from mysql.connector import Error
from numpy import *
from math import *
#from Crypto.Cipher import AES
from random import *

### calculate distance between two points of coordinate ###
class GridMethods:
    def getDistance(self, lat1, lat2, lon1, lon2):
        R = 6371000
    
        deg1 = radians(float(lat1))
        deg2 = radians(float(lat2))
        lon = radians(float(lon2) - float(lon1))
        dis = acos(sin(deg1)*sin(deg2)+cos(deg1)*cos(deg2)*cos(lon))*R
    
        return dis
        
    def getResult(self):
        global arr
        global counter_copy
        global is_isolated
        is_isolated = 0        
        maxCount = 1
        centerPointArr = []
        dataToStore = []        

        getDis = GridMethods()
        arr_result = copy(arr)
        #print arr_result
#        if arr_result.shape[0] <= 1:
#            is_isolated = 1   ### label an isolated event ###
#            print 'There was an isolated event has happened'
        
#        else:
            ### compare all elements in the array and increase number of events belonging to each grid when it has ###
        print counter_copy            
        for i in range(counter_copy - 1):
            for j in range(counter_copy - (i+1)):
                dis = getDis.getDistance(arr_result[i, 2], arr_result[i+j+1, 2], arr_result[i, 3], arr_result[i+j+1, 3])
                
                ### if an event belongs to another, increase the number of event in both ###
                if dis < 1000:
                   a = int(arr_result[i, 4])
                   a += 1
                   arr_result[i, 4] = str(a)

                   a = int(arr_result[i+j+1, 4])
                   a += 1
                   arr_result[i+j+1, 4] = str(a)
       
        for t in range(counter_copy):
            b = int(arr_result[t, 4])
#            if b == 1:
#                print 'device id : ' + arr_result[t, 0] + ' with coordinate ' + arr_result[t, 2], arr_result[t, 3] + ' was an isolated event'
            if b >= maxCount:
                maxCount = b
                centerPointArr = array([[arr_result[t, 0], arr_result[t, 2], arr_result[t, 3], arr_result[t, 4]]])
                dataToStore = array([arr_result[t, 0], arr_result[t, 1], arr_result[t, 2], arr_result[t, 3], arr_result[t, 4], arr_result[t, 5]])
            elif b == maxCount:
                centerPointArr = append(centerPointArr, [[arr_result[t, 0], arr_result[t, 2], arr_result[t, 3], arr_result[t, 4]]], axis=0)
                    
        #print centerPointArr
        #print dataToStore
        #if maxCount >= 1:
            #if(dataToStore.size != 0):
            #    EventMethods().storeResultData(dataToStore)        
            #Broadcast().broadcast(centerPointArr)
        return arr_result   ### return result array ###
   
        
### insert data into a local database ###
class EventMethods:     
    def storeData(self, data_received):
        mysqldata = JsonMethods().stripJsonObject(data_received)
        try:
            con = connector.connect(user='acp', password='secretpassword', host='localhost', database='acp')
            if con.is_connected():
                print 'Db connected to store event data'
        
            query = '''replace into client(device_id, timestamp, longitude, latitude) values (%s, %s, %s, %s)''', (mysqldata["device id"],mysqldata["timestamp"],mysqldata["longitude"],mysqldata["latitude"])

            curr = con.cursor()
            curr.execute(*query)
            con.commit()
            curr.close()
            con.close()
        
            return mysqldata
        except Error as e:
            print(e)
            
    def storeResultData(self, data_after_event):
        try:
            con = connector.connect(user='acp', password='secretpassword', host='localhost', database='acp')
            if con.is_connected():
                print 'Db connected to store result data'
            
            if data_after_event.size > 6:
                for i in range(data_after_event.size/6):
                    query = '''replace into earthquake_event(device_id, timestamp, latitude, longitude, count_of_events, counter) values (%s, %s, %s, %s, %s, %s)''', (str(data_after_event[i][0]), int(data_after_event[i][1]), float(data_after_event[i][2]), float(data_after_event[i][3]), int(data_after_event[i][4]), int(data_after_event[i][5]))
                    curr = con.cursor()
                    curr.execute(*query)
                    con.commit()
            else:           
                query = '''replace into earthquake_event(device_id, timestamp, latitude, longitude, count_of_events, counter) values (%s, %s, %s, %s, %s, %s)''', (str(data_after_event[0]), int(data_after_event[1]), float(data_after_event[2]), float(data_after_event[3]), int(data_after_event[4]), int(data_after_event[5]))
                curr = con.cursor()
                curr.execute(*query)
                con.commit()
                curr.close()
                con.close()

        except Error as e:
            print(e)

    
class JsonMethods:
    def createJsonObjFromArr(self, array):
        jsonObj = json.dumps(array.tolist())
        return jsonObj
    
    def createJsonObject(self, data):
        jsonObj = json.dumps(data)
        return jsonObj

    def stripJsonObject(self, data):
        stripData = json.loads(data)
        return stripData


class HashFile:
    def insertToFile(self, id_data, soc, addr):
        global countSoc
        global idFile
        global socFile
        global addrFile
        global fileCode
        global keyFile

        print id_data["device id"]

        hashName = 'soc_id_' + str(countSoc)
        if countSoc == 0:
            idFile = {hashName : id_data["device id"]}
            socFile = {hashName : soc}
            addrFile = {hashName : addr}
            fileCode = {id_data["device id"] : hashName} ### to retrieve data by device id ###
            countSoc += 1
        else:
            ### update when a new device sent data ###
            if fileCode.get(id_data["device id"], 'empty') == 'empty':
                idFile.update({hashName : id_data["device id"]})
                socFile.update({hashName : soc})
                addrFile.update({hashName : addr})
                fileCode.update({id_data["device id"] : hashName})
                countSoc += 1
            ### up to date soc, address for exist device ### 
            else:
                hash_id = fileCode[id_data["device id"]]
                socFile[hash_id] = soc
                addrFile[hash_id] = addr

       
### broadcast message to all clients###
class Broadcast:
    def broadcast(self, centerData):
        global countSoc
        global socFile
        global addrFile
        global idFile

        if countSoc > 0:
           # test = array([[65, 25]])
            jsonObjData = JsonMethods().createJsonObjFromArr(centerData)
           # newSoc = SocketMethods().createSocket()
            for k in range(countSoc):
                hashName = 'soc_id_' + str(k)
                newSoc = socFile[hashName]   ### retrieve socket ###
                newAddr = addrFile[hashName]
                newSoc.sendto(jsonObjData, newAddr)
                print 'Broadcast message sent to device id : ' + idFile[hashName] 
                print newAddr
                print hashName
            
            
class SocketMethods:
    def createSocket(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
  
            print 'Socket created' 

        except socket.error, msg:
	        print 'Failed to create socket. Error Code : ' + str(msg[0]) + ' Message ' + msg
	        sys.exit()
        return s

    def socketBind(self, soc, host, port_rec):        
        try:
            soc.bind((host, port_rec))
    
        except socket.error ,msg:
	        print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
	        sys.exit()

        print 'Socket bind complete'
        return soc

            
### thread managing time window ###             
class TimeCountThread(threading.Thread):
    def run(self):
        global on_event
        global counter
        global counter_copy
        t_begin = time.time()
        
        while(1):
            if (time.time() - t_begin >= 10):
                on_event = 0   ### no time window is opened ###
                print counter_copy
                counter_copy = counter  ### to avoid re-set counter while calculating ###
                counter = 0   
                print 'time window breaks'
                break
                
        print(GridMethods().getResult())  ### call result methods ###
 
 
### main thread, receiving data and start() other thread when data comes ###
class MainThread(threading.Thread):
    def run(self):
        global socFile
        global addrFile
        global on_event
        global arr
        global counter
        global countSoc
        global idFile
       
        filter_id = ''
        on_event = 0
        counter = 0
        countSoc = 0
        #m_event = EventMethods()
        
        while(1):
            #s = SocketMethods().createSocket()
            #s_bound = SocketMethods().socketBind(s, host, port_rec)
	        
            #d = s_bound.recvfrom(2048)
            #data = d[0]
            #addr = d[1]
            lat = uniform(-90.0, 90.0)
            lon = uniform(-180.0, 180.0)
            dv_id = randint(100000000000000,999999999999999)
            timestamp = time.time()
            #if data:
                #if len(data) > 70:  ### general size -> 102, 103, 104 ###
                #print len(data)
                #print data 
            
                ### if first event ###
            if on_event == 0:
                on_event = 1
                counter += 1
                #sqlData = m_event.storeData(data)
                #arr = array([[sqlData["device id"], sqlData["timestamp"], sqlData["latitude"], sqlData["longitude"], 1, counter]])
                arr = array([[dv_id, timestamp, lat, lon, 1, counter]])
                filter_id = dv_id                    
                t_thread = TimeCountThread()
                t_thread.start()
            
            ### if time window is already opened ###
            else:
                #sqlData = JsonMethods().stripJsonObject(data)
                
                if not filter_id == dv_id:
                    counter += 1
                    #sqlData = m_event.storeData(data)
                    #arr = append(arr, [[sqlData["device id"], sqlData["timestamp"], sqlData["latitude"], sqlData["longitude"], 1, counter]], axis=0)
                    arr = append(arr, [[dv_id, timestamp, lat, lon, 1, counter]], axis=0)
                    #print counter
                    filter_id = dv_id

                #elif len(data) < 70: ### general size -> 31
                   # s_bound.sendto(securedData, addr)                    
                    #id_data = JsonMethods().stripJsonObject(data)
                    #HashFile().insertToFile(id_data, s, addr)
                    #print data
                    #print idFile
                    #print socFile
                    #print addrFile
                    #print countSoc
                    #print fileCode
              
#            s.close()    
#        s.close()

            
### main ###          
if __name__ == '__main__':
    host = '85.23.168.159'
    port_rec = 80
    port_send = 90  

    mThread = MainThread()
    mThread.start()


