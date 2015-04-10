#!/bin/python
#encoding=utf8

import urllib
import urllib2
import requests
import json
import logging
import os
from db import *


app_master="http://YARN_HOST:8088"
logging.basicConfig(filename = os.path.join(os.getcwd(), 'run.log'), level = logging.DEBUG,format = '%(asctime)s - %(levelname)s: %(message)s')
#"No","ApplicationId","Used Memory","Queue","Submitter","VCores","RunningContainer","JobName")
title_item_format = "{:^3s}\t{:^40s}\t{:^10s}\t{:^25s}\t{:^20s}\t{:^10s}\t{:^10s}\t{:<40s}"
#Return the items of json
def rtnJsonItems(url,root,subitem):
        response = requests.get(url)
        json_items = json.loads(response.text)

        items = json_items[root][subitem]
        return items

#usedMemoryBytes=0
#Return the job counters
def getJobCounter(appId):
        url = app_master+"/proxy/{appId}/ws/v1/mapreduce/jobs".format(appId=appId)
        #response = requests.get(url)
        #json_jobs = json.loads(response.text)

        
        jobs = rtnJsonItems(url,"jobs","job")
        if len(jobs)==1:
                jobId = jobs[0]["id"]
                url = app_master+"/proxy/{appId}/ws/v1/mapreduce/jobs/{jobId}/counters".format(appId=appId,jobId=jobId)
                counterGroups = rtnJsonItems(url,"jobCounters","counterGroup")

                dictCounterGroups = {counterGroup["counterGroupName"]:counterGroup for counterGroup in counterGroups}
                
                print dictCounterGroups
                taskCounter = {counter["name"]:counter for counter in dictCounterGroups["org.apache.hadoop.mapreduce.TaskCounter"]["counter"]}
                memoryBytes = taskCounter["PHYSICAL_MEMORY_BYTES"]["totalCounterValue"]
                return (memoryBytes)
        elif len(jobs)>1:
                print len(jobs)
                return (0)
        else:
                print "None Results"
                return (0)

insert_flag = time.strftime("%Y%m%d%H%M", time.localtime())
def start():
        url = app_master+'/ws/v1/cluster/apps?state=RUNNING'
        apps = rtnJsonItems(url,"apps","app")
        usedMemoryBytes = 0
        usedVcores = 0
        usedContainer =0
        i=0
        apps = sorted(apps,key=lambda app:app["allocatedMB"],reverse=True)

        db = openConnection()

        for app in apps:
                try:
                       
                        i+=1

                        print (title_item_format.format(str(i),app["id"],str(app["allocatedMB"]),app["queue"],app["user"],str(app["allocatedVCores"]),str(app["runningContainers"]),app["name"]))
                        sql = "insert hdp_realtime_jobmonitor (name,app_id,used_memory,queue_name," \
                                        "submitter,used_vcores,running_container,time,insert_flag) values (%s,%s,%s,%s,%s,%s,%s,now(),%s)"

                        usedMemoryBytes += app["allocatedMB"]
                        usedVcores += app["allocatedVCores"]
                        usedContainer += app["runningContainers"]
						
                        #executeSQL(db,sql,(app["name"],app["id"],app["allocatedMB"],app["queue"],app["user"],app["allocatedVCores"],app["runningContainers"],insert_flag))
                        
                except Exception,ex:
                        logging.error(ex)
                        continue

        closeConnection(db)
        return (usedMemoryBytes,usedVcores,usedContainer)
logging.info("----START JOB MONITOR----")
print ("------------------------------------------"+insert_flag+"----------------------------------------------------------------------")
print (title_item_format.format("No","ApplicationId","UsedMemory","Queue","Submitter","VCores","Container","JobName"))
(counterValue,counterVcores,counterContainer) = start()

#counterValue = getJobCounter("application_1421180010019_143210")
print ("------------------------------------------------------------------------------------------------------------------------------")
print ("Total used %4.2f TB (%s MB) memories,total used %s cores,total containers %s") % (counterValue*1.00/1024/1024,counterValue,counterVcores,counterContainer)

logging.info("----END JOB MONITOR----")
