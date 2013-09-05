#!/usr/bin/env python
"""
_LuminosityBased_

Luminosity based splitting algorithm that will adapt the number of events in a job according to already known
event processing time, information which is collected by the monitoring systems. Complete discussion at :

https://hypernews.cern.ch/HyperNews/CMS/get/wmDevelopment/499.html

#Event based splitting algorithm that will chop a fileset into
#a set of jobs based on event counts
"""

import pdb
import re
import os
import httplib
import json
import logging
import traceback
from math import ceil

from WMCore.JobSplitting.JobFactory import JobFactory
from WMCore.WMBS.File import File
from WMCore.DataStructs.Run import Run

class LuminosityBased(JobFactory):
    """
    Split jobs by number of events
    """
    def algorithm(self, *args, **kwargs):
        """
        _algorithm_

        An event base splitting algorithm.  All available files are split into a
        set number of events per job.
        """
        
        eventsPerJob = int(kwargs.get("events_per_job", 100))
        eventsPerLumi = int(kwargs.get("events_per_lumi", eventsPerJob))
        getParents   = kwargs.get("include_parents", False)
        lheInput = kwargs.get("lheInputFiles", False)
        collectionName  = kwargs.get('collectionName', None)
        primaryDataset  = kwargs.get('primaryDataset', None)
        cmsswversion  = kwargs.get('cmsswversion', "CMSSW_5_3_8_patch3")
        testDqmLuminosityPerLs = kwargs.get('testDqmLuminosityPerLs', None)
        testPerfCurve = kwargs.get('testPerfCurve', None)
        minLuminosity = int(kwargs.get('minLuminosity', 1))
        maxLuminosity = int(kwargs.get('maxLuminosity', 9000))
        timePerEvent, sizePerEvent, memoryRequirement = \
                    self.getPerformanceParameters(kwargs.get('performance', {}))
        acdcFileList = []

        # If we have runLumi info, we need to load it from couch
        if collectionName:
            try:
                from WMCore.ACDC.DataCollectionService import DataCollectionService
                couchURL       = kwargs.get('couchURL')
                couchDB        = kwargs.get('couchDB')
                filesetName    = kwargs.get('filesetName')
                collectionName = kwargs.get('collectionName')
                owner          = kwargs.get('owner')
                group          = kwargs.get('group')
                logging.info('Creating jobs for ACDC fileset %s' % filesetName)
                dcs = DataCollectionService(couchURL, couchDB)
                acdcFileList = dcs.getProductionACDCInfo(collectionName, filesetName, owner, group)
            except Exception, ex:
                msg =  "Exception while trying to load goodRunList\n"
                msg +=  "Refusing to create any jobs.\n"
                msg += str(ex)
                msg += str(traceback.format_exc())
                logging.error(msg)
                return

        totalJobs    = 0

        locationDict = self.sortByLocation()
        perfCurveCache = {}
        dqmLuminosityPerLsCache = {}
        for location in locationDict:
            self.newGroup()
            fileList = locationDict[location]
            getRunLumiInformation = True
            #for f in fileList:
            #    if f['lfn'].startswith("MCFakeFile"):
            #        #We have one MCFakeFile, then it needs run information
            #        getRunLumiInformation = True
            #        break
            if getRunLumiInformation:
                if self.package == 'WMCore.WMBS':
                    loadRunLumi = self.daoFactory(
                                        classname = "Files.GetBulkRunLumi")
                    fileLumis = loadRunLumi.execute(files = fileList)
                    for f in fileList:
                        lumiDict = fileLumis.get(f['id'], {})
                        for run in lumiDict.keys():
                            f.addRun(run = Run(run, *lumiDict[run]))
            for f in fileList:
                currentEvent = f['first_event']
                eventsInFile = f['events']
                runs = list(f['runs'])
                #We got the runs, clean the file.
                f['runs'] = set()
				
				# Unsure of what is files with parents and if it will be needed in PromptReco and ReReco
                #if getParents:
                #    parentLFNs = self.findParent(lfn = f['lfn'])
                #    for lfn in parentLFNs:
                #        parent = File(lfn = lfn)
                #        f['parents'].add(parent)
                
				#LOAD LUMINOSITY PER LS FROM DQM

                if not testDqmLuminosityPerLs: # If we have it beforehand is because the test sent it from the test file.
                    # Test if the curve is in the Cache before fecthing it from DQM
                    if not dqmLuminosityPerLsCache[run]: 
                        dqmLuminosityPerLs = self.getLuminosityPerLsFromDQM(run)
                    else :
                        dqmLuminosityPerLs = dqmLuminosityPerLsCache[run]
                else :
                    dqmLuminosityPerLs = testDqmLuminosityPerLs
                # At the end we should actually get it from the splitter args. That should be passed by the stdSpec.
				# LOAD PERFORMANCE CURVE FROM DASHBOARD, WIDE RANGE OR RUN RANGE? (FALLBACK TO DQM? DO NOT THINK SO, NEEDS RUN NUMBER)
        		#minLuminosity
        		#maxLuminosity
        		#
                if not testPerfCurve: # If we have it forehand is because the test sent it from the test file.
                    perfCurve = self.getPerfCurve(cmsswversion, primaryDataset)
                else :
                    perfCurve = testPerfCurve
                #pdb.set_trace()
                # DEFAULT TPE IS MANDATORY!!!
                # NOW, WE HAVE THIS "F" OBJECT HERE, THAT IS THE FILE. IN THE RUN/LUMI DICTIONARY WE SHOULD BE ABLE TO KNOW ITS LUMINOSITY. 
                # IN THE TEST, THE FILE HAS NO RUN/LUMI. ADD THIS FIRST TO THE FAKE RUN.
				# 
				# TUNE EVENTS PER JOB ACCORDING TO DESIRED JOB LENGTH. CONFIGURABLE, DEFAULTS TO 8H
				# 
				# UPDATE EVENTS PER JOB HERE

                if not f['lfn'].startswith("MCFakeFile"):
                    #Then we know for sure it is not a MCFakeFile, so process
                    #it as usual
                    if eventsInFile >= eventsPerJob:
                        while currentEvent < eventsInFile:
                            self.newJob(name = self.getJobName(length=totalJobs))
                            self.currentJob.addFile(f)
                            if eventsPerJob + currentEvent < eventsInFile:
                                jobTime = eventsPerJob * timePerEvent
                                diskRequired = eventsPerJob * sizePerEvent
                                self.currentJob["mask"].setMaxAndSkipEvents(eventsPerJob, currentEvent)
                            else:
                                jobTime = (eventsInFile - currentEvent) * timePerEvent
                                diskRequired = (eventsInFile - currentEvent) * sizePerEvent
                                self.currentJob["mask"].setMaxAndSkipEvents(None,
                                                                            currentEvent)
                            self.currentJob.addResourceEstimates(jobTime = jobTime,
                                                                 memory = memoryRequirement,
                                                                 disk = diskRequired)
                            currentEvent += eventsPerJob
                            totalJobs    += 1
                    else:
                        self.newJob(name = self.getJobName(length=totalJobs))
                        self.currentJob.addFile(f)
                        jobTime = eventsInFile * timePerEvent
                        diskRequired = eventsInFile * sizePerEvent
                        self.currentJob.addResourceEstimates(jobTime = jobTime,
                                                             memory = memoryRequirement,
                                                             disk = diskRequired)
                        totalJobs += 1
                else:
                	# DO we need to worry about ACDC? Likely, but then the IF condition should be based in the ACDCFILELIST variable and not random FakeMC file
                    if acdcFileList:
                        if f['lfn'] in [x['lfn'] for x in acdcFileList]:
                            totalJobs = self.createACDCJobs(f, acdcFileList,
                                                            timePerEvent, sizePerEvent, memoryRequirement,
                                                            lheInput, totalJobs)
                        continue

    def createACDCJobs(self, fakeFile, acdcFileInfo,
                       timePerEvent, sizePerEvent, memoryRequirement,
                       lheInputOption, totalJobs = 0):
        """
        _createACDCJobs_

        Create ACDC production jobs, this are treated differentely
        since it is an exact copy of the failed jobs.
        """
        for acdcFile in acdcFileInfo:
            if fakeFile['lfn'] == acdcFile['lfn']:
                self.newJob(name = self.getJobName(length = totalJobs))
                self.currentJob.addBaggageParameter("lheInputFiles", lheInputOption)
                self.currentJob.addFile(fakeFile)
                self.currentJob["mask"].setMaxAndSkipEvents(acdcFile["events"],
                                                            acdcFile["first_event"])
                self.currentJob["mask"].setMaxAndSkipLumis(len(acdcFile["lumis"]) - 1,
                                                           acdcFile["lumis"][0])
                jobTime = (acdcFile["events"] - acdcFile["first_event"] + 1) * timePerEvent
                diskRequired = (acdcFile["events"] - acdcFile["first_event"] + 1) * sizePerEvent
                self.currentJob.addResourceEstimates(jobTime = jobTime,
                                                     memory = memoryRequirement,
                                                     disk = diskRequired)
                totalJobs += 1
        return totalJobs
    
    def getPerformanceFromDQM(self, run):
        
        # Get the proxy, as CMSWEB doesn't allow us to use plain HTTP
        hostCert = os.getenv("X509_USER_PROXY")
        hostKey  = hostCert
        dqmUrl = "fakeDQMUrl"
        # it seems that curl -k works, but as we already have everything, I will just provide it
        
        # Make function to fetch this from DQM. Returning Null or False if it fails
        getUrl = "%sjsonfairy/archive/%s/DQM/TimerService/event_byluminosity" % (dqmUrl, run)
        logging.debug("Requesting performance information from %s" % getUrl)
        
        regExp=re.compile('https://(.*)(/dqm.+)')
        regExpResult = regExp.match(getUrl)
        dqmHost = regExpResult.group(1)
        dqmPath = regExpResult.group(2)
        
        connection = httplib.HTTPSConnection(dqmHost, 443, hostKey, hostCert)
        connection.request('GET', dqmPath)
        response = connection.getresponse()
        responseData = response.read()
        responseJSON = json.loads(responseData)
        if not responseJSON["hist"]["bins"].has_key("content") :
            logging.info("Actually got a JSON from DQM perf in for run %d  , but content was bad, Bailing out"
                         % run)
            return False
        logging.debug("We have the performance curve")
        return responseJSON

    def getPerfCurve(self, cmsswversion, primaryDataset):
        return "a"
    
