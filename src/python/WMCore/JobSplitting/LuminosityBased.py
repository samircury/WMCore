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
        targetJobLength = int(kwargs.get('targetJobLength', None))
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

            if self.package == 'WMCore.WMBS':
                loadRunLumi = self.daoFactory(
                                    classname = "Files.GetBulkRunLumi")
                fileLumis = loadRunLumi.execute(files = fileList)
                for f in fileList:
                    lumiDict = fileLumis.get(f['id'], {})
                    for run in lumiDict.keys():
                        f.addRun(run = Run(run, *lumiDict[run]))

            # Now a very important note on why/how those loops are structured :
            # We don't really need to worry about iterating through the runs, 
            # as this is a reconstruction-only splitting algorithm, which 
            # guarantees that the input is always RAW data, which is bound to
            # T0 algorithms, those guarantee that RAW data has no file with multiple runs. 
            # __ all files will have 1 run __
            for f in fileList:
                currentEvent = f['first_event']
                eventsInFile = f['events']
                # Keeping this one just in case, but we know that we will have 1 run per file
                runs = list(f['runs'])
                run = runs[0].run
                #We got the runs, clean the file.
                #f['runs'] = set()
				
				# Unsure of what is files with parents and if it will be needed in PromptReco and ReReco
                #if getParents:
                #    parentLFNs = self.findParent(lfn = f['lfn'])
                #    for lfn in parentLFNs:
                #        parent = File(lfn = lfn)
                #        f['parents'].add(parent)
                
				#LOAD LUMINOSITY PER LS FROM DQM

                if not testDqmLuminosityPerLs: # If we have it beforehand is because the test sent it from the test file.
                    # Test if the curve is in the Cache before fecthing it from DQM
                    if not dqmLuminosityPerLsCache.has_key(run): 
                        dqmLuminosityPerLs = self.getLuminosityPerLsFromDQM(run)
                        dqmLuminosityPerLsCache[run] = dqmLuminosityPerLs
                else :
                    dqmLuminosityPerLs = testDqmLuminosityPerLs
                # At the end we should actually get it from the splitter args. That should be passed by the stdSpec.
				# LOAD PERFORMANCE CURVE FROM DASHBOARD, WIDE RANGE OR RUN RANGE? (FALLBACK TO DQM? DO NOT THINK SO, NEEDS RUN NUMBER)
        		#minLuminosity
        		#maxLuminosity
        		#
                if not testPerfCurve: # If we have it forehand is because the test sent it from the test file.
                    # Test if the curve is in the Cache before fecthing it from DQM
                    if not perfCurveCache.has_key(cmsswversion+primaryDataset): 
                        perfCurve = self.getPerfCurve(cmsswversion, primaryDataset)
                        perfCurveCache[cmsswversion+primaryDataset] = perfCurve
                    #perfCurve = self.getPerfCurve(cmsswversion, primaryDataset)
                else :
                    perfCurve = testPerfCurve

                # Now we got everything :
                #  * Lumi range of file
                #  * All sorts of information (luminosity, perf)
                # So we should do the following :
                #  * Get avg luminosity of the file range
                avgLumi = self.getFileAvgLuminosity(f, dqmLuminosityPerLs)
                #  * Get closest point in the curve, if multiple, average. Acceptable range should be defined
                # Interesting feature here : if it finds too much points with the given precision (3rd param)
                # It will call itself again, lowering the precision by 0.1 steps until it finds less than 5, more than 2 points
                # This way we can have a much more precise range into the curve, if there is a lot of data
                fileTimePerEvent = self.getFileTimePerEvent(avgLumi, perfCurve, 0.10)
                #  * If this can't be found, (not enough data somewhere, use timePerEvent)
                if fileTimePerEvent == 0 :
                    fileTimePerEvent = timePerEvent
                #  * Get the TpE and find how much eventsPerJob we want for this file.
                eventsPerJob = int(targetJobLength/fileTimePerEvent)
                # This should become a logging.debug message!
                print "This file has average instantaneous luminosity %f average time per event %f and is getting %i events per job" % (avgLumi, fileTimePerEvent, eventsPerJob)
                # 
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

    def getFileAvgLuminosity(self, f, dqmLuminosityPerLs):
        runs = list(f['runs'])
        lumis = runs[0].lumis
        # find a way to get rid of this totalLumi
        totalLumi = 0 
        for lumiSection in lumis :
            totalLumi += dqmLuminosityPerLs[lumiSection]
        avgLumi = totalLumi/float(len(lumis))
        # This is totally weird because we should have 23s lumiSections, not 46, however the number we get with 23 doesnt make sense, have a look here :
        # https://github.com/samircury/WMCore/blob/b34e8fa4922e51909f0addccfcd5883641f72a82/src/python/WMComponent/TaskArchiver/TaskArchiverPoller.py#L989
        return avgLumi*46
            
    def getFileTimePerEvent(self, avgLumi, perfCurve, precision = 0.1, enoughPrecision = False):
        # FIXME: Would be very interesting to have an algorithm that recursively increases 
        # precision if too much points are found (until it finds 5 or less points)
        # To have a better notion, run the unit test while printing len(interestingPoints)

        # Find points in a range of 5% of the avgLumi
        stdDev = int(avgLumi*precision)
        print "precision : %f" % precision

        interestingPoints = list()
        #print "now going to search for %i" % avgLumi
        for point in perfCurve :
            #pdb.set_trace()
            if point[0] > avgLumi-stdDev and point[0] < avgLumi+stdDev :
                interestingPoints.append(point)                
        print len(interestingPoints)
        if len(interestingPoints) == 0 :
            if precision != 0.1 :
                self.getFileTimePerEvent(avgLumi, perfCurve, precision = precision+0.01, enoughPrecision = True)
            return 0
        if len(interestingPoints) > 5 and precision > 0.01 and enoughPrecision == False:
            self.getFileTimePerEvent(avgLumi, perfCurve, precision = precision-0.01)
                #print "found %i" % point[0]
        totalSec = 0
        for point in interestingPoints:
            totalSec += point[1]
        avgTPE = totalSec/len(interestingPoints)
           
        return avgTPE

    def getLuminosityPerLsFromDQM(self, run):
        
        # Get the proxy, as CMSWEB doesn't allow us to use plain HTTP
        hostCert = os.getenv("X509_USER_PROXY")
        hostKey  = hostCert
        dqmUrl = "https://cmsweb.cern.ch/dqm/online/"
        # it seems that curl -k works, but as we already have everything, I will just provide it
        
        # Make function to fetch this from DQM. Returning Null or False if it fails
        getUrl = "%sjsonfairy/archive/%s/Global/Online/ALL/PixelLumi/PixelLumiDqmZeroBias/totalPixelLumiByLS" % (dqmUrl, str(run))
#        getUrl = "%sjsonfairy/archive/%s/DQM/TimerService/event_byluminosity" % (dqmUrl, run)
        #logging.info("Requesting performance information from %s" % getUrl)
        print "Requesting performance information from %s" % getUrl
        
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
    
