#!/usr/bin/env python
"""
_CondorPlugin_

Example of Condor plugin
For glide-in use.
"""

import os
import re
import time
import Queue
import os.path
import logging
import threading
import traceback
import subprocess
import multiprocessing

import WMCore.Algorithms.BasicAlgos as BasicAlgos

from WMCore.Credential.Proxy           import Proxy
from WMCore.DAOFactory                 import DAOFactory
from WMCore.WMInit                     import getWMBASE
from WMCore.BossAir.Plugins.BasePlugin import BasePlugin, BossAirPluginException
from WMCore.FwkJobReport.Report        import Report
from WMCore.Algorithms                 import SubprocessAlgos

def submitWorker(input, results, timeout = None):
    """
    _outputWorker_

    Runs a subprocessed command.

    This takes whatever you send it (a single ID)
    executes the command
    and then returns the stdout result

    I planned this to do a glite-job-output command
    in massive parallel, possibly using the bulkID
    instead of the gridID.  Either way, all you have
    to change is the command here, and what is send in
    in the complete() function.
    """

    # Get this started
    while True:
        try:
            work = input.get()
        except (EOFError, IOError), ex:
            crashMessage = "Hit EOF/IO in getting new work\n"
            crashMessage += "Assuming this is a graceful break attempt.\n"
            crashMessage += str(ex)
            logging.error(crashMessage)
            break
        except Exception, ex:
            msg =  "Hit unidentified exception getting work\n"
            msg += str(ex)
            msg += "Assuming everything's totally hosed.  Killing process.\n"
            logging.error(msg)
            break

        if work == 'STOP':
            # Put the brakes on
            logging.error("submitWorker multiprocess issued STOP command!")
            break

        command = work.get('command', None)
        idList  = work.get('idList', [])
        if not command:
            results.put({'stdout': '', 'stderr': '999100\n Got no command!', 'idList': idList})
            continue

        try:
            stdout, stderr, returnCode = SubprocessAlgos.runCommand(cmd = command, shell = True, timeout = timeout)
            if returnCode == 0:
                results.put({'stdout': stdout, 'stderr': stderr, 'idList': idList})
            else:
                results.put({'stdout': stdout,
                             'stderr': 'Non-zero exit code: %s\n stderr: %s' % (returnCode, stderr),
                             'idList': idList})
        except Exception, ex:
            msg =  "Critical error in subprocess while submitting to condor"
            msg += str(ex)
            msg += str(traceback.format_exc())
            logging.error(msg)
            results.put({'stdout': '', 'stderr': '999101\n %s' % msg, 'idList': idList})

    return 0


def parseError(error):
    """
    Do some basic condor error parsing

    """

    errorCondition = True
    errorMsg       = error

    if 'ERROR: proxy has expired\n' in error:
        errorCondition = True
        errorMsg = 'CRITICAL ERROR: Your proxy has expired!\n'
    elif '999100\n' in error:
        errorCondition = True
        errorMsg = "CRITICAL ERROR: Failed to build submit command!\n"
    elif 'Failed to open command file' in error:
        errorCondition = True
        errorMsg = "CONDOR ERROR: jdl file not found by submitted jobs!\n"
    elif 'It appears that the value of pthread_mutex_init' in error:
        # glexec insists on spitting out to stderr
        lines = error.split('\n')
        if len(lines) == 2 and not lines[1]:
            errorCondition = False
            errorMsg = error

    return errorCondition, errorMsg





class CondorPlugin(BasePlugin):
    """
    _CondorPlugin_

    Condor plugin for glide-in submissions
    """

    @staticmethod
    def stateMap():
        """
        For a given name, return a global state


        """

        stateDict = {'New': 'Pending',
                     'Idle': 'Pending',
                     'Running': 'Running',
                     'Held': 'Error',
                     'Complete': 'Complete',
                     'Error': 'Error',
                     'Timeout': 'Error',
                     'Removed': 'Running',
                     'Unknown': 'Error'}

        # This call is optional but needs to for testing
        #BasePlugin.verifyState(stateDict)

        return stateDict

    def __init__(self, config):

        self.config = config

        BasePlugin.__init__(self, config)

        self.locationDict = {}

        myThread = threading.currentThread()
        daoFactory = DAOFactory(package="WMCore.WMBS", logger = myThread.logger,
                                dbinterface = myThread.dbi)
        self.locationAction = daoFactory(classname = "Locations.GetSiteInfo")


        self.packageDir = None

        if os.path.exists(os.path.join(getWMBASE(),
                                       'src/python/WMCore/WMRuntime/Unpacker.py')):
            self.unpacker = os.path.join(getWMBASE(),
                                         'src/python/WMCore/WMRuntime/Unpacker.py')
        else:
            self.unpacker = os.path.join(getWMBASE(),
                                         'WMCore/WMRuntime/Unpacker.py')

        self.agent         = getattr(config.Agent, 'agentName', 'WMAgent')
        self.sandbox       = None
        self.scriptFile    = None
        self.submitDir     = None
        self.removeTime    = getattr(config.BossAir, 'removeTime', 60)
        self.multiTasks    = getattr(config.BossAir, 'multicoreTaskTypes', [])
        self.useGSite      = getattr(config.BossAir, 'useGLIDEINSites', False)
        self.submitWMSMode = getattr(config.BossAir, 'submitWMSMode', False)


        # Build ourselves a pool
        self.pool     = []
        self.input    = multiprocessing.Queue()
        self.result   = multiprocessing.Queue()
        self.nProcess = getattr(self.config.BossAir, 'nCondorProcesses', 4)

        # Set up my proxy and glexec stuff
        self.proxy      = None
        self.serverCert = getattr(config.BossAir, 'delegatedServerCert', None)
        self.serverKey  = getattr(config.BossAir, 'delegatedServerKey', None)
        self.myproxySrv = getattr(config.BossAir, 'myproxyServer', None)
        self.proxyDir   = getattr(config.BossAir, 'proxyDir', '/tmp/')
        self.serverHash = getattr(config.BossAir, 'delegatedServerHash', None)
        self.glexecPath = getattr(config.BossAir, 'glexecPath', None)
        self.jdlProxyFile    = None # Proxy name to put in JDL (owned by submit user)
        self.glexecProxyFile = None # Copy of same file owned by submit user

        if self.serverCert and self.serverKey and self.myproxySrv:
            self.proxy = self.setupMyProxy()

        # Build a request string
        self.reqStr = "(Memory >= 1 && OpSys == \"LINUX\" ) && (Arch == \"INTEL\" || Arch == \"X86_64\") && stringListMember(GLIDEIN_CMSSite, DESIRED_Sites)"
        if hasattr(config.BossAir, 'condorRequirementsString'):
            self.reqStr = config.BossAir.condorRequirementsString

        return


    def __del__(self):
        """
        __del__

        Trigger a close of connections if necessary
        """
        self.close()


    def setupMyProxy(self):
        """
        _setupMyProxy_

        Setup a WMCore.Credential.Proxy object with which to retrieve
        proxies from myproxy using the server Cert
        """

        args = {}
        args['server_cert'] = self.serverCert
        args['server_key']  = self.serverKey
        args['myProxySvr']  = self.myproxySrv
        args['credServerPath'] = self.proxyDir
        args['logger'] = logging
        return Proxy(args = args)


    def close(self):
        """
        _close_

        Kill all connections and terminate
        """
        terminate = False
        for x in self.pool:
            try:
                self.input.put('STOP')
            except Exception, ex:
                msg =  "Hit some exception in deletion\n"
                msg += str(ex)
                logging.error(msg)
                terminate = True
        self.input.close()
        self.result.close()
        for proc in self.pool:
            if terminate:
                try:
                    proc.terminate()
                except Exception, ex:
                    logging.error("Failure while attempting to terminate process")
                    logging.error(str(ex))
                    continue
            else:
                try:
                    proc.join()
                except Exception, ex:
                    try:
                        proc.terminate()
                    except Exception, ex2:
                        logging.error("Failure to join or terminate process")
                        logging.error(str(ex))
                        logging.error(str(ex2))
                        continue
        # At the end, clean the pool
        self.pool = []
        return



    def submit(self, jobs, info):
        """
        _submit_


        Submit jobs for one subscription
        """

        # If we're here, then we have submitter components
        self.scriptFile = self.config.JobSubmitter.submitScript
        self.submitDir  = self.config.JobSubmitter.submitDir
        timeout         = getattr(self.config.JobSubmitter, 'getTimeout', 300)

        if len(self.pool) == 0:
            # Starting things up
            # This is obviously a submit API
            logging.info("Starting up CondorPlugin worker pool")
            for x in range(self.nProcess):
                p = multiprocessing.Process(target = submitWorker,
                                            args = (self.input, self.result, timeout))
                p.start()
                self.pool.append(p)

        if not os.path.exists(self.submitDir):
            os.makedirs(self.submitDir)


        successfulJobs = []
        failedJobs     = []
        jdlFiles       = []

        if len(jobs) == 0:
            # Then we have nothing to do
            return successfulJobs, failedJobs



        # Now assume that what we get is the following; a mostly
        # unordered list of jobs with random sandboxes.
        # We intend to sort them by sandbox.

        submitDict = {}
        nSubmits   = 0
        for job in jobs:
            sandbox = job['sandbox']
            if not sandbox in submitDict.keys():
                submitDict[sandbox] = []
            submitDict[sandbox].append(job)


        # Now submit the bastards
        queueError = False
        for sandbox in submitDict.keys():
            jobList = submitDict.get(sandbox, [])
            idList = [x['jobid'] for x in jobList]
            if queueError:
                # If the queue has failed, then we must not process
                # any more jobs this cycle.
                continue
            while len(jobList) > 0:
                jobsReady = jobList[:self.config.JobSubmitter.jobsPerWorker]
                jobList   = jobList[self.config.JobSubmitter.jobsPerWorker:]
                idList    = [x['id'] for x in jobsReady]
                jdlList = self.makeSubmit(jobList = jobsReady)
                if not jdlList or jdlList == []:
                    # Then we got nothing
                    logging.error("No JDL file made!")
                    return {'NoResult': [0]}
                jdlFile = "%s/submit_%i_%i.jdl" % (self.submitDir, os.getpid(), idList[0])
                handle = open(jdlFile, 'w')
                handle.writelines(jdlList)
                handle.close()
                jdlFiles.append(jdlFile)

                # Now submit them
                logging.info("About to submit %i jobs" %(len(jobsReady)))
                if self.glexecPath:
                    command = 'CS=`which condor_submit`; export GLEXEC_CLIENT_CERT=%s; ' % self.glexecProxyFile + \
                              'export GLEXEC_SOURCE_PROXY=%s; export GLEXEC_TARGET_PROXY=%s; %s $CS %s' % \
                              (self.glexecProxyFile, self.jdlProxyFile, self.glexecPath, jdlFile)
                else:
                    command = "condor_submit %s" % jdlFile

                try:
                    self.input.put({'command': command, 'idList': idList})
                except AssertionError, ex:
                    msg =  "Critical error: input pipeline probably closed.\n"
                    msg += str(ex)
                    msg += "Error Procedure: Something critical has happened in the worker process\n"
                    msg += "We will now proceed to pull all useful data from the queue (if it exists)\n"
                    msg += "Then refresh the worker pool\n"
                    logging.error(msg)
                    queueError = True
                    break
                nSubmits += 1

        # Now we should have sent all jobs to be submitted
        # Going to do the rest of it now
        for n in range(nSubmits):
            try:
                res = self.result.get(block = True, timeout = timeout)
            except Queue.Empty:
                # If the queue was empty go to the next submit
                # Those jobs have vanished
                logging.error("Queue.Empty error received!")
                logging.error("This could indicate a critical condor error!")
                logging.error("However, no information of any use was obtained due to process failure.")
                logging.error("Either process failed, or process timed out after %s seconds." % timeout)
                queueError = True
                continue
            except AssertionError, ex:
                msg =  "Found Assertion error while retrieving output from worker process.\n"
                msg += str(ex)
                msg += "This indicates something critical happened to a worker process"
                msg += "We will recover what jobs we know were submitted, and resubmit the rest"
                msg += "Refreshing worker pool at end of loop"
                logging.error(msg)
                queueError = True
                continue
            
            output = res['stdout']
            error  = res['stderr']
            idList = res['idList']

            if not error == '':
                logging.error("Printing out command stderr")
                logging.error(error)
                errorCheck, errorMsg = parseError(error = error)
            else:
                errorCheck = None

            if errorCheck:
                condorErrorReport = Report()
                condorErrorReport.addError("JobSubmit", 61202, "CondorError", errorMsg)
                for jobID in idList:
                    for job in jobs:
                        if job.get('id', None) == jobID:
                            job['fwjr'] = condorErrorReport
                            failedJobs.append(job)
                            break
            else:
                for jobID in idList:
                    for job in jobs:
                        if job.get('id', None) == jobID:
                            successfulJobs.append(job)
                            break

        # Remove JDL files unless commanded otherwise
        if getattr(self.config.JobSubmitter, 'deleteJDLFiles', True):
            for f in jdlFiles:
                os.remove(f)

        # If the queue failed, clean the processes from the queue
        # This prevents jobs from building up in memory anywhere
        if queueError:
            logging.error("Purging worker pool due to previous queueError")
            self.close()


        # We must return a list of jobs successfully submitted,
        # and a list of jobs failed
        return successfulJobs, failedJobs




    def track(self, jobs, info = None):
        """
        _track_

        Track the jobs while in condor
        This returns a three-way ntuple
        First, the total number of jobs still running
        Second, the jobs that need to be changed
        Third, the jobs that need to be completed
        """


        # Create an object to store final info
        trackList = []

        changeList   = []
        completeList = []
        runningList  = []
        noInfoFlag   = False

        # Get the job
        jobInfo = self.getClassAds()
        if jobInfo == None:
            return runningList, changeList, completeList
        if len(jobInfo.keys()) == 0:
            noInfoFlag = True

        for job in jobs:
            # Now go over the jobs from WMBS and see what we have
            if not job['jobid'] in jobInfo.keys():
                # Two options here, either put in removed, or not
                # Only cycle through Removed if condor_q is sending
                # us no information
                if noInfoFlag:
                    if not job['status'] == 'Removed':
                        # If the job is not in removed, move it to removed
                        job['status']      = 'Removed'
                        job['status_time'] = int(time.time())
                        changeList.append(job)
                    elif time.time() - float(job['status_time']) > self.removeTime:
                        # If the job is in removed, and it's been missing for more
                        # then self.removeTime, remove it.
                        completeList.append(job)
                else:
                    completeList.append(job)
            else:
                jobAd     = jobInfo.get(job['jobid'])
                jobStatus = int(jobAd.get('JobStatus', 0))
                statName  = 'Unknown'
                if jobStatus == 1:
                    # Job is Idle, waiting for something to happen
                    statName = 'Idle'
                elif jobStatus == 5:
                    # Job is Held; experienced an error
                    statName = 'Held'
                elif jobStatus == 2 or jobStatus == 6:
                    # Job is Running, doing what it was supposed to
                    # NOTE: Status 6 is transferring output
                    # I'm going to list this as running for now because it fits.
                    statName = 'Running'
                elif jobStatus == 3:
                    # Job is in X-state: List as error
                    statName = 'Error'
                elif jobStatus == 4:
                    # Job is completed
                    statName = 'Complete'
                else:
                    # What state are we in?
                    logging.info("Job in unknown state %i" % jobStatus)

                # Get the global state
                job['globalState'] = CondorPlugin.stateMap()[statName]

                if statName != job['status']:
                    # Then the status has changed
                    job['status']      = statName
                    job['status_time'] = jobAd.get('stateTime', 0)
                    changeList.append(job)


                runningList.append(job)



        return runningList, changeList, completeList


    def complete(self, jobs):
        """
        Do any completion work required

        In this case, look for a returned logfile
        """

        for job in jobs:
            if job.get('cache_dir', None) == None or job.get('retry_count', None) == None:
                # Then we can't do anything
                logging.error("Can't find this job's cache_dir in CondorPlugin.complete")
                logging.error("cache_dir: %s" % job.get('cache_dir', 'Missing'))
                logging.error("retry_count: %s" % job.get('retry_count', 'Missing'))
                continue
            reportName = os.path.join(job['cache_dir'], 'Report.%i.pkl' % job['retry_count'])
            if os.path.isfile(reportName) and os.path.getsize(reportName) > 0:
                # Then we have a real report.
                # Do nothing
                continue
            if os.path.isdir(reportName):
                # Then something weird has happened.
                # File error, do nothing
                logging.error("Went to check on error report for job %i.  Found a directory instead.\n" % job['id'])
                logging.error("Ignoring this, but this is very strange.\n")

            # If we're still here, we must not have a real error report
            logOutput = 'Could not find jobReport\n'
            logPath = os.path.join(job['cache_dir'], 'condor.log')
            if os.path.isfile(logPath):
                logTail = BasicAlgos.tail(errLog, 50)
                logOutput += 'Adding end of condor.log to error message:\n'
                logOutput += logTail
            if not os.path.isdir(job['cache_dir']):
                msg =  "Serious Error in Completing condor job with id %s!\n" % job.get('id', 'unknown')
                msg += "Could not find jobCache directory - directory deleted under job: %s\n" % job['cache_dir']
                msg += "Creating artificial cache_dir for failed job report\n"
                logging.error(msg)
                os.makedirs(job['cache_dir'])
                logOutput += msg
                condorReport = Report()
                condorReport.addError("NoJobReport", 99304, "NoCacheDir", logOutput)
                condorReport.save(filename = reportName)
                continue
            condorReport = Report()
            condorReport.addError("NoJobReport", 99303, "NoJobReport", logOutput)
            condorReport.save(filename = reportName)
            logging.debug("No returning job report for job %i" % job['id'])


        return






    def kill(self, jobs, info = None):
        """
        Kill a list of jobs based on the WMBS job names

        """

        for job in jobs:
            jobID = job['jobid']
            # This is a very long and painful command to run
            command = 'condor_rm -constraint \"WMAgent_JobID =?= %i\"' % (jobID)
            proc = subprocess.Popen(command, stderr = subprocess.PIPE,
                                    stdout = subprocess.PIPE, shell = True)
            out, err = proc.communicate()

        return





    # Start with submit functions


    def initSubmit(self, jobList=None):
        """
        _makeConfig_

        Make common JDL header
        """
        jdl = []


        # -- scriptFile & Output/Error/Log filenames shortened to
        #    avoid condorg submission errors from > 256 character pathnames

        jdl.append("universe = vanilla\n")
        jdl.append("requirements = %s\n" % self.reqStr)
        #jdl.append("should_transfer_executable = TRUE\n")

        jdl.append("should_transfer_files = YES\n")
        jdl.append("when_to_transfer_output = ON_EXIT\n")
        jdl.append("log_xml = True\n" )
        jdl.append("notification = NEVER\n")
        jdl.append("Executable = %s\n" % self.scriptFile)
        jdl.append("Output = condor.$(Cluster).$(Process).out\n")
        jdl.append("Error = condor.$(Cluster).$(Process).err\n")
        jdl.append("Log = condor.$(Cluster).$(Process).log\n")
        # Things that are necessary for the glide-in

        jdl.append('+DESIRED_Archs = \"INTEL,X86_64\"\n')
        jdl.append("+WMAgent_AgentName = \"%s\"\n" %(self.agent))

        # Check for multicore
        if jobList[0].get('taskType', None) in self.multiTasks:
            jdl.append('+DESIRES_HTPC = True\n')
        else:
            jdl.append('+DESIRES_HTPC = False\n')

        if self.proxy:
            # Then we have to retrieve a proxy for this user
            job0   = jobList[0]
            userDN = job0.get('userdn', None)
            if not userDN:
                # Then we can't build ourselves a proxy
                logging.error("Asked to build myProxy plugin, but no userDN available!")
                logging.error("Checked job %i" % job0['id'])
                return jdl
            logging.error("Fetching proxy for %s" % userDN)
            # Build the proxy
            # First set the userDN of the Proxy object
            self.proxy.userDN = userDN
            # Second, get the actual proxy
            if self.serverHash:
                # If we built our own serverHash, we have to be able to send it in
                filename = self.proxy.logonRenewMyProxy(credServerName = self.serverHash)
            else:
                # Else, build the serverHash from the proxy sha1
                filename = self.proxy.logonRenewMyProxy()
            logging.error("Proxy stored in %s" % filename)
            if self.glexecPath:
                self.jdlProxyFile = '%s.user' % filename
                self.glexecProxyFile = filename
                command = 'export GLEXEC_CLIENT_CERT=%s; export GLEXEC_SOURCE_PROXY=%s; ' % \
                          (self.glexecProxyFile, self.glexecProxyFile) + \
                          'export GLEXEC_TARGET_PROXY=%s; %s /usr/bin/id' % \
                          (self.jdlProxyFile, self.glexecPath)
                proc = subprocess.Popen(command, stderr = subprocess.PIPE,
                                        stdout = subprocess.PIPE, shell = True)
                out, err = proc.communicate()
                logging.error("Created new user proxy with glexec %s" % self.jdlProxyFile)
            else:
                self.jdlProxyFile = filename
            jdl.append("x509userproxy = %s\n" % self.jdlProxyFile)

        return jdl




    def makeSubmit(self, jobList):
        """
        _makeSubmit_

        For a given job/cache/spec make a JDL fragment to submit the job

        """

        if len(jobList) < 1:
            #I don't know how we got here, but we did
            logging.error("No jobs passed to plugin")
            return None

        jdl = self.initSubmit(jobList)


        # For each script we have to do queue a separate directory, etc.
        for job in jobList:
            if job == {}:
                # Then I don't know how we got here either
                logging.error("Was passed a nonexistant job.  Ignoring")
                continue
            jdl.append("initialdir = %s\n" % job['cache_dir'])
            jdl.append("transfer_input_files = %s, %s/%s, %s\n" \
                       % (job['sandbox'], job['packageDir'],
                          'JobPackage.pkl', self.unpacker))
            argString = "arguments = %s %i\n" \
                        % (os.path.basename(job['sandbox']), job['id'])
            jdl.append(argString)

            jobCE = job['location']
            if not jobCE:
                # Then we ended up with a site that doesn't exist?
                logging.error("Job for non-existant site %s" \
                              % (job['location']))
                continue

            if self.useGSite:
                jdl.append('+GLIDEIN_CMSSite = \"%s\"\n' % (jobCE))
            if self.submitWMSMode and len(job.get('possibleSites', [])) > 0:
                strg = list(job.get('possibleSites')).__str__().lstrip('[').rstrip(']')
                strg = filter(lambda c: c not in "\'", strg)
                jdl.append('+DESIRED_Sites = \"%s\"\n' % strg)
            else:
                jdl.append('+DESIRED_Sites = \"%s\"\n' %(jobCE))

            

            # Transfer the output files
            jdl.append("transfer_output_files = Report.%i.pkl\n" % (job["retry_count"]))

            # Add priority if necessary
            if job.get('priority', None) != None:
                try:
                    prio = int(job['priority'])
                    jdl.append("priority = %i\n" % prio)
                except ValueError:
                    logging.error("Priority for job %i not castable to an int\n" % job['id'])
                    logging.error("Not setting priority")
                    logging.debug("Priority: %s" % job['priority'])
                except Exception, ex:
                    logging.error("Got unhandled exception while setting priority for job %i\n" % job['id'])
                    logging.error(str(ex))
                    logging.error("Not setting priority")

            jdl.append("+WMAgent_JobID = %s\n" % job['jobid'])

            jdl.append("Queue 1\n")

        return jdl





    def getCEName(self, jobSite):
        """
        _getCEName_

        This is how you get the name of a CE for a job
        """

        if not jobSite in self.locationDict.keys():
            siteInfo = self.locationAction.execute(siteName = jobSite)
            self.locationDict[jobSite] = siteInfo[0].get('ce_name', None)
        return self.locationDict[jobSite]





    def getClassAds(self):
        """
        _getClassAds_

        Grab classAds from condor_q using xml parsing
        """

        constraint = "\"WMAgent_JobID =!= UNDEFINED\""


        jobInfo = {}

        command = ['condor_q', '-constraint', 'WMAgent_JobID =!= UNDEFINED',
                   '-constraint', 'WMAgent_AgentName == \"%s\"' % (self.agent),
                   '-format', '(JobStatus:\%s)  ', 'JobStatus',
                   '-format', '(stateTime:\%s)  ', 'EnteredCurrentStatus',
                   '-format', '(WMAgentID:\%d):::',  'WMAgent_JobID']

        pipe = subprocess.Popen(command, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = False)
        stdout, stderr = pipe.communicate()
        classAdsRaw = stdout.split(':::')

        if not pipe.returncode == 0:
            # Then things have gotten bad - condor_q is not responding
            logging.error("condor_q returned non-zero value %s" % str(pipe.returncode))
            logging.error("Skipping classAd processing this round")
            return None


        if classAdsRaw == '':
            # We have no jobs
            return jobInfo

        for ad in classAdsRaw:
            # There should be one for every job
            if not re.search("\(", ad):
                # There is no ad.
                # Don't know what happened here
                continue
            statements = ad.split('(')
            tmpDict = {}
            for statement in statements:
                # One for each value
                if not re.search(':', statement):
                    # Then we have an empty statement
                    continue
                key = str(statement.split(':')[0])
                value = statement.split(':')[1].split(')')[0]
                tmpDict[key] = value
            if not 'WMAgentID' in tmpDict.keys():
                # Then we have an invalid job somehow
                logging.error("Invalid job discovered in condor_q")
                logging.error(tmpDict)
                continue
            else:
                jobInfo[int(tmpDict['WMAgentID'])] = tmpDict

        logging.info("Retrieved %i classAds" % len(jobInfo))


        return jobInfo

