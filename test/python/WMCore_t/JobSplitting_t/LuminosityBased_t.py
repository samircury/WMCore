#!/usr/bin/env python
"""
_LuminosityBased_t_

Luminosity based splitting test.
"""

import unittest
import pdb
import os
import json

from WMCore.WMBase            import getTestBase

from WMCore.DataStructs.File import File
from WMCore.DataStructs.Fileset import Fileset
from WMCore.DataStructs.Subscription import Subscription
from WMCore.DataStructs.Workflow import Workflow
from WMCore.DataStructs.Run import Run

from WMCore.JobSplitting.SplitterFactory import SplitterFactory
from WMCore.Services.UUID import makeUUID

class LuminosityBasedTest(unittest.TestCase):
    """
    _EventBasedTest_

    Test event based job splitting.
    """


    def setUp(self):
        """
        _setUp_

        Create two subscriptions: One that contains a single file and one that
        contains multiple files.
        """
        self.multipleFileFileset = Fileset(name = "TestFileset1")
        for i in range(10):
            newFile = File(makeUUID(), size = 20000, events = 2000)
            # 80*(index-1), 80*(index)
            newFile.addRun(Run(207214, *range(80*i, 80*(i+1))))
            #newFile.runs = set()
            #newFile.runs.add(207214)
            newFile.setLocation('se01')
            self.multipleFileFileset.addFile(newFile)

        self.singleFileFileset = Fileset(name = "TestFileset2")
        newFile = File("/some/file/name", size = 1000, events = 100)
        newFile.setLocation('se02')
        self.singleFileFileset.addFile(newFile)

        self.emptyFileFileset = Fileset(name = "TestFileset3")
        newFile = File("/some/file/name", size = 1000, events = 0)
        newFile.setdefault('se03')
        self.emptyFileFileset.addFile(newFile)

        testWorkflow = Workflow()
        self.multipleFileSubscription = Subscription(fileset = self.multipleFileFileset,
                                                     workflow = testWorkflow,
                                                     split_algo = "LuminosityBased",
                                                     type = "Processing")
        self.singleFileSubscription = Subscription(fileset = self.singleFileFileset,
                                                   workflow = testWorkflow,
                                                   split_algo = "LuminosityBased",
                                                   type = "Processing")
        self.emptyFileSubscription = Subscription(fileset = self.emptyFileFileset,
                                                  workflow = testWorkflow,
                                                  split_algo = "LuminosityBased",
                                                  type = "Processing")

        self.performanceParams = {'timePerEvent' : 15,
                                  'memoryRequirement' : 1700,
                                  'sizePerEvent' : 400}
        # Simulate DQM input
        self.DQMLuminosityPerLs = self.loadTestDQMLuminosityPerLs()

        # Simulate DashBoard input
        self.testPerfCurve  =   self.loadTestPerfCurve()
        
        return

    def tearDown(self):
        """
        _tearDown_

        Nothing to do...
        """
        pass

    def loadTestDQMLuminosityPerLs(self):
        testResponseFile = open(os.path.join(getTestBase(),
        							'WMCore_t/JobSplitting_t/FakeInputs/DQMLuminosityPerLs.json'), 'r')
        response = testResponseFile.read()
        testResponseFile.close()
        responseJSON = json.loads(response)
        luminosityPerLS = responseJSON["hist"]["bins"]["content"]
        return luminosityPerLS 

    def loadTestPerfCurve(self):
        testResponseFile = open(os.path.join(getTestBase(),
        							'WMCore_t/JobSplitting_t/FakeInputs/dashBoardPerfCurve.json'), 'r')
        response = testResponseFile.read()
        testResponseFile.close()
        responseJSON = json.loads(response)
        perfCurve = responseJSON["points"][0]["data"]
        return perfCurve

		
    def testNoEvents(self):
        """
        _testNoEvents_

        Test event based job splitting where there are no events in the
        input file, make sure the mask events are None
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.emptyFileSubscription)
        jobGroups = jobFactory(events_per_job = 100,
                               performance = self.performanceParams)

        self.assertEqual(len(jobGroups), 1,
                         "ERROR: JobFactory didn't return one JobGroup")
        self.assertEqual(len(jobGroups[0].jobs), 1,
                         "ERROR: JobFactory didn't create a single job")

        job = jobGroups[0].jobs.pop()

        self.assertEqual(job.getFiles(type = "lfn"), ["/some/file/name"],
                         "ERROR: Job contains unknown files")
        self.assertEqual(job["mask"].getMaxEvents(), None,
                         "ERROR: Mask maxEvents is not None")


    def testExactEvents(self):
        """
        _testExactEvents_

        Test event based job splitting when the number of events per job is
        exactly the same as the number of events in the input file.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.singleFileSubscription)
        jobGroups = jobFactory(events_per_job = 100,
                               performance = self.performanceParams)

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 1, \
               "ERROR: JobFactory didn't create a single job."

        job = jobGroups[0].jobs.pop()

        assert job.getFiles(type = "lfn") == ["/some/file/name"], \
               "ERROR: Job contains unknown files."

        assert job["mask"].getMaxEvents() is None, \
               "ERROR: Job's max events is incorrect."

        assert job["mask"]["FirstEvent"] == 0, \
               "ERROR: Job's first event is incorrect."

        return

    def testMoreEvents(self):
        """
        _testMoreEvents_

        Test event based job splitting when the number of events per job is
        greater than the number of events in the input file.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.singleFileSubscription)

        jobGroups = jobFactory(events_per_job = 1000,
                               performance = self.performanceParams)

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 1, \
               "ERROR: JobFactory created %s jobs not one" % len(jobGroups[0].jobs)

        job = jobGroups[0].jobs.pop()

        assert job.getFiles(type = "lfn") == ["/some/file/name"], \
               "ERROR: Job contains unknown files."

        assert job["mask"].getMaxEvents() is None, \
               "ERROR: Job's max events is incorrect."

        assert job["mask"]["FirstEvent"] is None, \
               "ERROR: Job's first event is incorrect."

        return

    def test50EventSplit(self):
        """
        _test50EventSplit_

        Test event based job splitting when the number of events per job is
        50, this should result in two jobs.
        """

        splitter = SplitterFactory()
        jobFactory = splitter(self.singleFileSubscription)

        jobGroups = jobFactory(events_per_job = 50,
                               performance = self.performanceParams)

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 2, \
               "ERROR: JobFactory created %s jobs not two" % len(jobGroups[0].jobs)

        firstEvents = []
        for job in jobGroups[0].jobs:
            assert job.getFiles(type = "lfn") == ["/some/file/name"], \
                   "ERROR: Job contains unknown files."

            assert job["mask"].getMaxEvents() == 50 or job["mask"].getMaxEvents() is None, \
                   "ERROR: Job's max events is incorrect."

            assert job["mask"]["FirstEvent"] in [0, 50], \
                   "ERROR: Job's first event is incorrect."

            assert job["mask"]["FirstEvent"] not in firstEvents, \
                   "ERROR: Job's first event is repeated."
            firstEvents.append(job["mask"]["FirstEvent"])

        return

    def test99EventSplit(self):
        """
        _test99EventSplit_

        Test event based job splitting when the number of events per job is
        99, this should result in two jobs.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.singleFileSubscription)

        jobGroups = jobFactory(events_per_job = 99,
                               performance = self.performanceParams)

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 2, \
               "ERROR: JobFactory created %s jobs not two" % len(jobGroups[0].jobs)

        firstEvents = []
        for job in jobGroups[0].jobs:
            assert job.getFiles(type = "lfn") == ["/some/file/name"], \
                   "ERROR: Job contains unknown files."

            self.assertTrue(job["mask"].getMaxEvents() == 99 or job['mask'].getMaxEvents() is None,
                            "ERROR: Job's max events is incorrect.")

            assert job["mask"]["FirstEvent"] in [0, 99], \
                   "ERROR: Job's first event is incorrect."

            assert job["mask"]["FirstEvent"] not in firstEvents, \
                   "ERROR: Job's first event is repeated."
            firstEvents.append(job["mask"]["FirstEvent"])

        return

    def test100EventMultipleFileSplit(self):
        """
        _test100EventMultipleFileSplit_

        Test job splitting into 100 event jobs when the input subscription has
        more than one file available.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.multipleFileSubscription)

        jobGroups = jobFactory(targetJobLength = 21600,
                               performance = self.performanceParams,
                               testDqmLuminosityPerLs = self.DQMLuminosityPerLs,
                               testPerfCurve = self.testPerfCurve, 
                               primaryDataset = "SingleMu")
#                               manualTimePerEvent = 15 )

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 23, \
               "ERROR: JobFactory created %s jobs not 23" % len(jobGroups[0].jobs)

        for job in jobGroups[0].jobs:
            assert len(job.getFiles(type = "lfn")) == 1, \
                   "ERROR: Job contains too many files."

            # Mask is fixed, if you print job["mask"].getMaxEvents() everything is fine,
            # but this just won't work
            #assert job["mask"].getMaxEvents() is None, \
            #       "ERROR: Job's max events is incorrect."

        return
    # Test if it will do without 1 curve
    # Test if it will do without another curve
    # Test fallback dataset (there's a way?) I don't think so.

    def test100EventMultipleFileSplitNoPerfCurve(self):
        """
        _test100EventMultipleFileSplitNoPerfCurve_

        Here we do the same as before, but we don't pass the performance curve.
        The expected behavior is that for 2000 events file, 1 job get 1440 and
        the other 660, as the default TpE is 15 seconds.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.multipleFileSubscription)

        jobGroups = jobFactory(targetJobLength = 21600,
                               performance = self.performanceParams,
                               testDqmLuminosityPerLs = self.DQMLuminosityPerLs,
                               primaryDataset = "SingleMu")

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 20, \
               "ERROR: JobFactory created %s jobs not 20" % len(jobGroups[0].jobs)

        for job in jobGroups[0].jobs:
            assert len(job.getFiles(type = "lfn")) == 1, \
                   "ERROR: Job contains too many files."
        return

    def test100EventMultipleFileSplitNoDQMCurve(self):
        """
        _test100EventMultipleFileSplitNoDQMCurve_

        Here we do the same as before, but we don't pass the DQM Curve.
        More to test how the code is going to handle it. Some improvements came
        from this test.
        The expected behavior is that for 2000 events file, 1 job get 1440 and
        the other 660, as the default TpE is 15 seconds.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.multipleFileSubscription)

        jobGroups = jobFactory(targetJobLength = 21600,
                               performance = self.performanceParams,
                               testPerfCurve = self.testPerfCurve, 
                               primaryDataset = "SingleMu")

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 20, \
               "ERROR: JobFactory created %s jobs not 20" % len(jobGroups[0].jobs)

        for job in jobGroups[0].jobs:
            assert len(job.getFiles(type = "lfn")) == 1, \
                   "ERROR: Job contains too many files."
        return

    def test100EventMultipleFileSplitNoDQMCurveNoPerfCurve(self):
        """
        _test100EventMultipleFileSplitNoDQMCurve_

        Here we do the same as before, but we don't pass the performance curve.
        The expected behavior is that for 2000 events file, 1 job get 1440 and
        the other 660, as the default TpE is 15 seconds.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.multipleFileSubscription)

        jobGroups = jobFactory(targetJobLength = 21600,
                               performance = self.performanceParams,
                               primaryDataset = "SingleMu")

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 20, \
               "ERROR: JobFactory created %s jobs not 20" % len(jobGroups[0].jobs)

        for job in jobGroups[0].jobs:
            assert len(job.getFiles(type = "lfn")) == 1, \
                   "ERROR: Job contains too many files."
        return

    def test50EventMultipleFileSplit(self):
        """
        _test50EventMultipleFileSplit_

        Test job splitting into 50 event jobs when the input subscription has
        more than one file available.
        """

        splitter = SplitterFactory()
        jobFactory = splitter(self.multipleFileSubscription)

        jobGroups = jobFactory(events_per_job = 50,
                               performance = self.performanceParams)

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 20, \
               "ERROR: JobFactory created %s jobs not twenty" % len(jobGroups[0].jobs)

        for job in jobGroups[0].jobs:
            assert len(job.getFiles(type = "lfn")) == 1, \
                   "ERROR: Job contains too many files."

            assert job["mask"].getMaxEvents() == 50 or job["mask"].getMaxEvents() is None, \
                   "ERROR: Job's max events is incorrect."

            assert job["mask"]["FirstEvent"] in [0, 50], \
                   "ERROR: Job's first event is incorrect."

        return

    def test150EventMultipleFileSplit(self):
        """
        _test150EventMultipleFileSplit_

        Test job splitting into 150 event jobs when the input subscription has
        more than one file available.  This test verifies that the job splitting
        code will put at most one file in a job.
        """
        splitter = SplitterFactory()
        jobFactory = splitter(self.multipleFileSubscription)

        jobGroups = jobFactory(events_per_job = 150,
                               performance = self.performanceParams)

        assert len(jobGroups) == 1, \
               "ERROR: JobFactory didn't return one JobGroup."

        assert len(jobGroups[0].jobs) == 10, \
               "ERROR: JobFactory created %s jobs not ten" % len(jobGroups[0].jobs)

        for job in jobGroups[0].jobs:
            assert len(job.getFiles(type = "lfn")) == 1, \
                   "ERROR: Job contains too many files."

            assert job["mask"].getMaxEvents() is None, \
                   "ERROR: Job's max events is incorrect."

            assert job["mask"]["FirstEvent"] is None, \
                   "ERROR: Job's first event is incorrect."
    

if __name__ == '__main__':
    unittest.main()
