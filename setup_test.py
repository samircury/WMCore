from distutils.core import Command
from unittest import TextTestRunner, TestLoader, TestSuite
from setup_build import get_relative_path, generate_filelist

from glob import glob
from os.path import splitext, basename, join as pjoin, walk
from ConfigParser import ConfigParser, NoOptionError
import os, sys, os.path
import unittest
import time

# pylint and coverage aren't standard, but aren't strictly necessary
# you should get them though
can_lint = False
can_coverage = False
can_nose = False

try:
    from pylint.lint import Run
    from pylint.lint import preprocess_options, cb_init_hook
    from pylint import checkers
    can_lint = True
except:
    pass
try:
    import coverage
    can_coverage = True
except:
    pass

try:
    import nose
    from nose.plugins import Plugin, PluginTester
    from nose.plugins.attrib import AttributeSelector
    import nose.failure
    import nose.case
    can_nose = True
except:
    pass

if can_nose:
    def get_subpackages(dir, prefix = ""):
        if prefix.startswith('.'):
            prefix = prefix[1:]
        result=[]
        for subdir in os.listdir(dir):
            if os.path.isdir(os.path.join(dir, subdir)):
                result.extend( get_subpackages(os.path.join(dir,subdir), prefix + "." + subdir) )
            else:
                # should be a file
                if subdir.endswith('.py'):
                    if subdir.startswith('__init__.py'):
                        continue
                    result.append( prefix + "."+ subdir[:-3])
        return result

    class DetailedOutputter(Plugin):
        name = "detailed"
        def __init__(self):
            pass

        def setOutputStream(self, stream):
            """Get handle on output stream so the plugin can print id #s
            """
            self.stream = stream

        def beforeTest(self, test):
            if ( test.id().startswith('nose.failure') ):
                pass
                #Plugin.beforeTest(self, test)
            else:

                self.stream.write('%s -- ' % (test.id(), ))

        def configure(self, options, conf):
            """
                Configure plugin. Skip plugin is enabled by default.
            """
            self.enabled = True

    class TestCommand(Command):
        """Runs our test suite"""
        # WARNING WARNING WARNING
        # if you set this to true, I will really delete your database
        #  after every test
        # WARNING WARNING WARNING
        user_options = [('reallyDeleteMyDatabaseAfterEveryTest=',
                         None,
                         'If you set this I WILL DELETE YOUR DATABASE AFTER EVERY TEST. DO NOT RUN ON A PRODUCTION SYSTEM'),
                         ('buildBotMode=',
                          None,
                          'Are we running inside buildbot?'),
                         ('workerNodeTestsOnly=',
                          None,
                          "Are we testing WN functionality? (Only runs WN-specific tests)"),
                         ('testCertainPath=',
                          None,
                          "Only runs tests below a certain path (i.e. test/python searches the whole test tree")]

        def initialize_options(self):
            self.reallyDeleteMyDatabaseAfterEveryTest = False
            self.buildBotMode = False
            self.workerNodeTestsOnly = False
            self.testCertainPath = False
            pass

        def finalize_options(self):
            pass

        def run(self):
            testPath = 'test/python'
            if self.testCertainPath:
                print "Using the tests below: %s" % self.testCertainPath
                testPath = self.testCertainPath
            else:
				print "Nose is scanning all tests"
            if self.reallyDeleteMyDatabaseAfterEveryTest:
                print "#### WE ARE DELETING YOUR DATABASE. 3 SECONDS TO CANCEL ####"
                print "#### buildbotmode is %s" % self.buildBotMode
                sys.stdout.flush()
                import WMQuality.TestInit
                WMQuality.TestInit.deleteDatabaseAfterEveryTest( "I'm Serious" )
                time.sleep(4)
            if self.workerNodeTestsOnly:
                retval =  nose.run(argv=[__file__,'--with-xunit', '-v',testPath,'-m', '(_t.py$)|(_t$)|(^test)','-a','workerNodeTest'],
                                    addplugins=[DetailedOutputter()])
            elif not self.buildBotMode:
                retval =  nose.run(argv=[__file__,'--with-xunit', '-v',testPath, '-m', '(_t.py$)|(_t$)|(^test)', '-a', '!workerNodeTest'])
            else:
                print "### We are in buildbot mode ###"
                srcRoot = os.path.join(os.path.normpath(os.path.dirname(__file__)), 'src', 'python')
                modulesToCover = []
                modulesToCover.extend(get_subpackages(os.path.join(srcRoot,'WMCore'), 'WMCore'))
                modulesToCover.extend(get_subpackages(os.path.join(srcRoot,'WMComponent'), 'WMComponent'))
                modulesToCover.extend(get_subpackages(os.path.join(srcRoot,'WMQuality'), 'WMQuality'))
                moduleList = ",".join(modulesToCover)
                sys.stdout.flush()
                retval =  nose.run(argv=[__file__,'--with-xunit', '-v',testPath,'-m', '(_t.py$)|(_t$)|(^test)','-a',
                                         '!workerNodeTest,!integration,!performance,!__integration__,!__performance__',
                                         '--with-coverage','--cover-html','--cover-html-dir=coverageHtml','--cover-erase',
                                         '--cover-package=' + moduleList, '--cover-inclusive'],
                                    addplugins=[DetailedOutputter()])

            if retval:
                sys.exit( 0 )
            else:
                sys.exit( 1 )
else:
    class TestCommand(Command):
        user_options = [ ]
        def run(self):
            print "Nose isn't installed. You must install the nose package to run tests (easy_install nose might do it)"
            sys.exit(1)
            pass

        def initialize_options(self):
            pass

        def finalize_options(self):
            pass
        pass

if can_lint:
    class LinterRun(Run):
        def __init__(self, args, reporter=None):
            self._rcfile = None
            self._plugins = []
            preprocess_options(args, {
                # option: (callback, takearg)
                'rcfile':       (self.cb_set_rcfile, True),
                'load-plugins': (self.cb_add_plugins, True),
                })
            self.linter = linter = self.LinterClass((
                ('rcfile',
                 {'action' : 'callback', 'callback' : lambda *args: 1,
                  'type': 'string', 'metavar': '<file>',
                  'help' : 'Specify a configuration file.'}),

                ('init-hook',
                 {'action' : 'callback', 'type' : 'string', 'metavar': '<code>',
                  'callback' : cb_init_hook,
                  'help' : 'Python code to execute, usually for sys.path \
    manipulation such as pygtk.require().'}),

                ('help-msg',
                 {'action' : 'callback', 'type' : 'string', 'metavar': '<msg-id>',
                  'callback' : self.cb_help_message,
                  'group': 'Commands',
                  'help' : '''Display a help message for the given message id and \
    exit. The value may be a comma separated list of message ids.'''}),

                ('list-msgs',
                 {'action' : 'callback', 'metavar': '<msg-id>',
                  'callback' : self.cb_list_messages,
                  'group': 'Commands',
                  'help' : "Generate pylint's full documentation."}),

                ('generate-rcfile',
                 {'action' : 'callback', 'callback' : self.cb_generate_config,
                  'group': 'Commands',
                  'help' : '''Generate a sample configuration file according to \
    the current configuration. You can put other options before this one to get \
    them in the generated configuration.'''}),

                ('generate-man',
                 {'action' : 'callback', 'callback' : self.cb_generate_manpage,
                  'group': 'Commands',
                  'help' : "Generate pylint's man page.",'hide': 'True'}),

                ('errors-only',
                 {'action' : 'callback', 'callback' : self.cb_error_mode,
                  'short': 'e',
                  'help' : '''In error mode, checkers without error messages are \
    disabled and for others, only the ERROR messages are displayed, and no reports \
    are done by default'''}),

                ('profile',
                 {'type' : 'yn', 'metavar' : '<y_or_n>',
                  'default': False,
                  'help' : 'Profiled execution.'}),

                ), option_groups=self.option_groups,
                   reporter=reporter, pylintrc=self._rcfile)
            # register standard checkers
            checkers.initialize(linter)
            # load command line plugins
            linter.load_plugin_modules(self._plugins)
            # read configuration
            linter.disable_message('W0704')
            linter.read_config_file()
            # is there some additional plugins in the file configuration, in
            config_parser = linter._config_parser
            if config_parser.has_option('MASTER', 'load-plugins'):
                plugins = splitstrip(config_parser.get('MASTER', 'load-plugins'))
                linter.load_plugin_modules(plugins)
            # now we can load file config and command line, plugins (which can
            # provide options) have been registered
            linter.load_config_file()
            if reporter:
                # if a custom reporter is provided as argument, it may be overriden
                # by file parameters, so re-set it here, but before command line
                # parsing so it's still overrideable by command line option
                linter.set_reporter(reporter)
            args = linter.load_command_line_configuration(args)
            # insert current working directory to the python path to have a correct
            # behaviour
            sys.path.insert(0, os.getcwd())
            if self.linter.config.profile:
                print >> sys.stderr, '** profiled run'
                from hotshot import Profile, stats
                prof = Profile('stones.prof')
                prof.runcall(linter.check, args)
                prof.close()
                data = stats.load('stones.prof')
                data.strip_dirs()
                data.sort_stats('time', 'calls')
                data.print_stats(30)
            sys.path.pop(0)

        def cb_set_rcfile(self, name, value):
            """callback for option preprocessing (ie before optik parsing)"""
            self._rcfile = value

        def cb_add_plugins(self, name, value):
            """callback for option preprocessing (ie before optik parsing)"""
            self._plugins.extend(splitstrip(value))

        def cb_error_mode(self, *args, **kwargs):
            """error mode:
            * checkers without error messages are disabled
            * for others, only the ERROR messages are displayed
            * disable reports
            * do not save execution information
            """
            self.linter.disable_noerror_checkers()
            self.linter.set_option('disable-msg-cat', 'WCRFI')
            self.linter.set_option('reports', False)
            self.linter.set_option('persistent', False)

        def cb_generate_config(self, *args, **kwargs):
            """optik callback for sample config file generation"""
            self.linter.generate_config(skipsections=('COMMANDS',))

        def cb_generate_manpage(self, *args, **kwargs):
            """optik callback for sample config file generation"""
            from pylint import __pkginfo__
            self.linter.generate_manpage(__pkginfo__)

        def cb_help_message(self, option, opt_name, value, parser):
            """optik callback for printing some help about a particular message"""
            self.linter.help_message(splitstrip(value))

        def cb_list_messages(self, option, opt_name, value, parser):
            """optik callback for printing available messages"""
            self.linter.list_messages()
else:
    class LinterRun:
        def __init__(self):
            pass
def lint_score(stats, evaluation):
    return eval(evaluation, {}, stats)

def lint_files(files, reports=False):
    """
    lint a (list of) file(s) and return the results as a dictionary containing
    filename : result_dict
    """

    rcfile=os.path.join(get_relative_path(),'standards/.pylintrc')

    arguements = ['--rcfile=%s' % rcfile, '--ignore=DefaultConfig.py']

    if not reports:
        arguements.append('-rn')

    arguements.extend(files)

    lntr = LinterRun(arguements)

    results = {}
    for file in files:
        lntr.linter.check(file)
        results[file] = {'stats': lntr.linter.stats,
                         'score': lint_score(lntr.linter.stats,
                                             lntr.linter.config.evaluation)
                         }
        if reports:
            print '----------------------------------'
            print 'Your code has been rated at %.2f/10' % \
                    lint_score(lntr.linter.stats, lntr.linter.config.evaluation)

    return results, lntr.linter.config.evaluation

class LintCommand(Command):
   description = "Lint all files in the src tree"
   """
   TODO: better format the test results, get some global result, make output
   more buildbot friendly.
   """

   user_options = [ ('package=', 'p', 'package to lint, default to None'),
               ('report', 'r', 'return a detailed lint report, default False')]

   def initialize_options(self):
       self._dir = get_relative_path()
       self.package = None
       self.report = False

   def finalize_options(self):
       if self.report:
           self.report = True

   def run(self):
       '''
       Find the code and run lint on it
       '''
       if can_lint:
           srcpypath = os.path.join(self._dir, 'src/python/')

           sys.path.append(srcpypath)

           files_to_lint = []

           if self.package:
               if self.package.endswith('.py'):
                   cnt = self.package.count('.') - 1
                   files_to_lint = generate_filelist(self.package.replace('.', '/', cnt), 'DeafultConfig.py')
               else:
                   files_to_lint = generate_filelist(self.package.replace('.', '/'), 'DeafultConfig.py')
           else:
               files_to_lint = generate_filelist(ignore='DeafultConfig.py')

           results, evaluation = lint_files(files_to_lint, self.report)
           ln = len(results)
           scr = 0
           print
           for k, v in results.items():
               print "%s: %.2f/10" % (k.replace('src/python/', ''), v['score'])
               scr += v['score']
           if ln > 1:
               print '--------------------------------------------------------'
               print 'Average pylint score for %s is: %.2f/10' % (self.package,
                                                                 scr/ln)

       else:
           print 'You need to install pylint before using the lint command'

class ReportCommand(Command):
   description = "Generate a simple html report for ease of viewing in buildbot"
   """
   To contain:
       average lint score
       % code coverage
       list of classes missing tests
       etc.
   """

   user_options = [ ]

   def initialize_options(self):
       pass

   def finalize_options(self):
       pass

   def run(self):
       """
       run all the tests needed to generate the report and make an
       html table
       """
       files = generate_filelist()

       error = 0
       warning = 0
       refactor = 0
       convention = 0
       statement = 0

       srcpypath = '/'.join([get_relative_path(), 'src/python/'])
       sys.path.append(srcpypath)

       cfg = ConfigParser()
       cfg.read('standards/.pylintrc')

       # Supress stdout/stderr
       sys.stderr = open('/dev/null', 'w')
       sys.stdout = open('/dev/null', 'w')
       # wrap it in an exception handler, otherwise we can't see why it fails
       try:
           # lint the code
           for stats in lint_files(files):
               error += stats['error']
               warning += stats['warning']
               refactor += stats['refactor']
               convention += stats['convention']
               statement += stats['statement']
       except Exception,e:
           # and restore the stdout/stderr
           sys.stderr = sys.__stderr__
           sys.stdout = sys.__stderr__
           raise e

       # and restore the stdout/stderr
       sys.stderr = sys.__stderr__
       sys.stdout = sys.__stderr__

       stats = {'error': error,
           'warning': warning,
           'refactor': refactor,
           'convention': convention,
           'statement': statement}

       lint_score = eval(cfg.get('MASTER', 'evaluation'), {}, stats)
       coverage = 0 # TODO: calculate this
       testless_classes = [] # TODO: generate this

       print "<table>"
       print "<tr>"
       print "<td colspan=2><h1>WMCore test report</h1></td>"
       print "</tr>"
       print "<tr>"
       print "<td>Average lint score</td>"
       print "<td>%.2f</td>" % lint_score
       print "</tr>"
       print "<tr>"
       print "<td>% code coverage</td>"
       print "<td>%s</td>" % coverage
       print "</tr>"
       print "<tr>"
       print "<td>Classes missing tests</td>"
       print "<td>"
       if len(testless_classes) == 0:
           print "None"
       else:
           print "<ul>"
           for c in testless_classes:
               print "<li>%c</li>" % c
           print "</ul>"
       print "</td>"
       print "</tr>"
       print "</table>"

class CoverageCommand(Command):
   description = "Run code coverage tests"
   """
   To do this, we need to run all the unittests within the coverage
   framework to record all the lines(and branches) executed
   unfortunately, we have multiple code paths per database schema, so
   we need to find a way to merge them.

   TODO: modify the test command to have a flag to record code coverage
         the file thats used can then be used here, saving us from running
         our tests twice
   """

   user_options = [ ]

   def initialize_options(self):
       pass

   def finalize_options(self):
       pass

   def run(self):
       """
       Determine the code's test coverage and return that as a float

       http://nedbatchelder.com/code/coverage/
       """
       if can_coverage:
           files = generate_filelist()
           dataFile = None
           cov = None

           # attempt to load previously cached coverage information if it exists
           try:
               dataFile = open("wmcore-coverage.dat","r")
               cov = coverage.coverage(branch = True, data_file='wmcore-coverage.dat')
               cov.load()
           except:
               cov = coverage.coverage(branch = True, )
               cov.start()
               runUnitTests()
               cov.stop()
               cov.save()

           # we have our coverage information, now let's do something with it
           # get a list of modules
           cov.report(morfs = files, file=sys.stdout)
           return 0
       else:
           print 'You need the coverage module installed before running the' +\
                           ' coverage command'