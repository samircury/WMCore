from cherrypy import expose, tools
from WMCore.WebTools.Page import TemplatedPage
from os import listdir

class SecureDocumentation(TemplatedPage):
    """
    The documentation for the framework
    """
    @expose
    @tools.cernoid()
    def index(self):
        templates = listdir(self.templatedir)
        index = "<h1>Documentation</h1>\n<ol>"
        for t in templates:
            if '.tmpl' in t:
                index = "%s\n<li><a href='%s'>%s</a></li>" % (index, 
                                                      t.replace('.tmpl', ''), 
                                                      t.replace('.tmpl', ''))
        index = "%s\n<li><a href='https://twiki.cern.ch/twiki/bin/view/CMS/DMWebtools'>twiki</a>" % (index)
        index = "%s\n<ol>" % (index)
        return index
    
    @expose
    def default(self, *args, **kwargs):
        if len(args) > 0:
            return self.templatepage(args[0])
        else:
            return self.index()