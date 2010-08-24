#!/usr/bin/env python
"""
_LoadFromName_

SQLite implementation of LoadFileset
"""

__all__ = []
__revision__ = "$Id: LoadFromName.py,v 1.3 2009/01/13 16:38:53 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Fileset.LoadFromName import LoadFromName as LoadFilesetMySQL

class LoadFromName(LoadFilesetMySQL):
    sql = LoadFilesetMySQL.sql
