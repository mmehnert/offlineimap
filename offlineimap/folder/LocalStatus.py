# Local status cache virtual folder
# Copyright (C) 2002 - 2008 John Goerzen
# <jgoerzen@complete.org>
#
#    This program is free software; you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation; either version 2 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program; if not, write to the Free Software
#    Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA

from Base import BaseFolder
import os, threading

from pysqlite2 import dbapi2 as sqlite

magicline = "OFFLINEIMAP LocalStatus CACHE DATA - DO NOT MODIFY - FORMAT 1"
newmagicline = "OFFLINEIMAP LocalStatus NOW IN SQLITE, DO NOT MODIFY"

class LocalStatusFolder(BaseFolder):
    def __deinit__(self):
        self.save()
        self.cursor.close()
        self.connection.close()

    def __init__(self, root, name, repository, accountname, config):
        self.name = name
        self.root = root
        self.sep = '.'
        self.config = config
        self.dofsync = config.getdefaultboolean("general", "fsync", True)
        self.filename = os.path.join(root, name)
        self.filename = repository.getfolderfilename(name)
        self.messagelist = {}
        self.repository = repository
        self.savelock = threading.Lock()
        self.doautosave = 1
        self.accountname = accountname
        BaseFolder.__init__(self)
	self.dbfilename = self.filename + '.sqlite'

	# MIGRATE
	if os.path.exists(self.filename):
		self.connection = sqlite.connect(self.dbfilename)
		self.cursor = self.connection.cursor()
		self.cursor.execute('CREATE TABLE status (id INTEGER PRIMARY KEY, flags VARCHAR(50))')
		if self.isnewfolder():
                    self.messagelist = {}
	            return
	        file = open(self.filename, "rt")
	        self.messagelist = {}
	        line = file.readline().strip()
	        assert(line == magicline)
	        for line in file.xreadlines():
	            line = line.strip()
	            uid, flags = line.split(':')
	            uid = long(uid)
	            flags = [x for x in flags]
		    flags.sort()
	            flags = ''.join(flags)
	            self.cursor.execute('INSERT INTO status (id,flags) VALUES (?,?)',
				(uid,flags))
	        file.close()
		self.connection.commit()
		os.rename(self.filename, self.filename + ".old")
		self.cursor.close()
		self.connection.close()

	# create new
	if not os.path.exists(self.dbfilename):
		self.connection = sqlite.connect(self.dbfilename)
		self.cursor = self.connection.cursor()
		self.cursor.execute('CREATE TABLE status (id INTEGER PRIMARY KEY, flags VARCHAR(50))')
	else:
		self.connection = sqlite.connect(self.dbfilename)
		self.cursor = self.connection.cursor()



    def getaccountname(self):
        return self.accountname

    def storesmessages(self):
        return 0

    def isnewfolder(self):
        return not os.path.exists(self.dbfilename)

    def getname(self):
        return self.name

    def getroot(self):
        return self.root

    def getsep(self):
        return self.sep

    def getfullname(self):
        return self.filename

    def deletemessagelist(self):
        if not self.isnewfolder():
            self.cursor.close()
            self.connection.close()
            os.unlink(self.dbfilename)

    def cachemessagelist(self):
        return

    def autosave(self):
        if self.doautosave:
            self.save()

    def save(self):
        self.connection.commit()

    def getmessagelist(self):
        if self.isnewfolder():
            self.messagelist = {}
            return

        self.messagelist = {}
        self.cursor.execute('SELECT id,flags from status')
        for row in self.cursor:
            flags = [x for x in row[1]]
            self.messagelist[row[0]] = {'uid': row[0], 'flags': flags}

        return self.messagelist

    def uidexists(self,uid):
        self.cursor.execute('SELECT id FROM status WHERE id=:id',{'id': uid})
        for row in self.cursor:
            if(row[0]==uid):
                return 1
        return 0

    def getmessageuidlist(self):
        self.cursor.execute('SELECT id from status')
        r = []
        for row in self.cursor:
            r.append(row[0])
        return r

    def getmessagecount(self):
        self.cursor.execute('SELECT count(id) from status');
        row = self.cursor.fetchone()
        return row[0]

    def savemessage(self, uid, content, flags, rtime):
        if uid < 0:
            # We cannot assign a uid.
            return uid

        if self.uidexists(uid):     # already have it
            self.savemessageflags(uid, flags)
            return uid

        self.messagelist[uid] = {'uid': uid, 'flags': flags, 'time': rtime}
        flags.sort()
        flags = ''.join(flags)
        self.cursor.execute('INSERT INTO status (id,flags) VALUES (?,?)',
                            (uid,flags))
        self.autosave()
        return uid

    def getmessageflags(self, uid):
        self.cursor.execute('SELECT flags FROM status WHERE id=:id',
                            {'id': uid})
        for row in self.cursor:
            flags = [x for x in row[0]]
            return flags
        return flags

    def getmessagetime(self, uid):
        return self.messagelist[uid]['time']

    def savemessageflags(self, uid, flags):
        self.messagelist[uid] = {'uid': uid, 'flags': flags}
        flags.sort()
        flags = ''.join(flags)
        self.cursor.execute('UPDATE status SET flags=? WHERE id=?',(flags,uid))
        self.autosave()

    def deletemessage(self, uid):
        self.deletemessages([uid])

    def deletemessages(self, uidlist):
        # Weed out ones not in self.messagelist
        uidlist = [uid for uid in uidlist if uid in self.messagelist]
        if not len(uidlist):
            return

        for uid in uidlist:
            del(self.messagelist[uid])
            #if self.uidexists(uid):
            self.cursor.execute('DELETE FROM status WHERE id=:id', {'id': uid})
