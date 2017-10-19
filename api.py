#!/usr/bin/env python
"""API for app that queries backend
"""

# Insecure as fuck but w/e
CNX = { 'user': 'abatisto_admin',
        'passwd': 'S7jse8irSP5NGvC2',
        'host': 'webdb.uvm.edu',
        'db': 'ABATISTO_MusicianNetwork' }

import logging
import json
import MySQLdb

class API(object):
    """Actual API"""

    def __init__(self):
        """Set some base things"""
        self.logger = logging.getLogger('api')
        self.env = None
        self.resp = None

        self.db_conn = MySQLdb.connect(**CNX)

    def __call__(self, environment, response):
        """Set the environment variables and response function to self, then handle request"""
        self.logger.info('Accessed')
        self.env = environment
        self.resp = response

        self.resp('200 OK', [('Content-Type', 'text/plain')])

        cur = self.db_conn.cursor()
        cur.execute('''
                    select * from profile
                    ''')

        yield str(cur.fetchone())

request_handler = API()
