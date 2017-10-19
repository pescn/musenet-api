#!/usr/bin/env python
"""API for app that queries backend
"""

import logging
import json
from urlparse import parse_qs
import MySQLdb, MySQLdb.cursors

# Insecure as fuck but w/e
CNX = { 'user': 'abatisto_admin',
        'passwd': 'S7jse8irSP5NGvC2',
        'host': 'webdb.uvm.edu',
        'db': 'ABATISTO_MusicianNetwork',
        'cursorclass': MySQLdb.cursors.DictCursor }

class API(object):
    """Actual API"""

    def __init__(self):
        """Set some base things"""
        self.logger = logging.getLogger('api')
        self.env = None
        self.resp = None
        self.query = None
        self.args = None

        self.db_conn = MySQLdb.connect(**CNX)

    def __call__(self, environment, response):
        """Set the environment variables and response function to self, then handle request"""
        self.logger.info('Accessed')
        self.env = environment.copy()
        self.resp = response

        # Check the action and proceed if correct
        self.query = parse_qs(self.env['QUERY_STRING'])
        action = str(self.query.get('action')[0])

        # All actions will return None if they fail
        if action and hasattr(self, action):
            result = getattr(self, action)()

            if result:
                self.start_resp()
                yield result
            else:
                self.bad_request()
        else:
            self.bad_request()

    def get_profile(self):
        """Do a get on a profile"""
        result = None

        if self.check_method('get') and self.check_query('email'):
            # Parse_qs implements as a list...
            email = self.query['email'][0]

            cur = self.db_conn.cursor()
            cur.execute('''
                        select *
                        from profile
                        where email = %s
                        ''', (email,))

            if cur.rowcount:
                result = json.dumps(cur.fetchone())

        return result

    def create_profile(self):
        """Create a new profile if all of the required parameters are in place and the email does not exist"""
        result = None

        # Lists to verify correct parameters are entered
        required = ['email', 'password', 'role', 'location']
        optional = ['genres', 'instruments', 'name']

        if self.check_method('get') and self.check_data(required):
            cur = self.db_conn.cursor()

            # See if it already exists
            email = self.args['email']
            cur.execute('''
                        SELECT *
                        FROM profile
                        WHERE email=%s
                        ''', (email,))

            if not cur.rowcount:
                cur.close()

        return result

    def check_query(self, param):
        """Determine if a parameter exists in a query, otherwise bad request"""
        return bool(self.query.get(param)[0])

    def check_data(self, params):
        """Determine if the params are within the data section of the request and set the arguments"""
        msg_size = int(self.env.get('CONTENT_LENGTH', 0))

        success = False
        if msg_size:
            self.args = json.loads(self.env['wsgi.input'].read(msg_size))
            success = all(param for param in params in self.args)
        else:
            self.args = None

        return success

    def check_method(self, method):
        """Input the method expected to validate it, otherwise bad request"""
        return self.env['REQUEST_METHOD'] == method.upper()

    def start_resp(self):
        """Easier way to start response"""
        self.resp('200 OK', [('Content-Type', 'application/json')])

    def bad_request(self):
        """Easier way to call bad request"""
        self.resp('400 BAD REQUEST', [('Content-Type', 'text/plain')])
        yield "400 BAD REQUEST"

request_handler = API()
