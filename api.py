#!/usr/bin/env python
"""API for app that queries backend
"""

import logging
import os
import json
from urlparse import parse_qs
import MySQLdb, MySQLdb.cursors

# Insecure as fuck but system makes it impossible to do anything
CNX = { 'user': 'abatisto_admin',
        'passwd': 'S7jse8irSP5NGvC2',
        'host': 'webdb.uvm.edu',
        'db': 'ABATISTO_MusicianNetwork',
        'cursorclass': MySQLdb.cursors.DictCursor }

PROFILE = { 'required': ['email', 'password', 'role', 'location'],
            'optional': ['genres', 'instruments', 'name', 'bio', 'phone'] }

INSTRUCTIONS = ('Parameters invalid.\n'
                'Please supply "?action=[your_action]" at the end of the url.\n\n'
                'Options:\n'
                '\tGET\n'
                '\t\taction=get_profile&email=[email_of_profile]\n'
                '\n\tPOST\n'
                '\t\taction=create_profile - JSON with required '
                'parameters %s and/or optional parameters %s' % (PROFILE['required'], PROFILE['optional']))

STATUS = { 'ok':     '200 OK',
           'bad':    '400 Bad Request',
           'exists': '409 Conflict',
           'error':  '500 Internal Server Error' }

class API(object):
    """Actual API"""

    def __init__(self):
        """Set some base things"""
        self.root = os.path.expanduser('~')
        handler = logging.FileHandler('%s/www-logs/api.log' % self.root)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger = logging.getLogger('api')
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(handler)

        self.env = None
        self.resp = None
        self.query = None
        self.args = None

        self.db_conn = MySQLdb.connect(**CNX)

    def __call__(self, environment, response):
        """Set the environment variables and response function to self, then handle request"""
        result = None
        status = '400 Bad Request'
        _type = 'text/plain'

        try:
            self.env = environment.copy()
            self.resp = response

            # Check the action and proceed if correct
            self.query = parse_qs(self.env['QUERY_STRING'])
            action = str(self.query.get('action')[0] if self.query.get('action') else None)

            # All actions will return None if they fail
            if action and hasattr(self, action):
                self.logger.info(action)
                result, status, _type = getattr(self, action)()

        except Exception:
            self.logger.exception("")
            status = STATUS['error']
            result = 'An error has occured'
        finally:
            self.db_conn.commit()
            self.start_resp(status, _type)

            result = INSTRUCTIONS if not result else result
            yield result

    def get_profile(self):
        """Do a get on a profile"""
        result = None
        status = STATUS['bad']
        _type = 'text/plain'

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
                _type = 'application/json'
                status = STATUS['ok']

        return result, status, _type

    def create_profile(self):
        """Create a new profile if all of the required parameters are in place and the email does not exist"""
        result = None
        status = STATUS['bad']
        _type = 'text/plain'

        # Verify required parameters are entered
        if self.check_method('post') and self.check_body(PROFILE['required']):
            cur = self.db_conn.cursor()

            # See if it already exists
            email = self.args['email']
            cur.execute('''
                        SELECT *
                        FROM profile
                        WHERE email=%s
                        ''', (email,))

            if not cur.rowcount:
                for arg in PROFILE['required'] + PROFILE['optional']:
                    if arg not in self.args:
                        self.args[arg] = None

                self.args['profile_picture'] = '%s' % email.replace("@", "_").replace(".", "_")
                os.mkdir('%s/www-root/api/pics/%s' % (self.root, self.args['profile_picture']))
                cur.execute('''
                            insert into profile
                            values (%(email)s, %(name)s,
                                    %(password)s, %(role)s,
                                    %(location)s, %(bio)s,
                                    %(phone)s, %(profile_picture)s)
                            ''', self.args)

                if cur.rowcount:
                    result = "Success"
                    status = STATUS['ok']

            else:
                self.logger.error('Profile exists: %s', self.args)
                result = 'Profile already exists'
                status = STATUS['exists']

            cur.close()

        return result, status, _type

    def check_query(self, param):
        """Determine if a parameter exists in a query, otherwise bad request"""
        return bool(self.query.get(param)[0]) if self.query.get(param) else None

    def check_body(self, params):
        """Determine if the params are within the data section of the request and set the arguments"""
        msg_size = int(self.env.get('CONTENT_LENGTH', 0))

        if msg_size:
            self.args = json.loads(self.env['wsgi.input'].read(msg_size), object_hook=self.ascii_encode_dict)
            success = all(param in self.args for param in params)
        else:
            self.args = None
            success = False

        return success

    @staticmethod
    def ascii_encode_dict(data):
        """Encode json in ascii"""
        ascii_encode = lambda x: x.encode('ascii')
        return dict(map(ascii_encode, pair) for pair in data.items())

    def check_method(self, method):
        """Input the method expected to validate it, otherwise bad request"""
        return self.env['REQUEST_METHOD'] == method.upper()

    def start_resp(self, status, _type):
        """Easier way to start response"""
        self.resp(status, [('Content-Type', _type)])

request_handler = API()
