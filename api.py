#!/usr/bin/env python
"""API for app that queries backend
"""

import logging
import os
import json
from hashlib import md5
from urlparse import parse_qs
import MySQLdb, MySQLdb.cursors

# Insecure as fuck but system makes it impossible to do anything
CNX = { 'user': 'abatisto_admin',
        'passwd': 'S7jse8irSP5NGvC2',
        'host': 'webdb.uvm.edu',
        'db': 'ABATISTO_MusicianNetwork',
        'cursorclass': MySQLdb.cursors.DictCursor }

STATUS = { 'ok':     '200 OK',
           'bad':    '400 Bad Request',
           'exists': '409 Conflict',
           'error':  '500 Internal Server Error' }

ACTIONS = [ { 'name': 'get_profile',
              'method': 'get',
              'query_params': ['email'],
              'args': {
                  'required': [],
                  'optional': []
              },
              'returns': 'application/json',
            },

            { 'name': 'create_profile',
              'method': 'post',
              'query_params': [],
              'args': {
                  'required': ['email', 'password', 'role', 'location'],
                  'optional': ['genres', 'instruments', 'name', 'bio', 'phone']
              },
              'returns': 'text/plain',
            }
          ]

INSTRUCTIONS = ('Parameters invalid.\n'
                'Please supply "?action=[your_action]&parameters=[your_param]" at the end of the url.\n\n'
                'Options:\n')

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
        try:
            self.env = environment.copy()
            self.resp = response

            result = None
            status = 'bad'
            _type = 'text/plain'

            # Check the action and proceed if correct
            self.query = parse_qs(self.env['QUERY_STRING'])
            action = self.parse_action()

            if action:
                result, status = getattr(self, action['name'])()

        except Exception:
            self.logger.exception('')
            status = 'error'

        finally:
            self.db_conn.commit() if result else self.db_conn.rollback()

            if status == 'error':
                result = STATUS[status]

            elif status == 'bad':
                result = INSTRUCTIONS

                for action in ACTIONS:
                    result += '\n%s' % self.format_dict(action)

            elif result == 'ok':
                _type = action['returns']

            self.start_resp(STATUS[status], _type)
            yield result

    def format_dict(self, _dict):
        """Do stuff"""
        _str = ''
        for key, val in _dict.items():
            if isinstance(val, dict):
                _str += self.format_dict(val)
            else:
                _str += '\t%s: %s\n' % (key, val)
        return _str

    @staticmethod
    def ascii_encode_dict(data):
        """Encode json in ascii"""
        ascii_encode = lambda x: x.encode('ascii')
        return dict(map(ascii_encode, pair) for pair in data.items())

    def start_resp(self, status, _type):
        """Easier way to start response"""
        self.resp(status, [('Content-Type', _type)])

    def parse_action(self):
        """Check arguments and methods from ACTIONS dictionary"""
        name = str(self.query.get('action')[0] if self.query.get('action') else None)

        method = self.env['REQUEST_METHOD']
        url = self.env['PATH_INFO']

        self.logger.info("%s: %s - Action: %s", url, method, name)

        # First check if its actually a method
        if hasattr(self, name):

            # Now cycle through the ACTIONS dictionary and verify...
            for action in ACTIONS:

                # The name is correct and that the url query contains the correct parameters
                if action['name'] == name and all(bool(self.query.get(param)) for param in action['query_params']):

                    msg_size = int(self.env.get('CONTENT_LENGTH', 0))

                    # The request actually needs arguments
                    if msg_size and action['args']['required']:
                        self.args = json.loads(self.env['wsgi.input'].read(msg_size), object_hook=self.ascii_encode_dict)

                        # And the request contains the correct arguments
                        if all(arg for arg in self.args in action['args']['required'] + action['args']['optional']) and \
                            all(arg for arg in action['args']['required'] in self.args):

                            # Then set all optional args that are not in there to None and return the action
                            for arg in action['args']['optional']:
                                if arg not in self.args:
                                    self.args[arg] = None

                            return action

                    # The request doesn't need arguments
                    if not msg_size and not action['args']['required']:
                        return action

        # If the check fails
        return None

    def get_profile(self):
        """Do a get on a profile"""
        email = self.query['email'][0]
        self.logger.info("Params: %s", email)

        cur = self.db_conn.cursor()
        cur.execute('''
                    select *
                    from profile
                    where email = %s
                    ''', (email,))

        if cur.rowcount:
            result = json.dumps(cur.fetchone())
            status = 'ok'

        return result, status

    def create_profile(self):
        """Create a new profile if all of the required parameters are in place and the email does not exist"""
        cur = self.db_conn.cursor()

        # See if it already exists
        email = self.args['email']
        cur.execute('''
                    select *
                    from profile
                    where email=%s
                    ''', (email,))

        if not cur.rowcount:
            # Create profile picture dir
            self.args['profile_picture'] = '%s' % email.replace("@", "_").replace(".", "_")
            os.mkdir('%s/www-root/api/pics/%s' % (self.root, self.args['profile_picture']))

            # Insert new profile and get rowcount
            cur.execute('''
                        insert into profile
                        values (%(email)s, %(name)s,
                                %(password)s, %(role)s,
                                %(location)s, %(bio)s,
                                %(phone)s, %(profile_picture)s)
                        ''', self.args)

            success = bool(cur.rowcount)

            genres = self.args.get('genre')
            if genres:
                for genre in genres:
                    cur.execute('''
                                insert into profile_genre (email, genre)
                                values (%s, %s)
                                ''', (self.args['email'], genre))

                    success = success and bool(cur.rowcount)

            instrs = self.args.get('instruments')
            if instrs:
                for instr in instrs:
                    cur.execute('''
                                insert into profile_instrument (email, instrument)
                                values (%s, %s)
                                ''', (self.args['email'], instr))

                    success = success and bool(cur.rowcount)

            if success:
                result = STATUS['ok']
                status = 'ok'

        else:
            self.logger.error('Profile exists: %s', self.args)
            result = 'Profile already exists'
            status = 'exists'

        cur.close()

        return result, status

request_handler = API()
