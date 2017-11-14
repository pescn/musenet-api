#!/usr/bin/env python
"""API for app that queries backend
"""

import logging
import os
import json
from random import randint
from passlib.hash import pbkdf2_sha256
from urlparse import parse_qs
import MySQLdb, MySQLdb.cursors

SALT_RANGE = {'min': 1, 'max': 2**16 - 1}

# Insecure as fuck but system makes it impossible to do anything
CNX = { 'user': 'abatisto_admin',
        'passwd': 'S7jse8irSP5NGvC2',
        'host': 'webdb.uvm.edu',
        'db': 'ABATISTO_MusicianNetwork',
        'cursorclass': MySQLdb.cursors.DictCursor }

STATUS = { 'ok':     '200 OK',
           'bad':    '400 Bad Request',
           'not':    '404 Not Found',
           'exists': '409 Conflict',
           'error':  '500 Internal Server Error' }

ACTIONS = [ { 'name': 'get_profile',
              'method': 'get',
              'url_params': ['email'],
              'args': {
                  'required': [],
                  'optional': []
              },
              'returns': 'application/json',
            },

            { 'name': 'create_profile',
              'method': 'post',
              'url_params': [],
              'args': {
                  'required': ['email', 'password', 'role', 'location'],
                  'optional': ['genres', 'instruments', 'name', 'bio', 'phone']
              },
              'returns': 'text/plain',
            },

            { 'name': 'login',
              'method': 'post',
              'url_params': [],
              'args': {
                  'required': ['email', 'password'],
                  'optional': []
              },
              'returns': 'text/plain',
            }
          ]

INSTRUCTIONS = ('Parameters invalid.\n'
                'Please supply "?action=[your_action]&parameters=[your_param]" at the end of the url.\n\n'
                'Options:\n')

class API(object):
    """Default all requests as bad, posts will return 200 OK by default when returning None"""

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
            self.db_conn.commit() if status == 'ok' else self.db_conn.rollback()

            if status == 'bad':
                self.logger.error('Invalid params')
                self.logger.error('Args: %s', self.args)
                self.logger.error('Url query: %s', self.query)

                result = INSTRUCTIONS

                for action in ACTIONS:
                    result += '\n%s' % self.format_dict(action)

            elif status == 'ok':
                if not result:
                    result = STATUS[status]

                _type = action['returns']

            else:
                result = STATUS[status]
                self.logger.error(result)

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
    def _hash(msg, salt=None):
        """Quick hashing"""
        if not salt:
            salt = str(randint(SALT_RANGE['min'], SALT_RANGE['max'])).encode()
        _hash = pbkdf2_sha256.using(salt=salt).hash(msg)
        return _hash, salt

    def start_resp(self, status, _type):
        """Easier way to start response"""
        self.resp(status, [('Content-Type', _type)])

    def parse_action(self):
        """Check arguments and methods from ACTIONS dictionary"""
        result = None

        name = str(self.query.get('action')[0] if self.query.get('action') else None)

        method = self.env['REQUEST_METHOD']

        self.logger.info("%s - %s", method, name)

        # First check if its actually a method
        if hasattr(self, name):

            # Cycle through the ACTIONS dictionary
            for action in ACTIONS:

                # Name is correct and the action is correct
                if action['name'] == name and action['method'].lower() == method.lower():

                    # Method needs arguments and actually has arguments
                    msg_size = int(self.env.get('CONTENT_LENGTH', 0))

                    if action['args']['required'] and msg_size:
                        self.args = json.loads(self.env['wsgi.input'].read(msg_size))

                        # Request contains the correct arguments
                        if all(arg in action['args']['required'] + action['args']['optional'] for arg in self.args) and \
                            all(arg in self.args for arg in action['args']['required']):

                            # Then set all optional args that are not in request to None, action is correct
                            for arg in action['args']['optional']:
                                if arg not in self.args:
                                    self.args[arg] = None

                            self.logger.info(self.args)
                            result = action

                    # Method needs url params
                    elif all(arg in action['url_params'] for arg in [param for param in self.query if param != "action"]):
                        self.logger.info(self.query)
                        result = action

        # If the check fails
        return result

    def get_profile(self):
        """Do a get on a profile"""
        email = self.query['email'][0]

        cur = self.db_conn.cursor()
        cur.execute('''
                    select email, role, location, name, bio, phone
                    from profile
                    where email = %s
                    ''', (email,))

        if cur.rowcount:
            result = cur.fetchone()

            cur.execute('''
                        select instrument
                        from profile_instrument
                        where email = %s
                        ''', (result['email'],))

            result['instruments'] = [row['instrument'] for row in cur.fetchall()]

            cur.execute('''
                        select genre
                        from profile_genre
                        where email = %s
                        ''', (result['email'],))

            result['genres'] = [row['genre'] for row in cur.fetchall()]

            status = 'ok'

        return json.dumps(result), status

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

            path = '%s/www-root/api/pics/%s' % (self.root, self.args['profile_picture'])
            if not os.path.exists(path):
                os.mkdir('%s/www-root/api/pics/%s' % (self.root, self.args['profile_picture']))

            self.args['password'], self.args['salt'] = self._hash(self.args['password'])

            # Insert new profile and get rowcount
            cur.execute('''

                        insert into profile (email, name, password, role, location, bio, phone, profile_picture, salt)
                        values (%(email)s, %(name)s,
                                %(password)s, %(role)s,
                                %(location)s, %(bio)s,
                                %(phone)s, %(profile_picture)s,
                                %(salt)s)
                        ''', self.args)

            success = bool(cur.rowcount)

            genres = self.args.get('genres')
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
                status = 'ok'

        else:
            self.logger.error('Profile exists: %s', self.args)
            status = 'exists'

        cur.close()

        return None, status

    def login(self):
        """Verify posted information is correct"""
        cur = self.db_conn.cursor()

        cur.execute('''
                    select *
                    from profile
                    where email=%(email)s
                    ''', self.args)

        if not cur.rowcount:
            self.logger.error('Profile does not exist')
            status = 'not'

        else:
            self.args['password'], self.args['salt'] = self._hash(self.args['password'], salt=cur.fetchone()['salt'])

            cur.execute('''
                        select 1
                        from profile
                        where email=%(email)s and password=%(password)s and salt=%(salt)s
                        ''', self.args)

            if not cur.rowcount:
                self.logger.error('Bad email/password combo')
            else:
                status = 'ok'

        return None, status

request_handler = API()
