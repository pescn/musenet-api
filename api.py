#!/usr/bin/env python
"""API for app that queries backend
"""

import logging
import os
import json
from datetime import datetime
from random import randint
from math import radians, cos, sin, asin, sqrt
from urlparse import parse_qs
from passlib.hash import pbkdf2_sha256
import MySQLdb, MySQLdb.cursors

AD_DAYS_LIMIT = 30
EARTH_RADIUS_KM = 6371
KM_TO_M = 0.62137119

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
              'url_params': {
                  'required': [],
                  'optional': ['email'],
              },
              'args': {
                  'required': [],
                  'optional': []
              },
              'returns': 'application/json',
            },

            { 'name': 'create_profile',
              'method': 'post',
              'url_params': {
                  'required': [],
                  'optional': [],
              },
              'args': {
                  'required': ['email', 'password', 'role', 'location'],
                  'optional': ['genres', 'instruments', 'name', 'bio', 'phone']
              },
              'returns': 'application/json',
            },

            { 'name': 'edit_profile',
              'method': 'post',
              'url_params': {
                  'required': ['email'],
                  'optional': [],
              },
              'args': {
                  'required': [],
                  'optional': ['genres', 'instruments', 'name', 'bio', 'phone', 'role', 'location']
              },
              'returns': 'text/plain',
            },

            { 'name': 'get_group',
              'method': 'get',
              'url_params': {
                  'required': [],
                  'optional': ['profile_email', 'group_id'],
              },
              'args': {
                  'required': [],
                  'optional': []
              },
              'returns': 'application/json',
            },

            { 'name': 'create_group',
              'method': 'post',
              'url_params': {
                  'required': [],
                  'optional': [],
              },
              'args': {
                  'required': ['name', 'profiles'],
                  'optional': ['genres', 'bio', 'location', 'email', 'type']
              },
              'returns': 'application/json',
            },

            { 'name': 'edit_group',
              'method': 'post',
              'url_params': {
                  'required': ['group_id'],
                  'optional': [],
              },
              'args': {
                  'required': [],
                  'optional': ['name', 'genres', 'bio', 'location', 'email', 'type']
              },
              'returns': 'text/plain',
            },

            { 'name': 'login',
              'method': 'post',
              'url_params': {
                  'required': [],
                  'optional': [],
              },
              'args': {
                  'required': ['email', 'password'],
                  'optional': []
              },
              'returns': 'text/plain',
            },

            { 'name': 'create_profile_ad',
              'method': 'post',
              'url_params': {
                  'required': ['email'],
                  'optional': [],
              },
              'args': {
                  'required': ['looking_for'],
                  'optional': ['genre', 'instrument', 'description']
              },
              'returns': 'application/json',
            },

            { 'name': 'create_group_ad',
              'method': 'post',
              'url_params': {
                  'required': ['group_id'],
                  'optional': [],
              },
              'args': {
                  'required': ['looking_for'],
                  'optional': ['genre', 'instrument', 'description']
              },
              'returns': 'application/json',
            },

            { 'name': 'get_ads',
              'method': 'get',
              'url_params': {
                  'required': [],
                  'optional': ['email', 'group_id', 'genre', 'instrument', 'role', 'keywords', 'location', 'date_range', ''],
              },
              'args': {
                  'required': [],
                  'optional': []
              },
              'returns': 'application/json',
              'note': 'May use either email or group_id, but not both.'
            },

            { 'name': 'add_profile_picture',
              'method': 'post',
              'url_params': {
                  'required': ['email'],
                  'optional': ['main'],
              },
              'args': {
                  'required': ['base64'],
                  'optional': []
              },
              'returns': 'application/json',
            },

            { 'name': 'add_group_picture',
              'method': 'post',
              'url_params': {
                  'required': ['group_id'],
                  'optional': ['main'],
              },
              'args': {
                  'required': ['base64'],
                  'optional': []
              },
              'returns': 'application/json',
            },

            { 'name': 'get_profile_picture',
              'method': 'get',
              'url_params': {
                  'required': ['email'],
                  'optional': [],
              },
              'args': {
                  'required': [],
                  'optional': []
              },
              'returns': 'application/json',
            },

            { 'name': 'get_group_picture',
              'method': 'get',
              'url_params': {
                  'required': ['group_id'],
                  'optional': [],
              },
              'args': {
                  'required': [],
                  'optional': []
              },
              'returns': 'application/json',
            },
          ]

INSTRUCTIONS = ('Parameters invalid.\n'
                'Please supply "?action=[your_action]&parameters=[your_param]" at the end of the url.\n\n'
                'Options:\n')

class API(object):
    """Default all requests as bad, posts will return 200 OK by default when returning None"""

    def __init__(self):
        """Set some base things"""
        self.root = os.path.expanduser('~')

        self.logger = logging.getLogger(os.path.basename(__file__.split('.')[0]))
        self.logger.setLevel(logging.INFO)

        if not self.logger.handlers:
            handler = logging.FileHandler('%s/www-logs/api.log' % self.root)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        self.env = None
        self.resp = None
        self.query = None
        self.args = None

        self.db_conn = MySQLdb.connect(**CNX)

    def __call__(self, environment, response):
        """Set the environment variables and response function to self, then handle request"""
        result = None
        status = 'bad'
        _type = 'text/plain'

        try:
            self.env = environment.copy()
            self.resp = response

            # Check the action and proceed if correct
            self.query = parse_qs(self.env['QUERY_STRING'])
            action = self.parse_action()

            if action:
                result, status = getattr(self, action['name'])()

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
                else:
                    result = json.dumps(result)

                _type = action['returns']

            else:
                result = STATUS[status]
                self.logger.error(result)

        except Exception:
            result = STATUS['error']
            self.logger.error(result)
            self.logger.exception('')

        finally:
            self.db_conn.commit() if status == 'ok' else self.db_conn.rollback()

            self.start_resp(STATUS[status], _type)
            yield result

    def format_dict(self, _dict):
        """Do stuff"""
        _str = ''
        for key, val in _dict.items():
            if isinstance(val, dict):
                _str += '\t%s\n' % (key)
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

    @staticmethod
    def query_str(insert=False, entity='', **kwargs):
        """Generate a DB query string"""
        if not insert:
            return ['{0}.{1}=%({0})s'.format(entity, name) for name in kwargs] if entity else ['{0}=%({0})s'.format(name) for name in kwargs]
        else:
            # Need to populate two lists together due to the fact that dictionary order in python
            # is not guaranteed to be the same each time
            query_str = []
            args = []
            for arg in kwargs:
                args.append(arg)
                query_str.append('%({0})s'.format(arg))

            return query_str, args

    def exists(self, where, **kwargs):
        """See if something exists in db"""
        cur = self.db_conn.cursor()

        query = self.query_str(**kwargs)
        cur.execute('''
                    select *
                    from {0}
                    where {1}
                    '''.format(where, ' and '.join(query)), kwargs)

        cur.close()
        return bool(cur.rowcount)

    def start_resp(self, status, _type):
        """Easier way to start response"""
        self.resp(status, [('Content-Type', _type)])

    def get_salt(self, email):
        """Get salt"""
        cur = self.db_conn.cursor()

        cur.execute('''
                    select salt
                    from profiles
                    where email = %s
                    ''', (email,))


        return cur.fetchall()[0]['salt']


    def parse_action(self):
        """Check arguments and methods from ACTIONS dictionary"""
        result = None

        name = str(self.query.pop('action')[0] if self.query.get('action') else None).lower()
        method = self.env['REQUEST_METHOD']

        self.logger.info("%s - %s", method, name)

        # First check if its actually a method
        if hasattr(self, name):

            # Cycle through the ACTIONS dictionary
            for action in ACTIONS:

                # Correct name and request type
                if action['name'] == name and action['method'].lower() == method.lower():

                    # Check arguments
                    msg_size = int(self.env.get('CONTENT_LENGTH', 0))

                    if action['args']['required'] or action['args']['optional']:
                        self.args = json.loads(self.env['wsgi.input'].read(msg_size))

                    needs_args = bool(action['args']['required'])
                    args = action['args']['required'] + action['args']['optional']
                    no_bad_args = all(arg in args and val for arg, val in self.args.items()) if self.args else True
                    has_required_args = needs_args and all(arg in self.args for arg in action['args']['required']) if self.args else False

                    needs_query = bool(action['url_params']['required'])
                    params = action['url_params']['required'] + action['url_params']['optional']
                    no_bad_query = all(arg in params and val for arg, val in self.query.items()) if self.query else True
                    has_required_query = needs_query and all(arg in self.query for arg in action['url_params']['required']) if self.query else False

                    args_okay = no_bad_args and ((needs_args and has_required_args) or not needs_args)
                    query_okay = no_bad_query and ((needs_query and has_required_query) or not needs_query)

                    result = action if args_okay and query_okay else None

        # If the check fails
        return result

    @staticmethod
    def haversine(lon1, lat1, lon2, lat2):
        """Calculate the great circle distance between two points on the earth (specified in decimal degrees)
        https://stackoverflow.com/questions/15736995/how-can-i-quickly-estimate-the-distance-between-two-latitude-longitude-points"""

        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

        # haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        const = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        circle = 2 * asin(sqrt(const))

        # Radius of earth in kilometers is 6371
        kilometers = EARTH_RADIUS_KM * circle

        # Convert km to m
        miles = kilometers * KM_TO_M

        return miles

    def get_profile(self):
        """Do a get on a profile"""
        result = None
        status = 'bad'

        email = self.query.get('email')

        cur = self.db_conn.cursor()

        if email:
            email = email[0]

            cur.execute('''
                        select email, role, location, name, bio, phone
                        from profiles
                        where email = %s
                        ''', (email,))
        else:
            cur.execute('''
                        select email, role, location, name, bio, phone
                        from profiles
                        ''')

        if cur.rowcount:
            results = cur.fetchall()

            for result in results:
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

            result = results[0] if len(results) == 1 else results

        else:
            status = 'not'

        return result, status

    def create_profile(self):
        """Create a new profile if all of the required parameters are in place and the email does not exist"""
        result = None
        status = 'bad'

        email = self.args['email']
        genres = self.args.pop('genres', None)
        instrs = self.args.pop('instruments', None)

        if not self.exists(where='profiles', email=email) and (not genres or isinstance(genres, list)) and (not instrs or isinstance(instrs, list)):

            # Create picture dir
            path = '%s/www-root/api/pics/%s' % (self.root, '%s' % email.replace("@", "_").replace(".", "_"))
            if not os.path.exists(path):
                os.mkdir(path)

            self.args['password'], self.args['salt'] = self._hash(self.args['password'])
            query_str, args = self.query_str(insert=True, **self.args)

            cur = self.db_conn.cursor()
            cur.execute('''
                        insert into profiles ({0})
                        values ({1})
                        '''.format(', '.join(args), ', '.join(query_str)), self.args)

            success = bool(cur.rowcount)

            if genres:
                for genre in genres:
                    cur.execute('''
                                insert into profile_genre (email, genre)
                                values (%s, %s)
                                ''', (email, genre))

                    success = success and bool(cur.rowcount)

            if instrs:
                for instr in instrs:
                    cur.execute('''
                                insert into profile_instrument (email, instrument)
                                values (%s, %s)
                                ''', (email, instr))

                    success = success and bool(cur.rowcount)

            if success:
                result = {'email':email}
                status = 'ok'

            cur.close()

        else:
            self.logger.error('Profile exists: %s', self.args)
            status = 'exists'

        return result, status

    def edit_profile(self):
        """Edit profile fields"""
        status = 'bad'

        email = self.query['email'][0]
        genres = self.args.pop('genres', None)
        instrs = self.args.pop('instruments', None)

        if not self.exists(where='profiles', email=email):
            self.logger.error('Profile does not exist')
            status = 'not'

        else:
            query_str = self.query_str(**self.args)

            self.args['email'] = email

            cur = self.db_conn.cursor()
            cur.execute('''
                        update profiles
                        set {0}
                        where email=%(email)s
                        '''.format(', '.join(query_str)), self.args)

            success = bool(cur.rowcount)

            if genres:
                for genre in genres:
                    cur.execute('''
                                update profile_genre
                                set genre = %s
                                where email = %s and %s not in (
                                    select genre
                                    from profile_genres
                                    where email = %s
                                )
                                ''', (genre, email) * 2)

                success = success and bool(cur.rowcount)

            if instrs:
                for instr in instrs:
                    cur.execute('''
                                update profile_instrument
                                set instrument = %s
                                where email = %s and %s not in (
                                    select instrument
                                    from profile_instrument
                                    where email = %s
                                )
                                ''', (instr, email) * 2)

                success = success and bool(cur.rowcount)

            if success:
                status = 'ok'

        return None, status

    def login(self):
        """Verify posted information is correct"""
        status = 'bad'

        email = self.args['email']

        if not self.exists(where='profiles', email=email):
            self.logger.error('Profile does not exist')
            status = 'not'

        else:
            self.args['password'], self.args['salt'] = self._hash(self.args['password'], salt=self.get_salt(email))

            cur = self.db_conn.cursor()
            cur.execute('''
                        select 1
                        from profiles
                        where email = %(email)s and
                              password = %(password)s and
                              salt = %(salt)s
                        ''', self.args)

            if cur.rowcount:
                status = 'ok'

        return None, status

    def create_group(self):
        """Create a group"""
        result = None
        status = 'bad'

        profiles = self.args.pop('profiles', None)
        genres = self.args.pop('genres', None)

        cur = self.db_conn.cursor()

        cur.execute('''
                    select group_id
                    from group
                    where name=%s
                    ''', (self.args['name'],))

        same_names = cur.fetchall()

        # Check to see if any group exists with the same name where one of the profiles submitted for the new group are in that group already
        # This prevents making a bunch of groups with the same name with the same profile
        same_name_same_profile = any(self.exists(where='group_profile', email=email, group_id=group['group_id']) for group in same_names for email in profiles)
        profiles_exist = all(self.exists(where='profiles', email=email) for email in profiles)

        if not same_name_same_profile and profiles_exist:
            query_str, args = self.query_str(insert=True, **self.args)

            cur = self.db_conn.cursor()
            cur.execute('''
                        insert into groups ({0})
                        values ({1})
                        '''.format(args, query_str), self.args)

            success = bool(cur.rowcount)

            if success:
                cur.execute('''
                            select LAST_INSERT_ID() as id
                            ''')
                group_id = cur.fetchall()[0]['id']

                for email in profiles:
                    cur.execute('''
                                insert into group_profile (email, group_id)
                                values (%s, %s)
                                ''', (email, group_id))

                success = success and bool(cur.rowcount)

            if genres and group_id:
                for genre in genres:

                    cur.execute('''
                                insert into group_genres (group_id, genre)
                                values (%s, %s)
                                ''', (group_id, genre))

                success = success and bool(cur.rowcount)

            if success:
                result = {'group_id': group_id}
                status = 'ok'

        elif not profiles_exist:
            status = 'not'

        elif same_name_same_profile:
            status = 'exists'

        return result, status

    def get_group(self):
        """Get a group"""
        result = None
        status = 'bad'

        group_id = self.query.get('group_id')
        email = self.query.get('profile_email')

        cur = self.db_conn.cursor()

        if group_id:
            group_id = group_id[0]

            cur.execute('''
                        select *
                        from groups
                        where group_id = %s
                        ''', (group_id,))
        elif email:
            email = email[0]

            cur.execute('''
                        select g.*
                        from group_profile p
                        inner join groups g
                        on p.email = %s and g.group_id = p.group_id
                        ''', (email,))

        elif email and group_id:
            email = email[0]
            group_id = group_id[0]

            cur.execute('''
                        select g.*
                        from group_profile p
                        inner join groups g
                        on p.email = %s and p.group_id = %s and g.group_id = p.group_id
                        ''', (email, group_id))
        else:
            cur.execute('''
                        select *
                        from groups
                        ''')

        if cur.rowcount:
            groups = cur.fetchall()

            for result in groups:
                cur.execute('''
                            select *
                            from group_profile
                            where group_id = %s
                            ''', (result['group_id'],))

                result['emails'] = [row['email'] for row in cur.fetchall()]

                cur.execute('''
                            select *
                            from group_genre
                            where group_id = %s
                            ''', (result['group_id'],))

                result['genres'] = [row['genre'] for row in cur.fetchall()]

            status = 'ok'

            result = groups[0] if len(groups) == 1 else groups

        else:
            status = 'not'

        return result, status

    def edit_group(self):
        """Edit a group"""
        status = 'bad'

        group_id = self.query['group_id'][0]
        genres = self.args.pop('genres', None)

        if self.exists(where='groups', group_id=group_id) and (not genres or isinstance(genres, list)):
            query_str = self.query_str(self.args)

            self.args['group_id'] = group_id

            cur = self.db_conn.cursor()
            cur.execute('''
                        update groups
                        set {0}
                        where group_id = %(group_id)s
                        '''.format(', '.join(query_str)), self.args)

            success = bool(cur.rowcount)

            if genres:
                for genre in genres:
                    cur.execute('''
                                update group_genre
                                set genre = %s
                                where group_id = %s and %s not in (
                                    select genre
                                    from group_genre
                                    where group_id = %s
                                )
                                ''', (genre, group_id) * 2)

                success = success and bool(cur.rowcount)

            if success:
                status = 'ok'

        else:
            self.logger.error('Group does not exist')
            status = 'not'

        return None, status

    def create_profile_ad(self):
        """Create an ad for a profile"""
        status = 'bad'
        result = None

        email = self.query.pop('email')[0]

        if self.exists(where='profiles', email=email) and not self.exists(where='ads', **self.args):
            query_str, args = self.query_str(insert=True, **self.args)

            cur = self.db_conn.cursor()
            cur.execute('''
                        insert into ads ({0})
                        values ({1})
                        '''.format(', '.join(args), ', '.join(query_str)), self.args)

            success = bool(cur.rowcount)

            if success:
                cur.execute('''
                            select LAST_INSERT_ID() as id
                            ''')
                ad_id = cur.fetchall()[0]['id']

                cur.execute('''
                            insert into profile_ad (email, ad_id)
                            values (%s, %s)
                            ''', (email, ad_id))

                success = success and bool(cur.rowcount)

            if success:
                status = 'ok'
                result = {'ad_id': ad_id}

        else:
            status = 'not'

        return result, status

    def create_group_ad(self):
        """Create an ad for a group"""
        status = 'bad'
        result = None

        group_id = self.query.pop('group_id')[0]

        if self.exists(where='groups', group_id=group_id) and not self.exists(where='ads', **self.args):
            query_str, args = self.query_str(insert=True, **self.args)

            cur = self.db_conn.cursor()
            cur.execute('''
                        insert into ads ({0})
                        values ({1})
                        '''.format(', '.join(args), ', '.join(query_str)), self.args)

            success = bool(cur.rowcount)

            if success:
                cur.execute('''
                            select LAST_INSERT_ID() as id
                            ''')
                ad_id = cur.fetchall()[0]['id']

                cur.execute('''
                            insert into group_ad (group_id, ad_id)
                            values (%s, %s)
                            ''', (group_id, ad_id))

                success = success and bool(cur.rowcount)

            if success:
                status = 'ok'
                result = {'ad_id': ad_id}
            else:
                status = 'error'

        else:
            status = 'not'

        return result, status

    def rank_ads(self, other_ads, entity_ads, entity):
        """Compare ads against a given entity and its associated ads in order to rank them for the best match"""
        ads = []
        self.logger.info(entity)
        for other_ad in other_ads:
            rank = 0

            entity_criteria = {'instrument': 'instruments', 'genre': 'genres'}
            ad_criteria = {'genre': 'genre', 'instrument': 'instrument'}

            # Don't include the ad if it's not looking for the entity
            if other_ad.get('looking_for', 'a') == entity.get('role', entity.get('type', 'b')):
                continue

            if 'type' in entity:
                del entity_criteria['instrument']

            # Calculate attribute rank
            criteria = [other_ad[key] in entity[val] for key, val in entity_criteria.items() if other_ad[key] in entity[val]]
            rank += len(criteria)

            # Calculate own ads attribute rank
            for entity_ad in entity_ads:
                if entity_ad['looking_for'] == other_ad['looking_for']:
                    criteria = [other_ad[key] == entity_ad[val] for key, val in ad_criteria.items() if other_ad[key] == entity_ad[val]]
                    rank += len(criteria) * 2

            # TODO: change all profiles and groups to include x and y coords in db
            # # Calculate distance rank by miles
            # if entity['location'] and other_ad['location']:
            #     (ent_x, ent_y) = entity['location'].split(':')
            #     (other_x, other_y) = other_ad['location'].split(':')
            #     distance = self.haversine(float(ent_x), float(ent_y), float(other_x), float(other_y))
            #     other_ad['distance'] = distance
            #     rank += distance
            # else:
            #     other_ad['distance'] = None

            # Calculate date rank by dividing thirty days by the number of days since the ad was created,
            # thus putting low number of days at a higher rank
            days = int(AD_DAYS_LIMIT / (datetime.today() - other_ad['created']).days)
            rank += days

            other_ad['rank'] = rank
            ads.append(other_ad)

        return ads

    def get_ads(self):
        """Get ads"""
        result = []

        email = self.query.get('email')[0] if self.query.get('email') else None
        group_id = self.query.get('group_id')[0] if self.query.get('group_id') else None

        if email and group_id:
            return None, 'bad'

        group_ids = []
        if email:
            self.query['profile_email'] = email
            groups = self.get_group()[0]
            group_ids = [group['group_id'] for group in groups] if groups else []

        group_ids.append(group_id)
        query_str = ['pa.email != %s'] + ['ga.group_id != %s'] * len(group_ids)
        query_str = ' or '.join(query_str)

        # Select all ads not associated with own groups or profile
        cur = self.db_conn.cursor()
        cur.execute('''
                    select a.*, ga.group_id, pa.email
                    from ads a
                    left join group_ad ga
                    on a.ad_id = ga.ad_id
                    left join profile_ad pa
                    on a.ad_id = pa.ad_id
                    where {0}
                    '''.format(query_str), (email,) + tuple(group_ids))

        ### PROFILE MATCH ###
        if email:
            profile = self.get_profile()[0]

            # Select ads associate with profile
            profile_ads = [ad for ad in cur if ad['email'] == profile['email']]

            # Select all ads that are looking for the profile's ad criteria or profile's attributes
            results = self.rank_ads(cur, profile_ads, profile)
            result = sorted(results, key=lambda k: k['rank'], reverse=True)

        ### GROUP MATCH ###
        elif group_id:
            group = self.get_group()[0]

            # Selet ads associated with group
            group_ads = [ad for ad in cur if ad['group_id'] == group['group_id']]

            # Select all ads that are looking for a group's ad criteria or group's attributes
            results = self.rank_ads(cur, group_ads, group)
            result = sorted(results, key=lambda k: k['rank'], reverse=True)

        else:
            result = cur.fetchall()

        for _ad in result:
            _ad['updated'] = _ad['updated'].strftime('%m-%d-%Y %H:%M:%S')
            _ad['created'] = _ad['created'].strftime('%m-%d-%Y %H:%M:%S')

        cur.close()

        return result, 'ok'

    def add_profile_picture(self):
        """Upload a profile picture with associated metadata"""
        status = 'bad'
        result = None

        email = self.query['email'][0]

        if self.exists(where='profiles', email=email):
            cur = self.db_conn.cursor()

            query_str, args = self.query_str(insert=True, **self.args)
            cur.execute('''
                        insert into pictures ({0})
                        values({1})
                        '''.format(', '.join(args), ', '.join(query_str)), self.args)

            success = bool(cur.rowcount)

            if success:
                cur.execute('''
                            select LAST_INSERT_ID() as id
                            ''')
                picture_id = cur.fetchall()[0]['id']

                if not self.args.get('main', None):
                    self.args['main'] = False

                cur.execute('''
                            insert into profile_picture (email, picture_id, main)
                            values (%s, %s, %s)
                            ''', (email, picture_id, self.args['main']))

            success = success and bool(cur.rowcount)

            if success:
                status = 'ok'
                result = {'picture_id': picture_id}

        else:
            status = 'not'

        return result, status

    def add_group_picture(self):
        """Upload a group picture with associated metadata"""
        status = 'bad'
        result = None

        group_id = self.query['group_id'][0]

        if self.exists(where='groups', group_id=group_id):
            cur = self.db_conn.cursor()

            query_str, args = self.query_str(insert=True, **self.args)
            cur.execute('''
                        insert into pictures ({0})
                        values({1})
                        '''.format(', '.join(args), ', '.join(query_str)), self.args)

            success = bool(cur.rowcount)

            if success:
                cur.execute('''
                            select LAST_INSERT_ID() as id
                            ''')
                picture_id = cur.fetchall()[0]['id']


                if not self.args.get('main', None):
                    self.args['main'] = False

                cur.execute('''
                            insert into group_picture (group_id, picture_id, main)
                            values (%s, %s, %s)
                            ''', (group_id, picture_id, self.args['main']))

            success = success and bool(cur.rowcount)

            if success:
                status = 'ok'
                result = {'picture_id': picture_id}

        else:
            status = 'not'

        return result, status

    def get_profile_picture(self):
        """Retrieve a profile picture with email"""
        status = 'bad'
        result = None

        email = self.query['email'][0]

        if self.exists(where='profiles', email=email):
            cur = self.db_conn.cursor()

            cur.execute('''
                        select p.base64, pp.main
                        from pictures p
                        inner join profile_picture pp
                        on pp.picture_id = p.picture_id
                        where pp.email = %s
                        ''', (email,))

            if cur.rowcount:
                self.logger.info("HERE")
                result = cur.fetchall()
                status = 'ok'

        else:
            status = 'not'

        return result, status

    def get_group_picture(self):
        """Retrieve a group picture with group_id"""
        status = 'bad'
        result = None

        group_id = self.query.pop('group_id')[0]

        if self.exists(where='groups', group_id=group_id):
            cur = self.db_conn.cursor()

            cur.execute('''
                        select p.base64, gp.main
                        from pictures p
                        inner join group_picture gp
                        on gp.picture_id = p.picture_id
                        where gp.group_id = %s
                        ''', (group_id,))

            if cur.rowcount:
                result = cur.fetchall()
                status = 'ok'

        else:
            status = 'not'

        return result, status

request_handler = API() # pylint: disable=C0103
