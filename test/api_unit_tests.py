#!/usr/bin/env python
"""Unit tests for API to verify it actually works
"""

# pylint: disable=dangerous-default-value

import unittest
import nose
import MySQLdb, MySQLdb.cursors
from nose.tools import eq_, assert_is_not_none, ok_
from requests import Session

from api import CNX
import api_tests_static as static

class APIUnitTests(unittest.TestCase):
    """Use nose"""

    def setUp(self):
        self.session = Session()
        self.db_conn = MySQLdb(**CNX)

    def get(self, action, data={}, params={}):
        """Wrapper function for get"""
        params = params + {'action': action}
        self.session.get(url=static.url, params=params, data=data)

    def post(self, action, data={}, params={}):
        """Wrapper function for get"""
        params = params + {'action': action}
        self.session.post(url=static.url, params=params, data=data)

    def test_01(self):
        """Create a profile, get it, and edit it"""

