import unittest
import os
import numpy as np
import pandas as pd
import pytest
from qa4sm_api.client_api import Connection, Session
from qa4sm_api.globals import QA4SM_TOKEN


class TestSession(unittest.TestCase):

    def setUp(self) -> None:
        self.session = Session(instance="test.qa4sm.eu", token="none")

    def test_url_pretty(self):
        url = self.session.url("asd", "jkl/")
        assert url == "https://test.qa4sm.eu/api/asd/jkl"

    @pytest.mark.skipif(QA4SM_TOKEN is None, reason="No Token available.")
    def test_login(self):
        status_code = self.session.login_with_token(QA4SM_TOKEN)
        assert status_code == 200


class TestConnectionTestInstance(unittest.TestCase):

    def setUp(self):
        self.con = Connection(instance="test.qa4sm.eu", token="None")

    def test_token_is_None(self):
        assert self.con.session.user is None

    def test_get_datasets(self):
        ds = self.con.datasets()
        assert ds.loc[1, "short_name"] == 'C3S_combined'
        assert ds.columns.size > 2

    def test_get_dataset_id(self):
        id = self.con.dataset_id("C3S_combined")
        assert id == 1

    def test_get_versions(self):
        vers = self.con.versions("C3S_combined")
        alsovers = self.con.versions(1)
        assert vers.equals(alsovers)

    def test_get_version_info(self):
        vers = self.con.version_info(70)
        assert vers["short_name"] == "C3S_V202505"
        assert vers["id"] == 70

    def test_get_variable_info(self):
        id = 1
        var = self.con.variable_info(id)
        assert var["short_name"] == "sm"
        assert var["id"] == id

    def test_get_filter_info(self):
        id = 1
        fil = self.con.filter_info(id)
        assert fil["name"] == "FIL_ALL_VALID_RANGE"
        assert fil["parameterised"] is np.False_
        assert fil["id"] == id

    def test_get_param_filter_info(self):
        id = 18
        fil = self.con.filter_info(id)
        assert fil["name"] == "FIL_ISMN_NETWORKS"
        assert fil["parameterised"] is np.True_
        assert fil["id"] == id

    def test_get_period(self):
        start, end = self.con.get_period(70)
        assert pd.to_datetime(start) < pd.to_datetime(end)


@pytest.mark.skipif(QA4SM_TOKEN is None, reason="No Token available.")
class TestConnectionWithToken(unittest.TestCase):

    def setUp(self):
        self.con = Connection(instance="test.qa4sm.eu", token=QA4SM_TOKEN)

    def test_url(self):
        assert (self.con.url("asd", "jkl/") ==
                "https://test.qa4sm.eu/api/asd/jkl")

    def test_user(self):
        user = self.con.user()
        assert user['auth_token'] == QA4SM_TOKEN
        assert user['username'] == self.con.session.user



if __name__ == '__main__':
    connection = TestConnectionWithToken()
    connection.setUp()
    connection.test_user()

    # connection = TestConnectionTestInstance()
    # connection.setUp()
