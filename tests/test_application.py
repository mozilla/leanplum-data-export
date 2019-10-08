import pytest

from leanplum_data_export.application import App


@pytest.fixture
def app():
    return App()


class TestApplication(object):

    def test_return_value(self, app):
        pass
