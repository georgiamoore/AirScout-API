import pytest

from api import api

@pytest.fixture
def client():
    with api.test_client() as client:
        yield client


def test_api_ping(client):
    res = client.get('/ping')
    assert res.json == {'ping': 'pong'}