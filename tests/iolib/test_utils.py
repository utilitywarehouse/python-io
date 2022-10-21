from unittest import mock

import pytest

from iolib.utils import build_google_api, to_snakecase, normalize_key


@mock.patch('iolib.utils.Credentials')
@mock.patch('iolib.utils.build')
def test_build_google_api(m_build, m_credentials):
    name = '<name>'
    version = '<version>'
    scopes = '<scopes>'
    service_account_json = '<service_account_json>'
    actual = build_google_api(name, version, scopes, service_account_json)
    expected = m_build.return_value
    assert expected == actual
    credentials = m_credentials.from_service_account_file.return_value
    m_credentials.from_service_account_file.assert_called_once_with(
        service_account_json,
        scopes=scopes)
    m_build.assert_called_once_with(name, version, credentials=credentials)


@mock.patch('iolib.utils.Credentials')
@mock.patch('iolib.utils.build')
@mock.patch('iolib.utils.os')
def test_build_google_api_with_credentials_from_env(m_os, m_build, m_credentials):
    name = '<name>'
    version = '<version>'
    scopes = '<scopes>'
    service_account_json = '<service_account_json>'
    m_os.environ = {'GOOGLE_APPLICATION_CREDENTIALS': service_account_json}
    actual = build_google_api(name, version, scopes)
    expected = m_build.return_value
    assert expected == actual
    credentials = m_credentials.from_service_account_file.return_value
    m_credentials.from_service_account_file.assert_called_once_with(
        service_account_json,
        scopes=scopes)
    m_build.assert_called_once_with(name, version, credentials=credentials)


@pytest.mark.parametrize(('value', 'expected'), (
    ('to_csv', 'to_csv'),
    ('user name', 'user_name'),
    ('Email Address', 'email_address'),
    ('phoneNumber', 'phone_number'),
    ('toHTML', 'to_html'),
))
def test_to_snakecase(value, expected):
    actual = to_snakecase(value)
    assert expected == actual


@pytest.mark.parametrize(('value', 'expected'), (
    ('name', 'name'),
    ('user_name', 'user'),
))
def test_normalize_key(value, expected):
    with mock.patch('iolib.utils.to_snakecase', side_effect=lambda x: x) as m_to_snakecase:
        actual = normalize_key(value)
    assert expected == actual
    m_to_snakecase.assert_called_once_with(value)
