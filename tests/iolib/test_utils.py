from unittest import mock

from iolib.utils import build_google_api


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
