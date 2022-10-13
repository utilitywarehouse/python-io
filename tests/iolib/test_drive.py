from unittest import mock

import pandas as pd
import pytest

from iolib import list_drive
from iolib.drive import API_NAME, API_VERSION, build, format_search_query


@mock.patch('iolib.drive.build_google_api')
def test_build(m_build_google_api):
    actual = build()
    expected = m_build_google_api.return_value
    m_build_google_api.assert_called_once_with(
        API_NAME,
        API_VERSION,
        scopes=['https://www.googleapis.com/auth/drive'],
        service_account_json=None)
    assert expected == actual


@mock.patch('iolib.drive.build_google_api')
def test_build_readonly(m_build_google_api):
    actual = build(readonly=True)
    expected = m_build_google_api.return_value
    m_build_google_api.assert_called_once_with(
        API_NAME,
        API_VERSION,
        scopes=['https://www.googleapis.com/auth/drive.metadata.readonly'],
        service_account_json=None)
    assert expected == actual


@mock.patch('iolib.drive.build_google_api')
def test_build_with_service_account_json(m_build_google_api):
    service_account_json = '<service_account_json>'
    actual = build(service_account_json=service_account_json)
    expected = m_build_google_api.return_value
    m_build_google_api.assert_called_once_with(
        API_NAME,
        API_VERSION,
        scopes=['https://www.googleapis.com/auth/drive'],
        service_account_json=service_account_json)
    assert expected == actual


@pytest.mark.parametrize(('kwargs', 'expected'), (
    ({}, None),
    ({'name': 'N'}, 'name = "N"'),
    ({'name': 'N', 'folder_id': 'F'}, 'name = "N" and "F" in parents'),
    ({'name': 'N', 'folder_id': 'F', 'mime_type': 'M'}, 'name = "N" and "F" in parents and mimeType = "M"'),
))
def test_format_search_query(kwargs, expected):
    actual = format_search_query(**kwargs)
    assert expected == actual


@mock.patch('iolib.drive.format_search_query')
@mock.patch('iolib.drive.build')
def test_list_drive(m_build, m_format_search_query):
    m_api = m_build.return_value
    kwargs = {'q': m_format_search_query.return_value}
    m_list = m_api.files.return_value.list
    m_list.return_value.execute.return_value = {
        'files': [{'id': 'I', 'mimeType': 'M'}]
    }
    actual = list_drive()
    expected = pd.DataFrame([{'id': 'I', 'mime_type': 'M'}])
    pd.testing.assert_frame_equal(expected, actual)
    m_build.assert_called_once_with(readonly=True, service_account_json=None)
    m_format_search_query.assert_called_once_with(None, None, None)
    m_list.assert_called_once_with(**kwargs)


@mock.patch('iolib.drive.format_search_query')
@mock.patch('iolib.drive.build')
def test_list_drive_with_search_params(m_build, m_format_search_query):
    m_api = m_build.return_value
    m_list = m_api.files.return_value.list
    m_list.return_value.execute.return_value = {'files': []}
    name = '<name>'
    folder_id = '<folder_id>'
    mime_type = '<mime_type>'
    list_drive(name=name, folder_id=folder_id, mime_type=mime_type)
    m_format_search_query.assert_called_once_with(name, folder_id, mime_type)


@mock.patch('iolib.drive.format_search_query')
@mock.patch('iolib.drive.build')
def test_list_drive_with_drive_id(m_build, m_format_search_query):
    m_api = m_build.return_value
    m_list = m_api.files.return_value.list
    m_list.return_value.execute.return_value = {'files': []}
    drive_id = '<drive_id>'
    kwargs = {'q': m_format_search_query.return_value,
              'corpora': 'drive',
              'driveId': '<drive_id>',
              'includeItemsFromAllDrives': True,
              'supportsAllDrives': True}
    list_drive(drive_id=drive_id)
    m_list.assert_called_once_with(**kwargs)


@mock.patch('iolib.drive.format_search_query')
@mock.patch('iolib.drive.build')
def test_list_drive_with_service_account_json(m_build, m_format_search_query):
    m_api = m_build.return_value
    m_list = m_api.files.return_value.list
    m_list.return_value.execute.return_value = {'files': []}
    service_account_json = '<service_account_json>'
    list_drive(service_account_json=service_account_json)
    m_build.assert_called_once_with(readonly=True,
                                    service_account_json=service_account_json)
