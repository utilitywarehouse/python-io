from unittest import mock

import pandas as pd
import pytest

from iolib import list_drive, list_drive_permissions
from iolib.drive import (
    API_NAME,
    API_VERSION,
    build,
    format_search_query,
    DrivePermissions,
)


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


@mock.patch('iolib.drive.build')
def test_drive_permissions_init(m_build):
    permissions = DrivePermissions()
    assert m_build.return_value.permissions.return_value == permissions.api
    m_build.assert_called_once_with(service_account_json=None)


@mock.patch('iolib.drive.build')
def test_drive_permissions_init_with_service_account_json(m_build):
    permissions = DrivePermissions(service_account_json='<service_account_json>')
    assert m_build.return_value.permissions.return_value == permissions.api
    m_build.assert_called_once_with(service_account_json='<service_account_json>')


@mock.patch.object(DrivePermissions, '__init__', return_value=None)
def test_drive_permissions_list(_):
    m_api = mock.MagicMock()
    response = {
        'permissions': [
            {
                'id': '<id>',
                'type': '<type>',
                'email': '<email>',
                'role': '<role>',
            }
        ]
    }
    DrivePermissions.api = m_api
    m_api.list.return_value.execute.return_value = response

    permissions = DrivePermissions()
    actual = permissions.list('<item_id>')
    expected = pd.DataFrame([{'id': '<id>',
                              'type': '<type>',
                              'email': '<email>',
                              'role': '<role>'}])
    pd.testing.assert_frame_equal(expected, actual)
    m_api.list.assert_called_once_with(
        fileId='<item_id>',
        fields='permissions/id,permissions/type,permissions/role,permissions/emailAddress')
    m_api.list.return_value.execute.assert_called_once_with()


def test_drive_permissions_validate_type():
    DrivePermissions._validate_type('user')
    assert True


def test_drive_permissions_validate_type_errors():
    with pytest.raises(AssertionError) as error:
        DrivePermissions._validate_type('other')
    assert 'Invalid type: "other"' == str(error.value)


def test_drive_permissions_validate_role():
    DrivePermissions._validate_role('reader')
    assert True


def test_drive_permissions_validate_role_errors():
    with pytest.raises(AssertionError) as error:
        DrivePermissions._validate_role('other')
    assert 'Invalid role: "other"' == str(error.value)


@mock.patch.object(DrivePermissions, '_validate_type')
@mock.patch.object(DrivePermissions, '_validate_role')
@mock.patch.object(DrivePermissions, '__init__', return_value=None)
def test_drive_permissions_create(_, m_validate_role, m_validate_type):
    m_api = mock.MagicMock()
    DrivePermissions.api = m_api
    m_api.create.return_value.execute.return_value = {
        'id': '<id>',
        'something-else': 'bah'
    }

    permissions = DrivePermissions()
    actual = permissions.create('<item_id>', '<email>', '<role>')
    expected = {'id': '<id>'}
    assert expected == actual

    m_api.create.assert_called_once_with(
        fileId='<item_id>',
        body={'emailAddress': '<email>', 'type': 'user', 'role': '<role>'},
        sendNotificationEmail=False
    )
    m_api.create.return_value.execute.assert_called_once_with()
    m_validate_role.assert_called_once_with('<role>')
    m_validate_type.assert_called_once_with('user')


@mock.patch.object(DrivePermissions, '_validate_type')
@mock.patch.object(DrivePermissions, '_validate_role')
@mock.patch.object(DrivePermissions, '__init__', return_value=None)
def test_drive_permissions_create_non_user_or_group(_, m_validate_role, m_validate_type):
    m_api = mock.MagicMock()
    DrivePermissions.api = m_api
    m_api.create.return_value.execute.return_value = {
        'id': '<id>',
        'something-else': 'bah'
    }

    permissions = DrivePermissions()
    actual = permissions.create('<item_id>', '<email>', '<role>', type='domain')
    expected = {'id': '<id>'}
    assert expected == actual

    m_api.create.assert_called_once_with(
        fileId='<item_id>',
        body={'emailAddress': '<email>', 'type': 'domain', 'role': '<role>'}
    )
    m_api.create.return_value.execute.assert_called_once_with()
    m_validate_role.assert_called_once_with('<role>')
    m_validate_type.assert_called_once_with('domain')


@mock.patch.object(DrivePermissions, '_validate_role')
@mock.patch.object(DrivePermissions, '__init__', return_value=None)
def test_drive_permissions_update(_, m_validate_role):
    m_api = mock.MagicMock()
    DrivePermissions.api = m_api
    m_api.update.return_value.execute.return_value = {
        'id': '<id>',
        'something-else': 'bah'
    }

    permissions = DrivePermissions()
    actual = permissions.update('<item_id>', '<permission_id>', '<role>')
    expected = {'id': '<id>'}
    assert expected == actual

    m_api.update.assert_called_once_with(
        fileId='<item_id>',
        permissionId='<permission_id>',
        body={'role': '<role>'}
    )
    m_api.update.return_value.execute.assert_called_once_with()
    m_validate_role.assert_called_once_with('<role>')


@mock.patch.object(DrivePermissions, '__init__', return_value=None)
def test_drive_permissions_delete(_):
    m_api = mock.MagicMock()
    DrivePermissions.api = m_api

    permissions = DrivePermissions()
    actual = permissions.delete('<item_id>', '<permission_id>')
    expected = {'id': '<id>'}
    assert actual is None

    m_api.delete.assert_called_once_with(fileId='<item_id>',
                                         permissionId='<permission_id>')
    m_api.delete.return_value.execute.assert_called_once_with()


@mock.patch('iolib.drive.DrivePermissions')
def test_list_drive_permissions(m_drive_permissions):
    actual = list_drive_permissions('<item_id>')
    assert m_drive_permissions.return_value.list.return_value == actual
    m_drive_permissions.return_value.list.assert_called_once_with('<item_id>')
    m_drive_permissions.assert_called_once_with(None)


@mock.patch('iolib.drive.DrivePermissions')
def test_list_drive_permissions_with_service_account_json(m_drive_permissions):
    actual = list_drive_permissions(
        item_id='<item_id>',
        service_account_json='<service_account_json>')
    assert m_drive_permissions.return_value.list.return_value == actual
    m_drive_permissions.return_value.list.assert_called_once_with('<item_id>')
    m_drive_permissions.assert_called_once_with('<service_account_json>')
