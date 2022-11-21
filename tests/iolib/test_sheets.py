import datetime
from unittest import mock

import numpy as np
import pandas as pd
import pytest

from iolib import write_sheets
from iolib.sheets import (
    API_NAME,
    API_VERSION,
    MIME_TYPE,
    build,
    format_cell_value,
)


@mock.patch('iolib.sheets.build_google_api')
def test_build(m_build_google_api):
    actual = build()
    expected = m_build_google_api.return_value
    m_build_google_api.assert_called_once_with(
        API_NAME,
        API_VERSION,
        scopes=['https://www.googleapis.com/auth/spreadsheets'],
        service_account_json=None)
    assert expected == actual


@mock.patch('iolib.sheets.build_google_api')
def test_build_readonly(m_build_google_api):
    actual = build(readonly=True)
    expected = m_build_google_api.return_value
    m_build_google_api.assert_called_once_with(
        API_NAME,
        API_VERSION,
        scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'],
        service_account_json=None)
    assert expected == actual


@pytest.mark.parametrize(('value', 'expected'), (
    (1, 1),
    (1.2, 1.2),
    ('foo', 'foo'),
    (None, None),
    ({1, 2}, [1, 2]),
    (np.array([1, 2]), [1, 2]),
    (datetime.date(2022, 10, 13), '2022-10-13'),
    (pd.Timestamp(datetime.datetime(2022, 10, 13, 11, 57, 36)), '2022-10-13 11:57:36'),
    (datetime.datetime(2022, 10, 13, 11, 54, 13), '2022-10-13 11:54:13'),
    (datetime.datetime(2022, 10, 13, 11, 54, 13, 123456), '2022-10-13 11:54:13.123456'),
))
def test_format_cell_value(value, expected):
    actual = format_cell_value(value)
    assert expected == actual


@mock.patch('iolib.sheets.build')
def test_write_sheets_errors_if_invalid_if_exists(m_build):
    with pytest.raises(AssertionError) as error:
        write_sheets(mock.ANY, mock.ANY, if_exists='foo')
    m_build.assert_not_called()
    assert 'Invalid if_exists ("foo")' == str(error.value)


@mock.patch('iolib.sheets.build')
@mock.patch('iolib.sheets.build_drive')
@mock.patch('iolib.sheets.list_drive')
def test_write_sheets_errors_if_multiple_files_with_same_name(m_list_drive, *args):
    name = '<name>'
    m_list_drive.return_value = pd.DataFrame({'name': [name, name]})
    with pytest.raises(Exception) as error:
        write_sheets(mock.ANY, name)
    assert f'Multiple spreadsheets found with name "{name}"' == str(error.value)


@mock.patch('iolib.sheets.build')
@mock.patch('iolib.sheets.build_drive')
@mock.patch('iolib.sheets.list_drive')
def test_write_sheets_errors_if_exists(m_list_drive, *args):
    name = '<name>'
    m_list_drive.return_value = pd.DataFrame({'name': [name]})
    with pytest.raises(ValueError) as error:
        write_sheets(mock.ANY, name)
    assert 'Spreadsheet already exists. Use `if_exists="replace"` to replace the spreadsheet' == str(error.value)


@mock.patch('iolib.sheets.build')
@mock.patch('iolib.sheets.build_drive')
@mock.patch('iolib.sheets.list_drive')
def test_write_sheets_errors_if_too_many_columns(m_list_drive, m_build_drive, *args):
    data = pd.DataFrame(columns=range(30))

    m_list_drive.return_value = pd.DataFrame()
    m_drive = m_build_drive.return_value
    m_create = m_drive.files.return_value.create
    m_create.return_value.execute.return_value = {'id': '<file_id>'}

    with pytest.raises(AssertionError) as error:
        write_sheets(data, mock.ANY)
    assert 'Too many columns to write to spreadsheet (30)' == str(error.value)


@mock.patch('iolib.sheets.format_cell_value', side_effect=lambda x: x)
@mock.patch('iolib.sheets.build')
@mock.patch('iolib.sheets.build_drive')
@mock.patch('iolib.sheets.list_drive')
def test_write_sheets(m_list_drive, m_build_drive, m_build, m_format_cell_value):
    data = pd.DataFrame([{'x': 1, 'y': 2}, {'x': 3, 'y': 4}])
    name = '<name>'

    m_list_drive.return_value = pd.DataFrame()
    m_drive = m_build_drive.return_value
    m_create = m_drive.files.return_value.create
    m_create.return_value.execute.return_value = {'id': '<file_id>'}
    m_update = m_build.return_value.spreadsheets.return_value.values.return_value.update
    m_update.return_value.execute.return_value = {'spreadsheetId': '<file_id>'}
    expected = {'id': '<file_id>'}

    actual = write_sheets(data, name)
    assert expected == actual

    range_name = 'Sheet1!A1:B3'
    values = [['x', 'y'], [1, 2], [3, 4]]
    m_build.assert_called_once_with(service_account_json=None)
    m_list_drive.assert_called_once_with(name=name, mime_type=MIME_TYPE)
    m_create.assert_called_once_with(body={'name': name, 'mimeType': MIME_TYPE},
                                     fields='id')
    m_update.assert_called_once_with(spreadsheetId='<file_id>',
                                     range=range_name,
                                     valueInputOption='USER_ENTERED',
                                     body={'values': values})
    assert 4 == m_format_cell_value.call_count
    m_format_cell_value.assert_has_calls([mock.call(i) for i in (1, 2, 3, 4)],
                                         any_order=True)


@mock.patch('iolib.sheets.format_cell_value', side_effect=lambda x: x)
@mock.patch('iolib.sheets.build')
@mock.patch('iolib.sheets.build_drive')
@mock.patch('iolib.sheets.list_drive')
def test_write_sheets_replaces_none_by_nan(m_list_drive, m_build_drive, m_build, m_format_cell_value):
    data = pd.DataFrame([{'x': 'A', 'y': np.nan}])
    name = '<name>'

    m_list_drive.return_value = pd.DataFrame()
    m_update = m_build.return_value.spreadsheets.return_value.values.return_value.update

    write_sheets(data, name)

    values = [['x', 'y'], ['A', None]]
    m_update.assert_called_once()
    assert m_update.call_args.kwargs['body'] == {'values': values}


@mock.patch('iolib.sheets.format_cell_value', side_effect=lambda x: x)
@mock.patch('iolib.sheets.build')
@mock.patch('iolib.sheets.build_drive')
@mock.patch('iolib.sheets.list_drive')
def test_write_sheets_overwrites_file(m_list_drive, m_build_drive, m_build, _):
    data = pd.DataFrame()
    name = '<name>'

    m_list_drive.return_value = pd.DataFrame({'id': ['<file_id>']})
    m_drive = m_build_drive.return_value
    m_delete = m_drive.files.return_value.delete
    m_create = m_drive.files.return_value.create
    m_create.return_value.execute.return_value = {'id': '<file_id>'}
    m_update = m_build.return_value.spreadsheets.return_value.values.return_value.update
    m_update.return_value.execute.return_value = {'spreadsheetId': '<file_id>'}
    expected = {'id': '<file_id>'}

    actual = write_sheets(data, name, if_exists='replace')
    assert expected == actual

    m_delete.assert_called_once_with(fileId='<file_id>')


@mock.patch('iolib.sheets.format_cell_value', side_effect=lambda x: x)
@mock.patch('iolib.sheets.build')
@mock.patch('iolib.sheets.build_drive')
@mock.patch('iolib.sheets.list_drive')
def test_write_sheets_with_location_params(m_list_drive, m_build_drive, m_build, _):
    data = pd.DataFrame()
    name = '<name>'
    folder_id = '<folder_id>'
    drive_id = '<drive_id>'

    m_list_drive.return_value = pd.DataFrame()
    m_drive = m_build_drive.return_value
    m_create = m_drive.files.return_value.create
    m_create.return_value.execute.return_value = {'id': '<file_id>'}
    m_update = m_build.return_value.spreadsheets.return_value.values.return_value.update
    m_update.return_value.execute.return_value = {'spreadsheetId': '<file_id>'}
    expected = {'id': '<file_id>'}

    actual = write_sheets(data, name, folder_id=folder_id, drive_id=drive_id)
    assert expected == actual

    m_list_drive.assert_called_once_with(name=name,
                                         mime_type=MIME_TYPE,
                                         folder_id=folder_id,
                                         drive_id=drive_id)


@mock.patch('iolib.sheets.format_cell_value', side_effect=lambda x: x)
@mock.patch('iolib.sheets.build')
@mock.patch('iolib.sheets.build_drive')
@mock.patch('iolib.sheets.list_drive')
def test_write_sheets_with_service_account(m_list_drive, m_build_drive, m_build, m_format_cell_value):
    data = pd.DataFrame()
    name = '<name>'
    service_account_json = '<service_account_json>'

    m_list_drive.return_value = pd.DataFrame()
    m_drive = m_build_drive.return_value
    m_create = m_drive.files.return_value.create
    m_create.return_value.execute.return_value = {'id': '<file_id>'}
    m_update = m_build.return_value.spreadsheets.return_value.values.return_value.update
    m_update.return_value.execute.return_value = {'spreadsheetId': '<file_id>'}
    expected = {'id': '<file_id>'}

    actual = write_sheets(data,
                          name,
                          service_account_json=service_account_json)
    assert expected == actual

    m_build.assert_called_once_with(service_account_json=service_account_json)
