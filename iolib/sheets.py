import datetime
import string

import numpy as np
import pandas as pd

from .drive import list_drive, build as build_drive
from .utils import build_google_api


API_NAME = 'sheets'
API_VERSION = 'v4'
MIME_TYPE = 'application/vnd.google-apps.spreadsheet'


def build(readonly=False, service_account_json=None):
    if readonly:
        scope = 'https://www.googleapis.com/auth/spreadsheets.readonly'
    else:
        scope = 'https://www.googleapis.com/auth/spreadsheets'

    return build_google_api(API_NAME,
                            API_VERSION,
                            scopes=[scope],
                            service_account_json=service_account_json)


def read_sheets(sheet_id,
                sheet_name,
                header=True,
                service_account_json=None,
                **kwargs):
    """
    Read dataframe from Google Sheets.

    Parameters
    ----------
    sheet_id : str
        Google Sheet id.
    sheet_name : str
        Sheet name of the sheet.
    header : bool, default=True
        Whether use the first row as header or not.
    service_account_json : str, optional
        Path to service account json file. Default as the one set in the
        environment as `GOOGLE_APPLICATION_CREDENTIALS`
    kwargs : kwargs
        To pass to pandas.DataFrame along with the data from the sheet.

    Returns
    -------
    data : pandas.DataFrame
    """
    api = build(service_account_json=service_account_json)
    result = (
        api
        .spreadsheets()
        .values()
        .get(spreadsheetId=sheet_id,
             range=sheet_name,
             valueRenderOption='UNFORMATTED_VALUE',
             dateTimeRenderOption='FORMATTED_STRING')
        .execute()
    )
    if header:
        values = result['values'][1:]
        columns = result['values'][0]
    else:
        values = result['values']
        columns = None
    return (
        pd.DataFrame(values, columns=columns, **kwargs)
        .replace({None: np.nan})
    )


def write_sheets(data,
                 name,
                 if_exists='fail',
                 folder_id=None,
                 drive_id=None,
                 service_account_json=None):
    """
    Write dataframe to Google Sheets.

    Parameters
    ----------
    data : pandas.DataFrame
    name : str, optional
        Google drive file name.
    if_exists : str = 'fail'
        Behavior when the destination file exists. Value can be one of:
        'fail'
            If spreadsheet exists, raise ValueError.
        'replace'
            If spreadsheet exists, delete it, recreate it, and insert data.
    folder_id : str, optional
        Google Drive folder id. Found in the url as
        https://drive.google.com/drive/folders/<folder_id>.
    drive_id : str, optional
        Shared rive if if any, formatted as folder_id.
    service_account_json : str, optional
        Path to service account json file. Default as the one set in the
        environment as `GOOGLE_APPLICATION_CREDENTIALS`

    Returns
    -------
    result : dict(id)

    TODOs
    -----
      - Support `data` as a dictionary with keys as sheet ids and values as
        spreadsheet values (dataframes)
      - Support for `if_exists='append'`.
      - Support `data` with more than len([A-Z]) columns
    """
    assert if_exists in ('fail', 'replace'), \
        f'Invalid if_exists ("{if_exists}")'
    api = build(service_account_json=service_account_json)
    drive = build_drive()

    # Check if spreadsheet exists.
    kwargs = {'name': name, 'mime_type': MIME_TYPE}
    if folder_id:
        kwargs['folder_id'] = folder_id
    if drive_id:
        kwargs['drive_id'] = drive_id
    result = list_drive(**kwargs)
    if not result.empty:
        if len(result) > 1:
            raise Exception(f'Multiple spreadsheets found with name "{name}"')
        if if_exists == 'fail':
            raise ValueError(
                'Spreadsheet already exists. Use `if_exists="replace"` '
                'to replace the spreadsheet')
        # Delete file.
        drive.files().delete(fileId=result['id'].iloc[0]).execute()

    # Create empty spreadsheet.
    body = {'name': name, 'mimeType': MIME_TYPE}
    if folder_id:
        body['parents'] = [folder_id]
    result = drive.files().create(body=body, fields='id').execute()
    spreadsheet_id = result['id']

    # Generate range.
    n_rows, n_cols = data.shape
    n_rows += 1  # Include row for columns.
    assert n_cols <= len(string.ascii_uppercase), \
        f'Too many columns to write to spreadsheet ({n_cols})'
    range_name = f'Sheet1!A1:{string.ascii_uppercase[n_cols-1]}{n_rows}'

    # Populate spreadsheet.
    values = (
        data
        .applymap(format_cell_value)
        .replace({np.nan: None})
        .apply(list, axis=1)
    )
    values = [list(data.columns)] + list(values)
    kwargs = {
        'spreadsheetId': spreadsheet_id,
        'range': range_name,
        'valueInputOption': 'USER_ENTERED',
        'body': {'values': values},
    }
    result = api.spreadsheets().values().update(**kwargs).execute()
    return {'id': result['spreadsheetId']}


def format_cell_value(value):  # wiki: ignore
    """
    Format the value of the spreadsheet cell. This value will be serialized as
    JSON in the Google libraries.

    Examples
    --------
    >>> format_cell_value(1.2)
    1.2
    >>> format_cell_value(np.array([1, 2]))
    [1, 2]
    >>> format_cell_value(datetime.date(2022, 10, 13))
    '2022-10-13'
    >>> format_cell_value(datetime.datetime(2022, 10, 13, 11, 54, 13, 123456)
    '2022-10-13 11:54:13.123456'
    """
    if isinstance(value, set):
        return sorted(list(value))
    if isinstance(value, np.ndarray):
        return list(value)
    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()
    if isinstance(value, datetime.datetime):
        fmt = '%Y-%m-%d %H:%M:%S'
        if value.microsecond:
            fmt = f'{fmt}.%f'
        return value.strftime(fmt)
    if isinstance(value, datetime.date):
        return value.isoformat()
    return value
