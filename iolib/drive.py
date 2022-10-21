import pandas as pd

from .utils import build_google_api, to_snakecase


API_NAME = 'drive'
API_VERSION = 'v3'


def build(readonly=False, service_account_json=None):
    if readonly:
        scope = 'https://www.googleapis.com/auth/drive.metadata.readonly'
    else:
        scope = 'https://www.googleapis.com/auth/drive'

    return build_google_api(API_NAME,
                            API_VERSION,
                            scopes=[scope],
                            service_account_json=service_account_json)


def list_drive(name=None,
               folder_id=None,
               mime_type=None,
               drive_id=None,
               service_account_json=None):
    """
    List files from Google Drive.

    Parameters
    ----------
    name : str, optional
        Google drive file name.
    folder_id : str, optional
        Google drive folder id. Found in the url as
        https://drive.google.com/drive/folders/<folder_id>.
    mime_type : str, optional
        Drive file mime type to filter.
        Example: 'application/vnd.google-apps.spreadsheet'.
    drive_id : str, optional
        Shared drive if if any, formatted as folder_id.
    service_account_json : str, optional
        Path to service account json file. Default as the one set in the
        environment as `GOOGLE_APPLICATION_CREDENTIALS`

    Returns
    -------
    df : pandas.DataFrame(kind, id, name, mime_type)
    """
    api = build(readonly=True, service_account_json=service_account_json)
    kwargs = {'q': format_search_query(name, folder_id, mime_type)}
    if drive_id:
        kwargs.update(corpora='drive',
                      driveId=drive_id,
                      includeItemsFromAllDrives=True,
                      supportsAllDrives=True)
    result = api.files().list(**kwargs).execute()['files']
    return pd.DataFrame(result).rename(columns=to_snakecase)


def format_search_query(name=None, folder_id=None, mime_type=None):
    """
    Format query to use in Google Drive `files.list`.

    Examples
    --------
    >>> format_search_query(name="report")
    'name = "report"'
    >>> format_search_query(mime_type='application/json')
    'mimeType = "application/json"'
    >>> format_search_query(folder_id='vtvyuhoi17523jiuyf12')
    '"vtvyuhoi17523jiuyf12" in parents'
    >>> format_search_query(name="report", folder_id='vtvyuhoi17523jiuyf12')
    'name = "report" and "vtvyuhoi17523jiuyf12" in parents'
    """
    queries = []
    if name:
        queries.append(f'name = "{name}"')
    if folder_id:
        queries.append(f'"{folder_id}" in parents')
    if mime_type:
        queries.append(f'mimeType = "{mime_type}"')
    return ' and '.join(queries) or None
