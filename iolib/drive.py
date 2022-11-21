import numpy as np
import pandas as pd

from .utils import build_google_api, normalize_key


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
    return pd.DataFrame(result).rename(columns=normalize_key)


def format_search_query(name=None, folder_id=None, mime_type=None):  # wiki: ignore
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


class DrivePermissions:
    api = None

    def __init__(self, service_account_json=None):
        self.api = (
            build(service_account_json=service_account_json)
            .permissions()
        )

    def list(self, item_id):
        """
        List permissions for a given Google Drive item (file, folder or drive).

        Parameters
        ----------
        item_id : str
            Google Drive item id.

        Returns
        -------
        df : pandas.DataFrame(id, type, email, role)
        """
        fields = ['permissions/id',
                  'permissions/type',
                  'permissions/role',
                  'permissions/emailAddress']
        response = (
            self.api
            .list(fileId=item_id, fields=','.join(fields))
            .execute()
        )
        return (
            pd.DataFrame(response['permissions'])
            .rename(columns=normalize_key)
        )

    def create(self, item_id, email, role, type='user'):
        """
        Create a permission for a Google Drive item (file, folder or drive).

        Parameters
        ----------
        item_id : str
            Google Drive item id.
        email : str
            Email for permission (user email, group email...).
        role : str
            Permission role. This must be in (writer, commenter, reader).
        type : str, default='user'
            Permission type. This must be in (user, group, domain, anyone)

        Returns
        -------
        result : dict(id)
            Dictionary with permission id.
        """
        self._validate_type(type)
        self._validate_role(role)
        kwargs = {
            'fileId': item_id,
            'body': {'emailAddress': email, 'type': type, 'role': role}
        }
        if type in ('user', 'group'):
            kwargs['sendNotificationEmail'] = False
        result = self.api.create(**kwargs).execute()
        return {'id': result['id']}

    def update(self, item_id, permission_id, role):
        """
        Update a permission for a Google Drive item (file, folder or drive).

        Parameters
        ----------
        item_id : str
            Google Drive item id.
        permission_id : str
            Permission id.
        role : str
            Permission role. This must be in (writer, commenter, reader).

        Returns
        -------
        result : dict(id)
            Dictionary with permission id.
        """
        self._validate_role(role)
        kwargs = {
            'fileId': item_id,
            'permissionId': permission_id,
            'body': {'role': role}
        }
        result = self.api.update(**kwargs).execute()
        return {'id': result['id']}

    def delete(self, item_id, permission_id):
        """
        Delete a permission for a Google Drive item (file, folder or drive).

        Parameters
        ----------
        item_id : str
            Google Drive item id.
        permission_id : str
            Permission id.

        Returns
        -------
        None
        """
        kwargs = {'fileId': item_id, 'permissionId': permission_id}
        self.api.delete(**kwargs).execute()
        return None

    @staticmethod
    def _validate_type(type):
        assert type in ('user', 'group', 'domain', 'anyone'), \
            f'Invalid type: "{type}"'

    @staticmethod
    def _validate_role(role):
        assert role in ('writer', 'commenter', 'reader'), \
            f'Invalid role: "{role}"'


def list_drive_permissions(item_id=None, service_account_json=None):
    """
    List permissions for a given Google Drive item (file, folder or drive).

    Parameters
    ----------
    item_id : str
        Google Drive item id.
    service_account_json : str, optional
        Path to service account json file. Default as the one set in the
        environment as `GOOGLE_APPLICATION_CREDENTIALS`

    Returns
    -------
    df : pandas.DataFrame(id, type, email, role)

    See also
    --------
    iolib.drive.DrivePermissions
    """
    return DrivePermissions(service_account_json).list(item_id)


def set_drive_permissions(item_id,
                          permissions,
                          mode='update',
                          service_account_json=None):
    """
    Set permissions to a given Google Drive item (file, folder or drive). The
    permissions provided can either update or replace the existing ones (update
    doesn't delete permissions). Owner permissions won't be deleted, even if
    the owner is not part of the permissions passed and the mode is "replace".

    Parameters
    ----------
    item_id : str
        Google Drive item id.
    permissions : list of dicts or pandas.DataFrame
        Permissions to set, with required fields "email" and "role", and
        optional field "type".
    mode : str, default="update"
        Either "update" to create/update permissions, or "replace", to
        create/update/delete permissions.
    service_account_json : str, optional
        Path to service account json file. Default as the one set in the
        environment as `GOOGLE_APPLICATION_CREDENTIALS`

    Returns
    -------
    None
    """
    assert mode in ('update', 'replace'), f'Invalid mode: "{mode}"'
    if isinstance(permissions, pd.DataFrame):
        permissions = permissions.replace({np.nan: None}).to_dict('records')

    manager = DrivePermissions(service_account_json)
    current_permissions_map = (
        manager.list(item_id)
        .set_index('email')
        .to_dict('index')
    )
    for permission in permissions:
        permission = {k: v for k, v in permission.items() if v is not None}
        keys = set(permission.keys())
        assert keys in ({'email', 'role'}, {'email', 'role', 'type'}), \
            f'Permission with invalid keys: {keys}'
        current_permission = current_permissions_map.pop(permission['email'],
                                                         None)
        if not current_permission:
            manager.create(item_id, **permission)
        elif current_permission['role'] != permission['role']:
            manager.update(item_id,
                           current_permission['id'],
                           permission['role'])
    if mode == 'replace':
        for current_permission in current_permissions_map.values():
            if current_permission['role'] != 'owner':
                manager.delete(item_id, current_permission['id'])
