import os
import re

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials


# Common keys/fields returned from third-party map to normalized keys to be
# used accross the whole library for consistency.
NORMALIZED_KEYS_MAP = {
    'pass': 'password',
    'passwd': 'password',
    'username': 'user',
    'user_name': 'user',
    'email_address': 'email',
}


def build_google_api(name, version, scopes, service_account_json=None):
    if not service_account_json:
        service_account_json = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    credentials = Credentials.from_service_account_file(service_account_json,
                                                        scopes=scopes)
    return build(name, version, credentials=credentials)


def to_snakecase(value):
    # Add underscore between lower and uppercased letter.
    value = re.sub(r'(?<=[a-z])([A-Z])', r'_\1', value)
    # Replace dash or space by underscore.
    value = re.sub(r'[ -]', '_', value)
    value = value.lower()
    return value


def normalize_key(key):
    """
    Normalized value from a key. The key will be snakecased and replaced if
    found in our common keys dictionary. This function is used for consistency
    in the function returns. For example, all the following keys will be
    returned as "user": "USER", "username", "User_Name".
    """
    key = to_snakecase(key)
    return NORMALIZED_KEYS_MAP.get(key, key)
