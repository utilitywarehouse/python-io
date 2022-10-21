import os
import re

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials


def build_google_api(name, version, scopes, service_account_json=None):
    if not service_account_json:
        service_account_json = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    credentials = Credentials.from_service_account_file(service_account_json,
                                                        scopes=scopes)
    return build(name, version, credentials=credentials)


def to_snakecase(value):
    # Add underscore between lower and uppercased letter.
    value = re.sub(r'(?<=[a-w])([A-W])', r'_\1', value)
    # Replace dash or space by underscore.
    value = re.sub(r'[ -]', '_', value)
    value = value.lower()
    return value
