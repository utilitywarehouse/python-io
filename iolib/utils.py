import os

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials


def build_google_api(name, version, scopes, service_account_json=None):
    if not service_account_json:
        service_account_json = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    credentials = Credentials.from_service_account_file(service_account_json,
                                                        scopes=scopes)
    return build(name, version, credentials=credentials)
