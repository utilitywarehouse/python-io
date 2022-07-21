import os

import requests


GITHUB_TOKEN = os.environ['GITHUB_TOKEN']
GITHUB_OWNER = 'utilitywarehouse'
GITHUB_REPOSITORY = 'python-io'
RELEASES_ENDPOINT = ('https://api.github.com/repos/'
                     f'{GITHUB_OWNER}/{GITHUB_REPOSITORY}/releases')

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
with open(os.path.join(root, 'VERSION')) as infile:
    version = infile.read().strip()
tag = f'v{version}'

session = requests.Session()
session.headers['Authorization'] = f'token {GITHUB_TOKEN}'

# Check if current version release exists.
response = session.get(f'{RELEASES_ENDPOINT}/tags/{tag}')

# Create version release if it doesn't exist.
if response.status_code == 404:
    response = session.post(
        RELEASES_ENDPOINT,
        json={'tag_name': tag, 'name': tag, 'prerelease': False})
    assert response.ok
    print(f'{tag} released')
