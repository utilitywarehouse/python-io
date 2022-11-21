from io import StringIO
import os

from google.cloud.storage import Client
import pandas as pd


def read_storage(bucket_name,
                 blob_name=None,
                 prefix=None,
                 service_account_json=None,
                 encoding='utf-8',
                 **kwargs):
    """
    Read blob or blobs from Storage as a dataframe. Call with blob name to read
    specific file or prefix to read from a list.

    Parameters
    ----------
    bucket_name : str
        Google Storage bucket name.
    blob_name : str, optional
        Google Storage blob name. Either this or prefix is required.
    prefix : str, optional
        Google Storage blob prefix. Use when want to retrieve a file from
        different parts. Either this or blob_name is required.
    service_account_json : str, optional
    encoding : str, default='utf-8'
        Path to service account json file. Default as the one set in the
        environment as `GOOGLE_APPLICATION_CREDENTIALS`
    **kwargs : kwargs
        kwargs passed to pandas.read_csv

    Returns
    -------
    df : pandas.DataFrame
        The resulting DataFrame.

    Examples
    --------
    >>> read_storage('my-bucket', blob_name='file1.csv')
    [...]
    >>> read_storage(bucket_name, prefix='file1-part')
    [...]
    """
    assert blob_name or prefix, 'Required blob_name or prefix in read_storage'

    if service_account_json:
        client = Client.from_service_account_json(service_account_json)
    else:
        client = Client()

    bucket = client.get_bucket(bucket_name)

    if blob_name:
        blob = bucket.blob(blob_name)
        return read_blob(blob, encoding=encoding, **kwargs)

    result = None
    for blob in bucket.list_blobs(prefix=prefix, max_results=500):
        df = read_blob(blob, encoding=encoding, **kwargs)
        if result is None:
            result = df
        else:
            result = pd.concat([result, df], ignore_index=True)
    return result


def read_blob(blob, encoding='utf-8', **kwargs):  # wiki: ignore
    _, ext = os.path.splitext(blob.name)
    if ext != '.csv':
        raise Exception('Only CSV currently supported')

    content = blob.download_as_string().decode(encoding)
    file = StringIO(content)

    return pd.read_csv(file, encoding=encoding, **kwargs)
