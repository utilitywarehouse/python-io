from unittest import mock

import pandas as pd
import pytest

from iolib import read_storage
from iolib.storage import read_blob


def test_read_blob():
    m_blob = mock.MagicMock()
    m_blob.name = 'name.csv'
    m_decode = m_blob.download_as_string.return_value.decode
    m_decode.return_value = 'a,b,c\n1,2,3'

    actual = read_blob(m_blob, encoding='utf-8', usecols=[0,1])
    expected = pd.DataFrame([[1, 2]], columns=['a', 'b'])

    pd.testing.assert_frame_equal(expected, actual)
    m_decode.assert_called_once_with('utf-8')


def test_read_blob_errors_if_not_csv():
    m_blob = mock.MagicMock()
    m_blob.name = 'name.txt'
    with pytest.raises(Exception) as error:
        read_blob(m_blob, encoding='utf-8', usecols=[0,1])
    assert 'Only CSV currently supported' == str(error.value)


@mock.patch('iolib.storage.Client')
@mock.patch('iolib.storage.read_blob')
def test_read_storage_with_blob_name(m_read_blob, m_client):
    m_get_bucket = m_client.return_value.get_bucket
    m_bucket = m_get_bucket.return_value
    m_blob = m_bucket.blob.return_value

    actual = read_storage('<bucket_name>', blob_name='<blob_name>', k=1)
    expected = m_read_blob.return_value
    assert expected == actual

    m_client.assert_called_once_with()
    m_get_bucket.assert_called_once_with('<bucket_name>')
    m_bucket.blob.assert_called_once_with('<blob_name>')
    m_read_blob.assert_called_once_with(m_blob, encoding='utf-8', k=1)


@mock.patch('iolib.storage.Client')
@mock.patch('iolib.storage.read_blob')
def test_read_storage_with_prefix(m_read_blob, m_client):
    m_get_bucket = m_client.return_value.get_bucket
    m_bucket = m_get_bucket.return_value
    m_list_blobs = m_bucket.list_blobs
    m_blob_1, m_blob_2 = mock.MagicMock(), mock.MagicMock()
    m_list_blobs.return_value = [m_blob_1, m_blob_2]
    m_read_blob.side_effect = [pd.DataFrame([{'a': 1}]), pd.DataFrame([{'a': 2}])]

    actual = read_storage('<bucket_name>', prefix='<prefix>', k=1)
    expected = pd.DataFrame([{'a': 1}, {'a': 2}])
    pd.testing.assert_frame_equal(expected, actual)

    m_client.assert_called_once_with()
    m_get_bucket.assert_called_once_with('<bucket_name>')
    m_list_blobs.assert_called_once_with(prefix='<prefix>', max_results=500)
    assert 2 == m_read_blob.call_count
    calls = [mock.call(m_blob_1, encoding='utf-8', k=1),
             mock.call(m_blob_2, encoding='utf-8', k=1)]
    assert calls == m_read_blob.call_args_list


def test_read_storage_errors_if_no_blob_name_or_prefix():
    with pytest.raises(AssertionError) as error:
        read_storage('<bucket_name>')
    assert 'Required blob_name or prefix in read_storage' == str(error.value)


@mock.patch('iolib.storage.Client')
@mock.patch('iolib.storage.read_blob')
def test_read_storage_with_service_account(m_read_blob, m_client):
    read_storage('<bucket_name>',
                 blob_name='<blob_name>',
                 service_account_json='<service_account_json>')
    m_client.from_service_account_json.assert_called_once_with('<service_account_json>')
    m_client.from_service_account_json.return_value.get_bucket.assert_called_once_with('<bucket_name>')
