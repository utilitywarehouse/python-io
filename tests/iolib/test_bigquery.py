from unittest import mock

import numpy as np
import pandas as pd
import pytest

from iolib.bigquery import BigqueryTableManager


@mock.patch('iolib.bigquery.Client')
def test_bigquery_table_manager_creates_client(m_client):
    manager = BigqueryTableManager(dataset=mock.ANY, table=mock.ANY)
    assert m_client.return_value == manager.client


@mock.patch('iolib.bigquery.Client')
def test_bigquery_table_manager_creates_client_from_service_account(m_client):
    manager = BigqueryTableManager(dataset=mock.ANY,
                                   table=mock.ANY,
                                   service_account_json='<service_account_json>')
    m_client.from_service_account_json.assert_called_once_with('<service_account_json>')
    assert m_client.from_service_account_json.return_value == manager.client


@mock.patch('iolib.bigquery.Client')
def test_bigquery_table_manager_defines_dataset_and_table_as_passed(m_client):
    manager = BigqueryTableManager(dataset='<dataset>', table='<table>')
    assert '<dataset>' == manager.dataset
    assert '<table>' == manager.table
    assert 'project' not in vars(manager.client)  # BQ client sets project automatically from service account.


@mock.patch('iolib.bigquery.Client')
def test_bigquery_table_manager_defines_dataset_and_table_by_parsing_table_id(m_client):
    manager = BigqueryTableManager(table_id='<dataset>.<table>')
    assert '<dataset>' == manager.dataset
    assert '<table>' == manager.table
    assert 'project' not in vars(manager.client)  # BQ client sets project automatically from service account.


@mock.patch('iolib.bigquery.Client')
def test_bigquery_table_manager_defines_project_dataset_and_table_by_parsing_table_id(m_client):
    manager = BigqueryTableManager(table_id='<project>.<dataset>.<table>')
    assert '<dataset>' == manager.dataset
    assert '<table>' == manager.table
    assert '<project>' == manager.client.project


@mock.patch('iolib.bigquery.Client')
def test_bigquery_table_manager_defines_project_as_passed(m_client):
    manager = BigqueryTableManager(dataset=mock.ANY,
                                   table=mock.ANY,
                                   project='<project>')
    assert '<project>' == manager.client.project


@mock.patch('iolib.bigquery.Client')
def test_bigquery_table_manager_errors_if_invalid_table_id(m_client):
    with pytest.raises(AssertionError) as error:
        BigqueryTableManager('foo.bar.egg.bacon')
    assert 'Invalid table_id `foo.bar.egg.bacon`' == str(error.value)


@mock.patch('iolib.bigquery.Client')
def test_bigquery_table_manager_errors_if_no_dataset_and_table(m_client):
    with pytest.raises(AssertionError) as error:
        BigqueryTableManager()
    assert 'dataset and table are required, either passed directly or as part of table_id' == str(error.value)


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
@mock.patch.object(BigqueryTableManager, 'client', new_callable=mock.PropertyMock())
def test_bigquery_table_manager_reads_with_query(m_client, _):
    manager = BigqueryTableManager()
    manager.dataset = '<dataset>'
    manager.table = '<table>'
    manager.client.project = '<project>'
    actual = manager.read(query='SELECT foo FROM `{table_id}`')
    m_client.query.assert_called_once_with(
        'SELECT foo FROM `<project>.<dataset>.<table>`')
    assert actual == m_client.query.return_value.to_dataframe.return_value.replace.return_value


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
@mock.patch.object(BigqueryTableManager, 'client', new_callable=mock.PropertyMock())
def test_bigquery_table_manager_reads_without_query(m_client, _):
    manager = BigqueryTableManager()
    manager.dataset = '<dataset>'
    manager.table = '<table>'
    m_client.project = '<project>'
    actual = manager.read()
    m_client.query.assert_called_once_with('SELECT * FROM `<project>.<dataset>.<table>`')
    assert actual == m_client.query.return_value.to_dataframe.return_value.replace.return_value


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
@mock.patch.object(BigqueryTableManager, 'client', new_callable=mock.PropertyMock())
def test_bigquery_table_manager_replaces_none_by_nan_when_reading(m_client, _):
    manager = BigqueryTableManager()
    m_client.query.return_value.to_dataframe.return_value = pd.DataFrame([{'x': None}])
    actual = manager.read(query='SELECT foo FROM bar')
    expected = pd.DataFrame([{'x': np.nan}])
    pd.testing.assert_frame_equal(expected, actual)
