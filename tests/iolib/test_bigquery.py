from unittest import mock

from google.cloud.bigquery import (
    Dataset,
    DatasetReference,
    Table,
    TableReference,
    SchemaField,
)
from google.api_core.exceptions import NotFound
import numpy as np
import pandas as pd
import pytest

from iolib.bigquery import BigqueryTableManager, parse_schema
from iolib import read_bigquery, iread_bigquery, write_bigquery


def raise_not_found(*args, **kwargs):
    raise NotFound(kwargs.get('message', 'not-found'))


@mock.patch('iolib.bigquery.Client')
def test_bigquery_table_manager_creates_client(m_client):
    manager = BigqueryTableManager(dataset='<dataset>', table='<table>')
    assert m_client.return_value == manager.client


@mock.patch('iolib.bigquery.Client')
def test_bigquery_table_manager_creates_client_from_service_account(m_client):
    manager = BigqueryTableManager(dataset='<dataset>',
                                   table='<table>',
                                   service_account_json='<service_account_json>')
    m_client.from_service_account_json.assert_called_once_with('<service_account_json>')
    assert m_client.from_service_account_json.return_value == manager.client


@mock.patch('iolib.bigquery.Client')
@mock.patch('iolib.bigquery.parse_schema')
def test_bigquery_table_manager_parses_schema(m_parse_schema, _):
    manager = BigqueryTableManager(dataset='<dataset>',
                                   table='<table>',
                                   schema='<schema>')
    m_parse_schema.assert_called_once_with('<schema>')


@mock.patch('iolib.bigquery.Client')
def test_bigquery_table_manager_defines_dataset_and_table_by_parsing_table(m_client):
    m_client.return_value.project = '<project>'
    manager = BigqueryTableManager(table='<dataset>.<table>')
    assert m_client.return_value.get_dataset.return_value == manager.dataset
    assert m_client.return_value.get_table.return_value == manager.table
    assert '<project>' == manager.client.project


@mock.patch('iolib.bigquery.Client')
def test_bigquery_table_manager_defines_project_dataset_and_table_by_parsing_table(m_client):
    manager = BigqueryTableManager(table='<project>.<dataset>.<table>')
    assert m_client.return_value.get_dataset.return_value == manager.dataset
    assert m_client.return_value.get_table.return_value == manager.table
    assert '<project>' == manager.client.project


@mock.patch('iolib.bigquery.Client')
def test_bigquery_table_manager_defines_project_as_passed(m_client):
    manager = BigqueryTableManager(dataset='<dataset>',
                                   table='<table>',
                                   project='<project>')
    assert '<project>' == manager.client.project


@mock.patch('iolib.bigquery.Client')
def test_bigquery_table_manager_defines_dataset_from_str(m_client):
    manager = BigqueryTableManager(dataset='<dataset>', table='<table>')
    dataset_ref = m_client.return_value.dataset.return_value
    dataset = m_client.return_value.get_dataset.return_value
    assert dataset == manager.dataset
    m_client.return_value.dataset.assert_called_once_with('<dataset>')
    m_client.return_value.get_dataset.assert_called_once_with(dataset_ref)


@mock.patch('iolib.bigquery.Client')
def test_bigquery_table_manager_defines_dataset_from_dataset_ref(m_client):
    dataset_ref = DatasetReference('<project>', '<dataset>')
    manager = BigqueryTableManager(dataset=dataset_ref, table='<table>')
    dataset = m_client.return_value.get_dataset.return_value
    assert dataset == manager.dataset
    m_client.return_value.get_dataset.assert_called_once_with(dataset_ref)


@mock.patch('iolib.bigquery.Client')
def test_bigquery_table_manager_defines_dataset_from_dataset(m_client):
    dataset_ref = DatasetReference('<project>', '<dataset>')
    dataset = Dataset(dataset_ref)
    manager = BigqueryTableManager(dataset=dataset, table='<table>')
    assert dataset == manager.dataset


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
@mock.patch.object(BigqueryTableManager, 'client', new_callable=mock.PropertyMock())
def test_bigquery_table_manager_gets_table(m_client, _):
    manager = BigqueryTableManager()
    actual = manager._get_or_define_table('<table_ref>', None)
    assert manager.client.get_table.return_value == actual
    m_client.get_table.assert_called_once_with('<table_ref>')


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
@mock.patch('iolib.bigquery.Table')
@mock.patch.object(BigqueryTableManager, 'client', new_callable=mock.PropertyMock())
def test_bigquery_table_manager_defines_table(m_client, m_table, _):
    manager = BigqueryTableManager()
    m_client.get_table.side_effect = raise_not_found
    actual = manager._get_or_define_table('<table_ref>', '<schema>')
    assert m_table.return_value == actual
    m_table.assert_called_once_with('<table_ref>', schema='<schema>')


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
def test_bigquery_table_manager_errors_if_missing_schema_when_creating_table(_):
    manager = BigqueryTableManager()
    manager.client = mock.PropertyMock()
    manager.client.get_table.side_effect = raise_not_found
    with pytest.raises(AssertionError) as error:
        manager._get_or_define_table('<table_ref>', None)
    assert 'schema is required to create tables' == str(error.value)


@mock.patch('iolib.bigquery.Client')
@mock.patch.object(BigqueryTableManager, '_get_or_define_table')
def test_bigquery_table_manager_defines_table_from_str(m_get_or_define_table, m_client):
    manager = BigqueryTableManager(dataset='<dataset>', table='<table>')
    dataset = m_client.return_value.get_dataset.return_value
    table_ref = dataset.table.return_value
    table = m_get_or_define_table.return_value
    assert table == manager.table
    dataset.table.assert_called_once_with('<table>')
    m_get_or_define_table.assert_called_once_with(table_ref, None)


@mock.patch('iolib.bigquery.Client')
@mock.patch.object(BigqueryTableManager, '_get_or_define_table')
def test_bigquery_table_manager_defines_table_and_dataset_from_table_ref(m_get_or_define_table, m_client):
    table_ref = TableReference(DatasetReference('<project>', '<dataset>'),
                               '<table>')
    table = m_get_or_define_table.return_value
    dataset = m_client.return_value.get_dataset.return_value
    table.dataset_id = '<dataset>'
    manager = BigqueryTableManager(table=table_ref)
    assert table == manager.table
    assert dataset == manager.dataset
    m_get_or_define_table.assert_called_once_with(table_ref, None)
    dataset_ref = m_client.return_value.dataset.return_value
    m_client.return_value.dataset.assert_called_once_with('<dataset>')
    m_client.return_value.get_dataset.assert_called_once_with(dataset_ref)


@mock.patch('iolib.bigquery.Client')
def test_bigquery_table_manager_defines_table_and_dataset_from_table(m_client):
    table_ref = TableReference(DatasetReference('<project>', '<dataset>'),
                               '<table>')
    table = Table(table_ref)
    dataset = m_client.return_value.get_dataset.return_value
    manager = BigqueryTableManager(table=table)
    assert table == manager.table
    assert dataset == manager.dataset
    dataset_ref = m_client.return_value.dataset.return_value
    m_client.return_value.dataset.assert_called_once_with('<dataset>')
    m_client.return_value.get_dataset(dataset_ref)


@mock.patch('iolib.bigquery.Client')
def test_bigquery_table_manager_errors_if_invalid_table_from_parsed_table_id(m_client):
    with pytest.raises(AssertionError) as error:
        BigqueryTableManager('foo.bar.egg.bacon')
    assert 'Invalid table_id `foo.bar.egg.bacon`' == str(error.value)


@mock.patch('iolib.bigquery.Client')
def test_bigquery_table_manager_errors_if_invalid_dataset(m_client):
    with pytest.raises(AssertionError) as error:
        BigqueryTableManager(dataset=1, table='<table>')
    assert 'Invalid dataset `1`' == str(error.value)


@mock.patch('iolib.bigquery.Client')
def test_bigquery_table_manager_errors_if_missing_dataset(m_client):
    with pytest.raises(AssertionError) as error:
        BigqueryTableManager(table='<table>')
    assert 'Dataset required if table provided' == str(error.value)


@mock.patch('iolib.bigquery.Client')
def test_bigquery_table_manager_errors_if_invalid_table(m_client):
    with pytest.raises(AssertionError) as error:
        BigqueryTableManager(table=1, dataset='<dataset>')
    assert 'Invalid table `1`' == str(error.value)


@mock.patch('iolib.bigquery.Client')
def test_bigquery_table_manager_is_created_with_no_table(m_client):
    manager = BigqueryTableManager()
    assert manager.table is None


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
@mock.patch.object(BigqueryTableManager, 'client', new_callable=mock.PropertyMock())
def test_bigquery_table_manager_raise_table_not_found(m_client, _):
    manager = BigqueryTableManager()
    manager.dataset = mock.PropertyMock()
    manager.dataset.dataset_id = '<dataset>'
    manager.table = mock.PropertyMock()
    manager.table.table_id = '<table>'
    manager.client.project = '<project>'
    with pytest.raises(NotFound) as error:
        manager._raise_table_not_found()
    message = 'Not found: Table <project>:<dataset>.<table>'
    assert message == error.value.message
    assert [{'message': message, 'domain': 'global', 'reason': 'notFound'}] == error.value.errors


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
@mock.patch.object(BigqueryTableManager, 'client', new_callable=mock.PropertyMock())
def test_bigquery_table_manager_reads_with_query(m_client, _):
    manager = BigqueryTableManager()
    manager.dataset = mock.PropertyMock()
    manager.dataset.dataset_id = '<dataset>'
    manager.table = mock.PropertyMock()
    manager.table.table_id = '<table>'
    manager.client.project = '<project>'
    actual = manager.read(query='SELECT foo FROM `{table_id}`')
    m_client.query.assert_called_once_with(
        'SELECT foo FROM `<project>.<dataset>.<table>`')
    assert actual == m_client.query.return_value.to_dataframe.return_value.replace.return_value


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
@mock.patch.object(BigqueryTableManager, 'client', new_callable=mock.PropertyMock())
def test_bigquery_table_manager_reads_without_query(m_client, _):
    manager = BigqueryTableManager()
    manager.dataset = mock.PropertyMock()
    manager.dataset.dataset_id = '<dataset>'
    manager.table = mock.PropertyMock()
    manager.table.table_id = '<table>'
    m_client.project = '<project>'
    actual = manager.read()
    m_client.query.assert_called_once_with('SELECT * FROM `<project>.<dataset>.<table>`')
    assert actual == m_client.query.return_value.to_dataframe.return_value.replace.return_value


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
@mock.patch.object(BigqueryTableManager, 'client', new_callable=mock.PropertyMock())
def test_bigquery_table_manager_replaces_none_by_nan_when_reading(m_client, _):
    manager = BigqueryTableManager()
    manager.dataset = mock.PropertyMock()
    manager.dataset.dataset_id = '<dataset>'
    manager.table = mock.PropertyMock()
    manager.table.table_id = '<table>'
    m_client.project = '<project>'
    m_client.query.return_value.to_dataframe.return_value = pd.DataFrame([{'x': None}])
    actual = manager.read(query='SELECT foo FROM bar')
    expected = pd.DataFrame([{'x': np.nan}])
    pd.testing.assert_frame_equal(expected, actual)


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
@mock.patch.object(BigqueryTableManager, '_raise_table_not_found', side_effect=raise_not_found)
def test_bigquery_table_manager_errors_when_reading_from_a_missing_table(m_raise_table_not_found, _):
    manager = BigqueryTableManager()
    manager.table = mock.PropertyMock()
    manager.table.created = None
    with pytest.raises(NotFound) as error:
        manager.read(query=mock.ANY)
    m_raise_table_not_found.assert_called_once()


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
@mock.patch.object(BigqueryTableManager, 'client', new_callable=mock.PropertyMock())
def test_bigquery_table_manager_read_from_query(m_client, _):
    manager = BigqueryTableManager()
    actual = manager.read(query='SELECT foo FROM `table`')
    m_client.query.assert_called_once_with('SELECT foo FROM `table`')
    assert actual == m_client.query.return_value.to_dataframe.return_value.replace.return_value


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
def test_bigquery_table_manager_errors_when_reading_with_no_query_and_no_table(_):
    manager = BigqueryTableManager()
    with pytest.raises(AssertionError) as error:
        manager.read()
    assert 'query is required when reading when no table is passed' == str(error.value)


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
def test_bigquery_table_manager_errors_if_invalid_if_exists_when_writing(_):
    manager = BigqueryTableManager()
    with pytest.raises(AssertionError) as error:
        manager.write(mock.ANY, if_exists='other')
    assert 'Invalid if_exists `other`' == str(error.value)


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
def test_bigquery_table_manager_errors_if_table_exists_when_writing(_):
    manager = BigqueryTableManager()
    manager.table = mock.PropertyMock()
    manager.table.created = True
    with pytest.raises(ValueError) as error:
        manager.write(mock.ANY, if_exists='fail')
    message = 'Table already exists. Use `if_exists="replace"` or '\
              '`if_exists="append"` if you want to modify the table.`'
    assert message == str(error.value)


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
@mock.patch.object(BigqueryTableManager, 'client', new_callable=mock.PropertyMock())
def test_bigquery_table_manager_creates_table_if_exists_is_fail_when_writing(m_client, _):
    manager = BigqueryTableManager()
    table = mock.PropertyMock()
    manager.table = table
    manager.table.created = False
    manager.write([], if_exists='fail')
    m_client.create_table.assert_called_once_with(table)


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
@mock.patch.object(BigqueryTableManager, 'client', new_callable=mock.PropertyMock())
def test_bigquery_table_manager_replaces_table_if_exists_is_replace_when_writing(m_client, _):
    manager = BigqueryTableManager()
    table = mock.PropertyMock()
    manager.table = table
    manager.table.crated = True
    data = [(1,), (2,), (3,)]
    m_client.insert_rows.return_value = None
    manager.write(data, if_exists='replace')
    new_table = m_client.create_table.return_value
    m_client.delete_table.assert_called_once_with(table.reference)
    m_client.create_table.assert_called_once_with(Table(manager.table.reference, schema=manager.table.schema))
    m_client.insert_rows.assert_called_once_with(new_table, data)


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
@mock.patch.object(BigqueryTableManager, 'client', new_callable=mock.PropertyMock())
def test_bigquery_table_manager_creates_table_if_exists_is_replace_when_writing(m_client, _):
    manager = BigqueryTableManager()
    table = mock.PropertyMock()
    manager.table = table
    manager.table.created = False
    manager.write([], if_exists='replace')
    m_client.create_table.assert_called_once_with(table)


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
@mock.patch.object(BigqueryTableManager, 'client', new_callable=mock.PropertyMock())
def test_bigquery_table_manager_creates_table_if_exists_is_append_when_writing(m_client, _):
    manager = BigqueryTableManager()
    table = mock.PropertyMock()
    manager.table = table
    manager.table.created = False
    manager.write([], if_exists='append')
    m_client.create_table.assert_called_once_with(table)


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
@mock.patch.object(BigqueryTableManager, 'client', new_callable=mock.PropertyMock())
def test_bigquery_table_manager_does_not_create_table_if_exists_is_append_when_writing(m_client, _):
    manager = BigqueryTableManager()
    table = mock.PropertyMock()
    manager.table = table
    manager.table.created = True
    manager.write([], if_exists='append')
    m_client.create_table.assert_not_called()


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
@mock.patch.object(BigqueryTableManager, 'client', new_callable=mock.PropertyMock())
def test_bigquery_table_manager_ireads_with_query(m_client, _):
    manager = BigqueryTableManager()
    manager.dataset = mock.PropertyMock()
    manager.dataset.dataset_id = '<dataset>'
    manager.table = mock.PropertyMock
    manager.table.table_id = '<table>'
    manager.table.created = True
    manager.client.project = '<project>'
    iterable = manager.iread(query='SELECT foo FROM `{table_id}`')
    list(iterable)
    m_client.query.assert_called_once_with(
        'SELECT foo FROM `<project>.<dataset>.<table>`')


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
@mock.patch.object(BigqueryTableManager, 'client', new_callable=mock.PropertyMock())
def test_bigquery_table_manager_ireads_without_query(m_client, _):
    manager = BigqueryTableManager()
    manager.dataset = mock.PropertyMock()
    manager.dataset.dataset_id = '<dataset>'
    manager.table = mock.PropertyMock()
    manager.table.table_id = '<table>'
    m_client.project = '<project>'
    iterable = manager.iread()
    list(iterable)
    m_client.query.assert_called_once_with(
        'SELECT * FROM `<project>.<dataset>.<table>`')


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
@mock.patch.object(BigqueryTableManager, 'client', new_callable=mock.PropertyMock())
def test_bigquery_table_manager_do_not_cast_when_ireading(m_client, _):
    manager = BigqueryTableManager()
    manager.dataset = mock.PropertyMock()
    manager.dataset.dataset_id = '<dataset>'
    manager.table = mock.PropertyMock()
    manager.table.table_id = '<table>'
    m_client.project = '<project>'
    row = '<row>'
    m_client.query.return_value.result.return_value = (i for i in [row])
    iterable = manager.iread(query='SELECT foo FROM bar')
    assert '<row>' == next(iterable)


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
@mock.patch.object(BigqueryTableManager, 'client', new_callable=mock.PropertyMock())
def test_bigquery_table_manager_casts_to_dict_when_ireading(m_client, _):
    manager = BigqueryTableManager()
    manager.dataset = mock.PropertyMock()
    manager.dataset.dataset_id = '<dataset>'
    manager.table = mock.PropertyMock()
    manager.table.table_id = '<table>'
    m_client.project = '<project>'
    row = mock.Mock()
    row.items.return_value = [('<key>', '<value>')]
    m_client.query.return_value.result.return_value = (i for i in [row])
    iterable = manager.iread(query='SELECT foo FROM bar', astype=dict)
    actual = next(iterable)
    expected = {'<key>': '<value>'}
    assert expected == actual


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
@mock.patch.object(BigqueryTableManager, 'client', new_callable=mock.PropertyMock())
def test_bigquery_table_manager_casts_to_series_when_ireading(m_client, _):
    manager = BigqueryTableManager()
    manager.dataset = mock.PropertyMock()
    manager.dataset.dataset_id = '<dataset>'
    manager.table = mock.PropertyMock()
    manager.table.table_id = '<table>'
    m_client.project = '<project>'
    row = mock.Mock()
    row.items.return_value = [('<key>', '<value>')]
    m_client.query.return_value.result.return_value = (i for i in [row])
    iterable = manager.iread(query='SELECT foo FROM bar', astype=pd.Series)
    actual = next(iterable)
    expected = pd.Series({'<key>': '<value>'})
    pd.testing.assert_series_equal(expected, actual)


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
@mock.patch.object(BigqueryTableManager, 'client', new_callable=mock.PropertyMock())
def test_bigquery_table_manager_casts_to_list_when_ireading(m_client, _):
    manager = BigqueryTableManager()
    manager.dataset = mock.PropertyMock()
    manager.dataset.dataset_id = '<dataset>'
    manager.table = mock.PropertyMock()
    manager.table.table_id = '<table>'
    m_client.project = '<project>'
    row = mock.Mock()
    row.values.return_value = [1, 2, 3]
    m_client.query.return_value.result.return_value = (i for i in [row])
    iterable = manager.iread(query='SELECT foo FROM bar', astype=list)
    actual = next(iterable)
    expected = [1, 2, 3]
    assert expected == actual


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
@mock.patch.object(BigqueryTableManager, 'client', new_callable=mock.PropertyMock())
def test_bigquery_table_manager_casts_to_tuple_when_ireading(m_client, _):
    manager = BigqueryTableManager()
    manager.dataset = mock.PropertyMock()
    manager.dataset.dataset_id = '<dataset>'
    manager.table = mock.PropertyMock()
    manager.table.table_id = '<table>'
    m_client.project = '<project>'
    row = mock.Mock()
    row.values.return_value = [1, 2, 3]
    m_client.query.return_value.result.return_value = (i for i in [row])
    iterable = manager.iread(query='SELECT foo FROM bar', astype=tuple)
    actual = next(iterable)
    expected = (1, 2, 3)
    assert expected == actual


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
@mock.patch.object(BigqueryTableManager, 'client', new_callable=mock.PropertyMock())
def test_bigquery_table_manager_casts_to_custom_type_when_ireading(m_client, _):
    manager = BigqueryTableManager()
    manager.dataset = mock.PropertyMock()
    manager.dataset.dataset_id = '<dataset>'
    manager.table = mock.PropertyMock()
    manager.table.table_id = '<table>'
    m_client.project = '<project>'
    row = [1, 2, 3]
    custom_type = mock.Mock()
    m_client.query.return_value.result.return_value = (i for i in [row])
    iterable = manager.iread(query='SELECT foo FROM bar', astype=custom_type)
    actual = next(iterable)
    expected = custom_type.return_value
    assert expected == actual
    custom_type.assert_called_once_with([1, 2, 3])


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
def test_bigquery_table_manager_errors_when_ireading_with_no_query_and_no_table(_):
    manager = BigqueryTableManager()
    iterable = manager.iread()
    with pytest.raises(AssertionError) as error:
        next(iterable)
    assert 'query is required when ireading when no table is passed' == str(error.value)


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
@mock.patch.object(BigqueryTableManager, '_raise_table_not_found', side_effect=raise_not_found)
def test_bigquery_table_manager_errors_when_ireading_from_a_missing_table(m_raise_table_not_found, _):
    manager = BigqueryTableManager()
    manager.table = mock.PropertyMock()
    manager.table.created = None
    iterable = manager.iread(query=mock.ANY)
    with pytest.raises(NotFound) as error:
        next(iterable)
    m_raise_table_not_found.assert_called_once()


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
@mock.patch.object(BigqueryTableManager, 'client', new_callable=mock.PropertyMock())
def test_bigquery_table_manager_writes_from_list(m_client, _):
    manager = BigqueryTableManager()
    manager.table = mock.PropertyMock()
    data = [(1,), (2,), (3,)]
    m_client.insert_rows.return_value = None
    manager.write(data, if_exists='append')
    m_client.insert_rows.assert_called_once_with(manager.table, data)


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
@mock.patch.object(BigqueryTableManager, 'client', new_callable=mock.PropertyMock())
def test_bigquery_table_manager_writes_from_dataframe(m_client, _):

    class DummyColumn:

        def __init__(self, name):
            self.name = name

    manager = BigqueryTableManager()
    manager.table = mock.PropertyMock()
    manager.table.schema = [DummyColumn('a')]
    data = pd.DataFrame([{'a': 'x', 'b': 1}, {'a': np.nan, 'b': 2}])
    m_client.insert_rows_from_dataframe.return_value = None
    manager.write(data, if_exists='append')
    m_client.insert_rows_from_dataframe.assert_called_once()
    args, kwargs = m_client.insert_rows_from_dataframe.call_args
    assert manager.table == args[0]
    pd.testing.assert_frame_equal(pd.DataFrame([{'a': 'x'}, {'a': np.nan}]), args[1])
    assert {'chunk_size': 1000} == kwargs


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
@mock.patch.object(BigqueryTableManager, 'client', new_callable=mock.PropertyMock())
def test_bigquery_table_manager_errors_if_insert_rows_from_dataframe_errors_when_writing(m_client, _):
    manager = BigqueryTableManager()
    manager.table = mock.PropertyMock()
    m_client.insert_rows_from_dataframe.return_value = 'An error from Bigquery'
    with pytest.raises(Exception) as error:
        manager.write(pd.DataFrame([{'x': 1}]), if_exists='append')
    assert 'An error from Bigquery' == str(error.value)


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
@mock.patch.object(BigqueryTableManager, 'client', new_callable=mock.PropertyMock())
def test_bigquery_table_manager_writes_in_batches(m_client, _):
    manager = BigqueryTableManager()
    manager.table = mock.PropertyMock()
    data = [(1,), (2,), (3,), (4,), (5,)]
    m_client.insert_rows.return_value = None
    manager.write(data, chunk_size=3, if_exists='append')
    calls = [mock.call(manager.table, [(1,), (2,), (3,)]),
             mock.call(manager.table, [(4,), (5,)])]
    assert calls == m_client.insert_rows.call_args_list


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
@mock.patch.object(BigqueryTableManager, 'client', new_callable=mock.PropertyMock())
def test_bigquery_table_manager_errors_if_insert_rows_errors_when_writing(m_client, _):
    manager = BigqueryTableManager()
    manager.table = mock.PropertyMock()
    m_client.insert_rows.return_value = 'An error from Bigquery'
    with pytest.raises(Exception) as error:
        manager.write([(1,)], if_exists='append')
    assert 'An error from Bigquery' == str(error.value)


@mock.patch.object(BigqueryTableManager, '__init__', return_value=None)
def test_bigquery_table_manager_errors_when_writing_with_no_table(_):
    manager = BigqueryTableManager()
    with pytest.raises(AssertionError) as error:
        manager.write(mock.ANY)
    assert 'table is required to write' == str(error.value)


@mock.patch('iolib.bigquery.BigqueryTableManager')
def test_read_bigquery_uses_manager(m_manager):
    actual = read_bigquery(table='<table>', query='<query>')
    m_manager.assert_called_once_with(table='<table>')
    m_manager.return_value.read.assert_called_once_with(query='<query>')


@mock.patch('iolib.bigquery.BigqueryTableManager')
def test_iread_bigquery_uses_manager(m_manager):
    actual = iread_bigquery(table_id='<table_id>', query='<query>')
    m_manager.assert_called_once_with(table_id='<table_id>')
    m_manager.return_value.iread.assert_called_once_with(query='<query>')


@mock.patch('iolib.bigquery.BigqueryTableManager')
def test_write_bigquery_uses_manager(m_manager):
    actual = write_bigquery(table='<table>', data='<data>')
    m_manager.assert_called_once_with(table='<table>')
    m_manager.return_value.write.assert_called_once_with(data='<data>')


@pytest.mark.parametrize(('schema', 'expected'), (
    ([], []),
    ([SchemaField(name='name', description='The name', field_type='STRING', mode='NULLABLE')],
     [SchemaField(name='name', description='The name', field_type='STRING', mode='NULLABLE')]),
    ([{'name': 'list', 'description': 'The list', 'type': 'STRING', 'mode': 'REPEATED'}],
     [SchemaField(name='list', description='The list', field_type='STRING', mode='REPEATED')]),
))
def test_parse_schema(schema, expected):
    assert expected == parse_schema(schema)
