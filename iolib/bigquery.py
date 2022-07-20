from google.cloud.bigquery import (
    Client,
    Dataset,
    DatasetReference,
    Table,
    TableReference,
    SchemaField,
)
import numpy as np
import pandas as pd


def parse_schema(schema):
    """
    Create a list of google.cloud.bigquery.SchemaField from a list of dicts.
    """
    if not schema or isinstance(schema[0], SchemaField):
        return schema
    key_map = {'type': 'field_type'}
    parsed = []
    for field in schema:
        kwargs = {key_map.get(k, k): v for k, v in field.items()}
        parsed.append(SchemaField(**kwargs))
    return parsed


class BigqueryTableManager:
    """
    Class to manage Bigquery table reads and writes.

    Parameters
    ----------
    table : str or google.cloud.bigquery.Table or
            google.cloud.bigquery.TablesetReference
        Bigquery table. If passed as string, this can be the table id with
        optional dataset and project (e.g. 'table', 'dataset.table',
        'project.dataset.table').
    dataset : str or google.cloud.bigquery.Dataset or
              google.cloud.bigquery.DatasetReference, optional
        Dataset containing the table. If not passed, it will be created from
        `table`.
    project : str, optional
        Project id where the table is located. If not passed, it will be
        created from `table` or Bigquery client.
    service_account_json : str, optional
        Path of the service account file. If not passed, it will be taken from
        the environment (GOOGLE_APPLICATION_CREDENTIALS).
    """
    client = None
    dataset = None
    table = None

    def __init__(self,
                 table,
                 dataset=None,
                 project=None,
                 service_account_json=None):
        if service_account_json:
            self.client = Client.from_service_account_json(
                service_account_json)
        else:
            self.client = Client()

        # Extract components from table_id.
        if isinstance(table, str) and '.'  in table:
            splits = table.split('.')
            n_splits = len(splits)
            assert n_splits in (2, 3), f'Invalid table_id `{table}`'
            if n_splits == 2:
                dataset, table = splits
            else:
                project, dataset, table = splits

        # Get table and dataset_id if table is a bigquery object.
        if isinstance(table, Table):
            self.table = table
            dataset = self.table.dataset_id
        elif isinstance(table, TableReference):
            self.table = self.client.get_table(table)
            dataset = self.table.dataset_id

        # Get dataset.
        if isinstance(dataset, Dataset):
            self.dataset = dataset
        elif isinstance(dataset, DatasetReference):
            self.dataset = self.client.get_dataset(dataset)
        elif isinstance(dataset, str):
            dataset_ref = self.client.dataset(dataset)
            self.dataset = self.client.get_dataset(dataset_ref)
        elif dataset is None:
            raise AssertionError('Missing dataset')
        else:
            raise AssertionError(f'Invalid dataset `{dataset}`')

        # Get table if table is a string.
        if isinstance(table, str):
            table_ref = self.client.table(table)
            self.table = self.client.get_table(table_ref)
        elif table is None:
            raise AssertionError('Missing table')
        elif not isinstance(table, (Table, TableReference)):
            raise AssertionError(f'Invalid table `{table}`')

        # Set different project than the default (the project where the
        # credentials were created from).
        if project:
            self.client.project = project

    def read(self, query='SELECT * FROM `{table_id}`'):
        """
        Read BigQuery table as a dataframe. Pass a BQ SQL query to be executed
        or nothing to read the whole table.

        Parameters
        ----------
        query : str
            A BigQuery SQL query. The variable `table_id` can be used in the
            query.

        Returns
        -------
        pd : pandas.DataFrame
            The resulting DataFrame from the executed query.

        Examples
        --------
        >>> manager = BigqueryTableManager(project='p', dataset='d', table='t')
        >>> manager.read()
        [...]
        >>> manager.read("SELECT foo FROM `{table_id}`")
        [...]
        """
        table_id = '.'.join([self.client.project,
                             self.dataset.dataset_id,
                             self.table.table_id])
        query = query.format(table_id=table_id)
        return self.client.query(query).to_dataframe().replace({None: np.nan})

    def write(self, data, replace=False, chunk_size=1000):
        """
        Write data into the BigQuery table.

        Parameters
        ----------
        data : pandas.DataFrame or indexable iterable
            Data to be stored in the table.
        replace : bool = False
            Whether to replace the table or not.
        chunk_size : int = 1000
            The number of rows to stream in a single chunk.

        Raises
        ------
            Exception: if an error is returned when writing rows from client.
        """

        # Get table schema, delete table and recreate it if required.
        if replace:
            schema = self.table.schema
            table_ref = self.table.reference
            self.client.delete_table(table_ref)
            table = Table(table_ref, schema=schema)
            self.table = self.client.create_table(table)

        # Write dataframe.
        if isinstance(data, pd.DataFrame):
            cols = [i.name for i in self.table.schema]
            data = data[cols].replace({np.nan: None})
            errors = self.client.insert_rows_from_dataframe(
                self.table,
                data,
                chunk_size=chunk_size)

        # Write indexable iterable.
        else:
            index_to = 0
            while True:
                index_from = index_to
                index_to = index_from + chunk_size
                rows = data[index_from:index_to]

                if not len(rows):
                    break

                errors = self.client.insert_rows(self.table, rows)
                if errors:
                    raise Exception(errors)


def read_bigquery(**kwargs):
    """
    Read BigQuery table as a DataFrame.

    Parameters
    ----------
    table : str or google.cloud.bigquery.Table or
            google.cloud.bigquery.TablesetReference
        Bigquery table. If passed as string, this can be the table id with
        optional dataset and project (e.g. 'table', 'dataset.table',
        'project.dataset.table').
    dataset : str or google.cloud.bigquery.Dataset or
              google.cloud.bigquery.DatasetReference, optional
        Dataset containing the table. If not passed, it will be created from
        `table`.
    project : str, optional
        Project id where the table is located. If not passed, it will be
        created from `table` or Bigquery client.
    service_account_json : str, optional
        Path of the service account file. If not passed, it will be taken from
        the environment (GOOGLE_APPLICATION_CREDENTIALS).
    query : str, optional
        BigQuery SQL query. Required if not all the rows and columns are
        required.

    Examples
    --------
    >>> read_bigquery(table='my-dataset.my-table')
    [...]
    >>> read_bigquery(table='my-project.my-dataset.my-table')
    [...]
    >>> read_bigquery(table=TableReference(dataset_ref, 'my-table'))
    [...]
    >>> read_bigquery(table=Table(table_ref),
    ...               service_account_json='/path/to/service_account.json')
    [...]
    >>> read_bigquery(project='my-project',
    ...               dataset='my-dataset',
    ...               table=my-table',
    ...               query='SELECT foo FROM `{table_id}` LIMIT 100')
    [...]

    See also
    --------
    iolib.BigqueryTableManager.read
    """
    init_kwarg_keys = ('project', 'dataset', 'table', 'service_account_json')
    read_kwargs_keys = ('query',)
    init_kwargs = {k: v for k, v in kwargs.items() if k in init_kwarg_keys}
    read_kwargs = {k: v for k, v in kwargs.items() if k in read_kwargs_keys}
    return BigqueryTableManager(**init_kwargs).read(**read_kwargs)


def write_bigquery(**kwargs):
    """
    Write into BigQuery table from a DataFrame or indexable iterable.

    Parameters
    ----------
    table : str or google.cloud.bigquery.Table or
            google.cloud.bigquery.TablesetReference
        Bigquery table. If passed as string, this can be the table id with
        optional dataset and project (e.g. 'table', 'dataset.table',
        'project.dataset.table').
    dataset : str or google.cloud.bigquery.Dataset or
              google.cloud.bigquery.DatasetReference, optional
        Dataset containing the table. If not passed, it will be created from
        `table`.
    project : str, optional
        Project id where the table is located. If not passed, it will be
        created from `table` or Bigquery client.
    service_account_json : str, optional
        Path of the service account file. If not passed, it will be taken from
        the environment (GOOGLE_APPLICATION_CREDENTIALS).
    data : pandas.DataFrame or indexable iterable
        Data to be stored in the table.
    replace : bool
        Whether to replace the table or not. False, by default.
    chunk_size : int
        The number of rows to stream in a single chunk. 1000, by default.

    Examples
    --------
    >>> write_bigquery(table='my-project.my-dataset.my-table', data=df)
    [...]
    >>> write_bigquery(table='my-dataset.my-table', data=rows, replace=True)
    [...]
    >>> write_bigquery(table=TableReference(dataset_ref, 'my-table'),
    ...                data=rows,
    ...                chunk_size=500)
    [...]
    >>> write_bigquery(table=Table(table_ref),
    ...                service_account_json='/path/to/service_account.json',
    ...                data=rows)
    [...]

    See also
    --------
    iolib.BigqueryTableManager.write
    """
    init_kwarg_keys = ('project', 'dataset', 'table', 'service_account_json')
    write_kwargs_keys = ('data', 'replace', 'chunk_size')
    init_kwargs = {k: v for k, v in kwargs.items() if k in init_kwarg_keys}
    write_kwargs = {k: v for k, v in kwargs.items() if k in write_kwargs_keys}
    return BigqueryTableManager(**init_kwargs).write(**write_kwargs)
