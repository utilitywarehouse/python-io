from google.cloud.bigquery import (
    Client,
    Dataset,
    DatasetReference,
    Table,
    TableReference,
    SchemaField,
)
from google.api_core.exceptions import NotFound
import numpy as np
import pandas as pd


def parse_schema(schema):  # wiki: ignore
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
            google.cloud.bigquery.TablesetReference, optional
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
    schema : list of dicts or google.cloud.bigquery.SchemaField, optional
        Table schema required only when creating tables.
    service_account_json : str, optional
        Path of the service account file. If not passed, it will be taken from
        the environment (GOOGLE_APPLICATION_CREDENTIALS).
    """
    client = None
    dataset = None
    table = None

    def __init__(self,
                 table=None,
                 dataset=None,
                 project=None,
                 schema=None,
                 service_account_json=None):
        if service_account_json:
            self.client = Client.from_service_account_json(
                service_account_json)
        else:
            self.client = Client()

        if schema:
            schema = parse_schema(schema)

        # Extract components from table_id.
        if isinstance(table, str) and '.'  in table:
            splits = table.split('.')
            n_splits = len(splits)
            assert n_splits in (2, 3), f'Invalid table_id `{table}`'
            if n_splits == 2:
                dataset, table = splits
            else:
                project, dataset, table = splits

        # Set different project than the default (the project where the
        # credentials were created from).
        if project:
            self.client.project = project

        # Get table and dataset_id if table is a bigquery object.
        if isinstance(table, Table):
            self.table = table
            dataset = self.table.dataset_id
        elif isinstance(table, TableReference):
            self.table = self._get_or_define_table(table, schema)
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
            if table is not None:
                raise AssertionError('Dataset required if table provided')
        else:
            raise AssertionError(f'Invalid dataset `{dataset}`')

        # Get table if table is a string.
        if isinstance(table, str):
            table_ref = self.dataset.table(table)
            self.table = self._get_or_define_table(table_ref, schema)
        elif table and not isinstance(table, (Table, TableReference)):
            raise AssertionError(f'Invalid table `{table}`')

        # Table can be None but only read will be permitted in that case.

    def _get_or_define_table(self, table_ref, schema):
        try:
            return self.client.get_table(table_ref)
        except NotFound:
            if not schema:
                raise AssertionError('schema is required to create tables')
            return Table(table_ref, schema=schema)

    def _raise_table_not_found(self):
        message = (f'Not found: Table {self.client.project}:'
                   f'{self.dataset.dataset_id}.{self.table.table_id}')
        error = {'message': message,
                 'domain': 'global',
                 'reason': 'notFound'}
        raise NotFound(message, errors=[error])

    @property
    def _table_id(self):
        return '.'.join([self.client.project,
                         self.dataset.dataset_id,
                         self.table.table_id])

    def read(self, query=None):
        """
        Read BigQuery table as a dataframe. Pass a BQ SQL query to be executed
        or nothing to read the whole table.

        Parameters
        ----------
        query : str, optional
            BigQuery SQL query. Required if table is None or if not all the
            rows and columns are required. The variable `table_id` can be used
            in the query.

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
        >>> manager = BigqueryTableManager()
        >>> manager.read("SELECT foo FROM `project.dataset.table`")
        [...]
        """
        assert self.table or query,\
            'query is required when reading when no table is passed'

        if self.table:
            if not self.table.created:
                self._raise_table_not_found()
            query = query or 'SELECT * FROM `{table_id}`'
            query = query.format(table_id=self._table_id)

        return self.client.query(query).to_dataframe().replace({None: np.nan})

    def iread(self, query=None, astype=None):
        """
        Read BigQuery table as an iterable. Pass a BQ SQL query to be executed
        or nothing to read the whole table.

        Parameters
        ----------
        query : str, optional
            BigQuery SQL query. Required if table is None or if not all the
            rows and columns are required. The variable `table_id` can be used
            in the query.
        astype : type, class, function
            Iterable row type. By default is None, which yields
            google.cloud.bigquery.table.Row. Examples: pd.Series, dict, list, a
            custom Model...

        Returns
        -------
        iterator : iter
            Iterable of rows with type defined by `astype`.

        Examples
        --------
        >>> manager = BigqueryTableManager(project='p', dataset='d', table='t')
        >>> iterator = manager.iread()
        >>> next(iterator)
        Row([...])
        >>> manager.read("SELECT foo FROM `{table_id}`")
        >>> next(iterator)
        {'foo': [...]}
        """
        assert self.table or query,\
            'query is required when ireading when no table is passed'

        if self.table:
            if not self.table.created:
                self._raise_table_not_found()
            query = query or 'SELECT * FROM `{table_id}`'
            query = query.format(table_id=self._table_id)

        for row in self.client.query(query).result():
            if astype is None:
                yield row
            elif astype == dict:
                yield dict(row.items())
            elif astype == pd.Series:
                yield pd.Series(dict(row.items()))
            elif hasattr(astype, '__iter__'):
                yield astype(row.values())
            else:
                yield astype(row)


    def write(self, data, if_exists='fail', chunk_size=1000):
        """
        Write data into the BigQuery table.

        Parameters
        ----------
        data : pandas.DataFrame or indexable iterable
            Data to be stored in the table.
        if_exists : str = 'fail'
            Behavior when the destination table exists. Value can be one of:
            'fail'
                If table exists, raise ValueError.
            'replace'
                If table exists, delete it, recreate it, and insert data.
            'append'
                If table exists, insert data. Create if does not exist.
        chunk_size : int = 1000
            The number of rows to stream in a single chunk.

        Raises
        ------
            Exception: if an error is returned when writing rows from client.
        """

        assert if_exists in ('fail', 'append', 'replace'), \
            f'Invalid if_exists `{if_exists}`'

        assert self.table, 'table is required to write'

        if if_exists == 'fail':
            if self.table.created:
                raise ValueError(
                    'Table already exists. Use `if_exists="replace"` or '
                    '`if_exists="append"` if you want to modify the table.`'
                )
            self.table = self.client.create_table(self.table)

        elif if_exists == 'replace':
            if self.table.created:
                schema = self.table.schema
                table_ref = self.table.reference
                self.client.delete_table(table_ref)
                table = Table(table_ref, schema=schema)
                self.table = self.client.create_table(table)
            else:
                self.table = self.client.create_table(self.table)

        elif if_exists == 'append' and not self.table.created:
            self.table = self.client.create_table(self.table)

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
            google.cloud.bigquery.TablesetReference, option
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
        BigQuery SQL query. Required if table is not passed or if not all the
        rows and columns are required. The variable `table_id` can be used in
        the query.

    Examples
    --------
    >>> read_bigquery(table='my-dataset.my-table')
    [...]
    >>> read_bigquery(table='my-project.my-dataset.my-table')
    [...]
    >>> read_bigquery(query='SELECT foo FROM `my-project.my-dataset.my-table`')
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


def iread_bigquery(**kwargs):
    """
    Read BigQuery table as an iterable.

    Parameters
    ----------
    table : str or google.cloud.bigquery.Table or
            google.cloud.bigquery.TablesetReference, option
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
        BigQuery SQL query. Required if table is not passed or if not all the
        rows and columns are required. The variable `table_id` can be used in
        the query.
    astype : type, class, function
        Iterable row type. By default, it yields
        google.cloud.bigquery.table.Row. Examples: pd.Series, dict, list, a
        custom Model...

    Examples
    --------
    >>> iterator = iread_bigquery(table_id='my-dataset.my-table')
    >>> next(iterator)
    Row([...])
    >>> iterator = iread_bigquery(table_id='my-project.my-dataset.my-table',
    ...                           query='SELECT foo FROM `{table_id}`',
    ...                           astype=dict)
    {'foo': [...]}
    >>> iterator = iread_bigquery(query='SELECT foo FROM `dataset.table`')
    [[...]]

    See also
    --------
    iolib.BigqueryTableManager.iread
    """
    init_kwarg_keys = ('table_id',
                       'project',
                       'dataset',
                       'table',
                       'service_account_json')
    iread_kwargs_keys = ('query', 'astype')
    init_kwargs = {k: v for k, v in kwargs.items() if k in init_kwarg_keys}
    iread_kwargs = {k: v for k, v in kwargs.items() if k in iread_kwargs_keys}
    return BigqueryTableManager(**init_kwargs).iread(**iread_kwargs)


def write_bigquery(**kwargs):
    """
    Write into BigQuery table from a DataFrame or indexable iterable.

    Parameters
    ----------
    table : str or google.cloud.bigquery.Table or
            google.cloud.bigquery.TablesetReference, option
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
    schema : list of dicts or google.cloud.bigquery.SchemaField, optional
        Table schema required only when creating tables.
    if_exists : str = 'fail'
        Behavior when the destination table exists. Value can be one of:
        'fail'
            If table exists, raise google.api_core.exceptions.NotFound.
        'replace'
            If table exists, delete it, recreate it, and insert data.
        'append'
            If table exists, insert data. Create if does not exist.
    chunk_size : int
        The number of rows to stream in a single chunk. 1000, by default.

    Examples
    --------
    >>> write_bigquery(table='my-project.my-dataset.my-table',
    ...                data=df,
    ...                schema=schema)
    [...]
    >>> write_bigquery(table='my-dataset.my-table',
    ...                data=rows,
    ...                if_exists='replace')
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
    init_kwarg_keys = ('project',
                       'dataset',
                       'table',
                       'schema',
                       'service_account_json')
    write_kwargs_keys = ('data', 'if_exists', 'chunk_size')
    init_kwargs = {k: v for k, v in kwargs.items() if k in init_kwarg_keys}
    write_kwargs = {k: v for k, v in kwargs.items() if k in write_kwargs_keys}
    return BigqueryTableManager(**init_kwargs).write(**write_kwargs)
