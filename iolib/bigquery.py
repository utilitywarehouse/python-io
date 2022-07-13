from google.cloud.bigquery import Client, Table
import numpy as np
import pandas as pd


class BigqueryTableManager:
    client = None
    dataset = None
    table = None

    def __init__(self,
                 table_id=None,
                 project=None,
                 dataset=None,
                 table=None,
                 service_account_json=None):
        if service_account_json:
            self.client = Client.from_service_account_json(
                service_account_json)
        else:
            self.client = Client()

        # Extract components from table_id.
        if table_id:
            splits = table_id.split('.')
            n_splits = len(splits)
            assert n_splits in (2, 3), f'Invalid table_id `{table_id}`'
            if n_splits == 2:
                dataset, table = splits
            else:
                project, dataset, table = splits

        assert dataset and table, \
            'dataset and table are required, either passed directly or as '\
            'part of table_id'

        self.dataset = dataset
        self.table = table

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
        table_id = f'{self.client.project}.{self.dataset}.{self.table}'
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
        table_ref = self.client.dataset(self.dataset).table(self.table)
        table = self.client.get_table(table_ref)

        # Get table schema, delete table and recreate it if required.
        if replace:
            schema = table.schema
            self.client.delete_table(table_ref)
            table = self.client.create_table(Table(table_ref, schema=schema))

        # Write dataframe.
        if isinstance(data, pd.DataFrame):
            cols = [i.name for i in table.schema]
            data = data[cols].replace({np.nan: None})
            errors = self.client.insert_rows_from_dataframe(
                table,
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

                errors = self.client.insert_rows(table, rows)
                if errors:
                    raise Exception(errors)


def read_bigquery(**kwargs):
    """
    Read BigQuery table as a DataFrame.

    Parameters
    ----------
    table_id : str
        BigQuery table id. Required if no dataset and table set.
    project : str
        BigQuery project. Required if table_id doesn't contain the project or
        if the project is different than the one set in the service account.
    dataset : str
        BigQuery dataset. Required if table_id not set.
    table : str
        BigQuery table. Required if table_id not set.
    service_account_json : str
        Path to Google Cloud service account JSON file. Default taken from
        environment variable GOOGLE_APPLICATION_CREDENTIALS.
    query : str
        BigQuery SQL query. Required if not all the rows and columns are
        required.

    Examples
    --------
    >>> read_bigquery(table_id='my-dataset.my-table')
    [...]
    >>> read_bigquery(table_id='my-project.my-dataset.my-table')
    [...]
    >>> read_bigquery(table_id='my-dataset.my-table',
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
    init_kwarg_keys = ('table_id',
                       'project',
                       'dataset',
                       'table',
                       'service_account_json')
    read_kwargs_keys = ('query',)
    init_kwargs = {k: v for k, v in kwargs.items() if k in init_kwarg_keys}
    read_kwargs = {k: v for k, v in kwargs.items() if k in read_kwargs_keys}
    return BigqueryTableManager(**init_kwargs).read(**read_kwargs)
