from google.cloud.bigquery import Client
import numpy as np


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
