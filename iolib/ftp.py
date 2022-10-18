from ftplib import FTP, FTP_TLS
from io import BytesIO

import pandas as pd


def connect(host,
            user=None,
            password=None,
            acct=None,
            timeout=None,
            source_address=None,
            encoding='utf-8',
            context=None,
            tls=False):
    """
    Create an ftplib.FTP instance if `tls` is false or an ftplib.FTP_TLS if tls
    is true. Argument context will be passed to the ftp instance when tls is
    true. For consistency accross iolib, `password` is used instead of ftplib's
    `passwd`.

    See more
    --------
    https://docs.python.org/3/library/ftplib.html
    """
    kwargs = {'host': host,
              'user': user or '',
              'passwd': password or '',
              'acct': acct or '',
              'timeout': timeout,
              'source_address': source_address,
              'encoding': encoding}
    if tls:
        client_class = FTP_TLS
        kwargs['context'] = context
    else:
        client_class = FTP
    return client_class(**kwargs)


def list_ftp(host,
             path='/',
             user=None,
             password=None,
             acct=None,
             timeout=None,
             source_address=None,
             encoding='utf-8',
             context=None,
             tls=False):
    """
    List files from an FTP server.

    Parameters
    ----------
    host : str
        Host to pass to FTP python client.
    path : str, default='/'
        Directoty to list.
    user : str, optional
        User to pass to FTP python client,
    password : str, optional
        Password to pass to FTP python client as "passwd".
    acct : str, optional
        Accounting information to pass to FTP python client.
    timeout : int, optional
        Timeout in seconds to pass to FTP python client.
    source_address : str, optional
        Source IP address to pass to FTP python client.
    encoding : str, default='utf-8'
        Encoding to pass to FTP python client.
    context : ssl.Context, optional
        SSL Context to pass to FTP python client.
    tls : bool, default=False
        Whether to use TLS or not.

    Returns
    -------
    df : pandas.DataFrame(name)

    See more
    --------
    https://docs.python.org/3/library/ftplib.html
    """
    ftp = connect(host,
                  user=user,
                  password=password,
                  acct=acct,
                  timeout=timeout,
                  source_address=source_address,
                  encoding=encoding,
                  context=context,
                  tls=tls)
    if not path.endswith('/'):
        path = f'{path}/'
    result = ftp.nlst(path)
    ftp.quit()
    return pd.DataFrame([i.replace(path, '', 1) for i in result],
                        columns=['name'])


def read_ftp(host,
             path,
             user=None,
             password=None,
             acct=None,
             timeout=None,
             source_address=None,
             encoding='utf-8',
             context=None,
             tls=False,
             **kwargs):
    """
    Read file from an FTP server. If the file is a CSV, it will be read as a
    pandas.DataFrame. Other formats not supported yet.

    Parameters
    ----------
    host : str
        Host to pass to FTP python client.
    path : str, default='/'
        Directoty to list.
    user : str, optional
        User to pass to FTP python client,
    password : str, optional
        Password to pass to FTP python client as "passwd".
    acct : str, optional
        Accounting information to pass to FTP python client.
    timeout : int, optional
        Timeout in seconds to pass to FTP python client.
    source_address : str, optional
        Source IP address to pass to FTP python client.
    encoding : str, default='utf-8'
        Encoding to pass to FTP python client.
    context : ssl.Context, optional
        SSL Context to pass to FTP python client.
    tls : bool, default=False
        Whether to use TLS or not.
    **kwargs : kwargs
        Extra parameters to pass to the file reader.

    Returns
    -------
    result : pandas.DataFrame(name)

    See more
    --------
    https://docs.python.org/3/library/ftplib.html

    TODOs
    -----
      - Support for other file formats.
    """
    ftp = connect(host,
                  user=user,
                  password=password,
                  acct=acct,
                  timeout=timeout,
                  source_address=source_address,
                  encoding=encoding,
                  context=context,
                  tls=tls)
    file = BytesIO()
    ftp.retrbinary(f'RETR {path}', callback=file.write)
    ftp.quit()
    file.seek(0)
    ext = path.rsplit('.', 1)[-1].lower()
    if ext != 'csv':
        raise Exception('Unsupported file format')
    return pd.read_csv(file, **kwargs)
