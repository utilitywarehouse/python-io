from unittest import mock

import pandas as pd
import pytest

from iolib import list_ftp, read_ftp, write_ftp
from iolib.ftp import connect


@mock.patch('iolib.ftp.FTP')
def test_connect(m_ftp):
    actual = connect(host='<host>',
                     user='<user>',
                     password='<password>',  # Called as passwd in ftplib.FTP.
                     acct=None,  # Called as '' in ftplib.FTP
                     timeout='<timeout>',
                     source_address='<source_address>',
                     encoding='<encoding>',
                     context='<context>',  # Ignored.
                     tls=False)
    expected = m_ftp.return_value
    assert expected == actual
    m_ftp.assert_called_once_with(host='<host>',
                                  user='<user>',
                                  passwd='<password>',
                                  acct='',
                                  timeout='<timeout>',
                                  source_address='<source_address>',
                                  encoding='<encoding>')


@mock.patch('iolib.ftp.FTP_TLS')
def test_connect_with_tls(m_ftp_tls):
    actual = connect(host='<host>',
                     user='<user>',
                     password='<password>',
                     acct=None,
                     timeout='<timeout>',
                     source_address='<source_address>',
                     encoding='<encoding>',
                     context='<context>',
                     tls=True)
    expected = m_ftp_tls.return_value
    assert expected == actual
    m_ftp_tls.assert_called_once_with(host='<host>',
                                  user='<user>',
                                  passwd='<password>',
                                  acct='',
                                  timeout='<timeout>',
                                  source_address='<source_address>',
                                  encoding='<encoding>',
                                  context='<context>')


@mock.patch('iolib.ftp.connect')
def test_list_ftp(m_connect):
    m_nlst = m_connect.return_value.nlst
    m_nlst.return_value = ['<path>/file1', '<path>/file2']
    actual = list_ftp(host='<host>',
                      path='<path>',
                      user='<user>',
                      password='<password>',
                      acct='<acct>',
                      timeout='<timeout>',
                      source_address='<source_address>',
                      encoding='<encoding>',
                      context='<context>',
                      tls='<tls>')
    expected = pd.DataFrame({'name': ['file1', 'file2']})
    pd.testing.assert_frame_equal(expected, actual)
    m_connect.assert_called_once_with('<host>',
                                      user='<user>',
                                      password='<password>',
                                      acct='<acct>',
                                      timeout='<timeout>',
                                      source_address='<source_address>',
                                      encoding='<encoding>',
                                      context='<context>',
                                      tls='<tls>')
    m_nlst.assert_called_once_with('<path>/')
    m_connect.return_value.quit.assert_called_once()


@mock.patch('iolib.ftp.connect')
def test_read_ftp_csv(m_connect):
    m_retrbinary = m_connect.return_value.retrbinary

    # File writer.
    def retrbinary(command, callback):
        callback(b'foo,bar\n1,2')

    m_retrbinary.side_effect = retrbinary
    actual = read_ftp(host='<host>',
                      path='file.csv',
                      user='<user>',
                      password='<password>',
                      acct='<acct>',
                      timeout='<timeout>',
                      source_address='<source_address>',
                      encoding='<encoding>',
                      context='<context>',
                      tls='<tls>',
                      usecols=['foo'])  # Passed to pandas.read_csv.
    expected = pd.DataFrame([{'foo': 1}])
    pd.testing.assert_frame_equal(expected, actual)
    m_connect.assert_called_once_with('<host>',
                                      user='<user>',
                                      password='<password>',
                                      acct='<acct>',
                                      timeout='<timeout>',
                                      source_address='<source_address>',
                                      encoding='<encoding>',
                                      context='<context>',
                                      tls='<tls>')
    m_retrbinary.assert_called_once()
    m_connect.return_value.quit.assert_called_once()


@mock.patch('iolib.ftp.connect')
def test_read_ftp_no_csv(m_connect):
    m_retrbinary = m_connect.return_value.retrbinary
    with pytest.raises(Exception) as error:
        actual = read_ftp(host='<host>',
                          path='file.xxx',
                          user='<user>',
                          password='<password>',
                          acct='<acct>',
                          timeout='<timeout>',
                          source_address='<source_address>',
                          encoding='<encoding>',
                          context='<context>',
                          tls='<tls>')
    assert 'Unsupported file format' == str(error.value)
    m_connect.assert_called_once_with('<host>',
                                      user='<user>',
                                      password='<password>',
                                      acct='<acct>',
                                      timeout='<timeout>',
                                      source_address='<source_address>',
                                      encoding='<encoding>',
                                      context='<context>',
                                      tls='<tls>')
    m_retrbinary.assert_called_once()
    m_connect.return_value.quit.assert_called_once()


@mock.patch('iolib.ftp.BytesIO')
@mock.patch('iolib.ftp.connect')
def test_write_ftp_csv(m_connect, m_bytesio):
    write_ftp(host='<host>',
              data=pd.DataFrame([{'foo': 1}]),
              path='file.csv',
              user='<user>',
              password='<password>',
              acct='<acct>',
              timeout='<timeout>',
              source_address='<source_address>',
              encoding='utf-8',
              context='<context>',
              tls='<tls>')
    m_connect.assert_called_once_with('<host>',
                                      user='<user>',
                                      password='<password>',
                                      acct='<acct>',
                                      timeout='<timeout>',
                                      source_address='<source_address>',
                                      encoding='utf-8',
                                      context='<context>',
                                      tls='<tls>')
    m_ftp = m_connect.return_value
    m_file = m_bytesio.return_value
    m_file.write.assert_called_once_with(b'foo\n1\n')
    m_file.seek.assert_called_once_with(0)
    m_ftp.storbinary.assert_called_once_with('STOR file.csv', m_file)
    m_ftp.quit.assert_called_once()


@mock.patch('iolib.ftp.BytesIO')
@mock.patch('iolib.ftp.connect')
def test_write_ftp_no_csv(m_connect, m_bytesio):
    with pytest.raises(Exception) as error:
        actual = write_ftp(host='<host>',
                           data='<data>',
                           path='file.xxx',
                           user='<user>',
                           password='<password>',
                           acct='<acct>',
                           timeout='<timeout>',
                           source_address='<source_address>',
                           encoding='<encoding>',
                           context='<context>',
                           tls='<tls>')
    assert 'Unsupported file format' == str(error.value)
    m_connect.assert_not_called()
    m_bytesio.assert_not_called()
