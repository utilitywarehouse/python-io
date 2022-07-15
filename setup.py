from setuptools import setup, find_packages


setup(
    name='python-io',
    description='Python tools to read/write from/to external services',
    version='0.1.0',
    packages=find_packages(include=['iolib*']),
    install_requires=[
        'google-cloud-bigquery==2.*',
        'pandas==1.4.*',
        'pyarrow==8.*',
    ],
    extras_require={
        'dev': [
            'pytest',
        ],
    },
)