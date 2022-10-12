from setuptools import setup, find_packages


with open('VERSION') as infile:
    version = infile.read().strip()


setup(
    name='python-io',
    description='Python tools to read/write from/to external services',
    version=version,
    packages=find_packages(include=['iolib*']),
    install_requires=[
        'google-cloud-bigquery>=2.0.0,<4.0.0',
        'pandas==1.4.*',
        'pyarrow==8.*',
        'db-dtypes',
    ],
    extras_require={
        'dev': [
            'pytest',
            'ipykernel',
        ],
    },
)
