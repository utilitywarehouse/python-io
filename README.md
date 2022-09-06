# python-io

Python tools to read/write from/to external services

## Installation

### pip

```
pip install git+https://git@github.com/utilitywarehouse/python-io.git
```

For development install the required packages as

```
pip install -e .
```

or use `[dev]` to install the extra packages

```
pip install -e .[dev]
```

### poetry

If using poetry, you'll need version 1.2+.

```
poetry add git+https://git@github.com/utilitywarehouse/python-io.git
```

To exclude dev packages:

```
poetry install --only main
```

### As a git sub-module

You might want to do this to avoid the need to handle git credentials (personal access token or priv/pub key pairs) in your build pipeline.

At the root of your project:

```
git submodule add https://github.com/utilitywarehouse/python-io _submodules/python-io
```

Then either:

```
pip install _submodules/python-io
```

or

```
poetry add _submodules/python-io
```

## Documentation

You can find the autogenerated documentation in [the wiki](https://github.com/utilitywarehouse/python-io/wiki).
