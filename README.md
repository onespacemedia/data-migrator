# data-migrator
Make data migrate again!

Data migration tool for Django. Runs as a management command. Can import / export config in JSON format.

## Installation

Put `datamigration.py` into a directory such as `project/apps/management/commands/datamigration.py`. Make sure you have all of the `__init__.py` files required in the tree.

## Usage

* Run `./manage.py datamigration`

## Options

You can pass in the following options:

* `--database database_name` to specify the "from" database to use.
* `--file datamigration.json` to specify the config file to use.
