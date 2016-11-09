import json

import fabric
from django.core.management.base import BaseCommand
from django.db import connection
# isort seems happy enough with this import, but pylint isn't, deferring to isort.
from fabric.api import local, prompt  # pylint: disable=ungrouped-imports
from fabric.contrib.console import confirm

fabric.state.output['running'] = False

VALIDATION_ERROR = """{selection} is not a valid option, please select from one
                   of the following {type}s: {choices}'"""
JSON_FILENAME = 'datamigration.json'


class Command(BaseCommand):
    help = "Migrate from an old database to this one."

    def add_arguments(self, parser):
        parser.add_argument('--database', help='Specify which database to use')
        parser.add_argument('--file', help='Load config from a JSON file')

    def get_databases(self):
        databases = local('echo "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname;" | psql', capture=True)
        return [database.strip() for database in databases.split('\n')[2:-1]]

    def validate_database(self, selection):
        databases = self.get_databases()

        if selection in databases:
            return selection

        raise KeyError(VALIDATION_ERROR.format(
            select=selection,
            type='database',
            choices=', '.join(databases)
        ))

    def get_tables(self):
        # Disabling pylint on the next line as it thinks the PSQL command is RegEx, it's not.
        tables = local('echo "\dt" | psql -d {}'.format(self.database), capture=True)  # pylint: disable=anomalous-backslash-in-string
        return [table.split('|')[1].strip() for table in tables.split('\n')[3:-1]]

    def validate_table(self, selection):
        tables = self.get_tables()

        if selection in tables:
            return selection

        raise KeyError(VALIDATION_ERROR.format(
            selection=selection,
            type='table',
            choices=', '.join(tables)
        ))

    def validate_local_table(self, selection):
        tables = connection.introspection.table_names()

        if selection in tables:
            return selection

        raise KeyError(VALIDATION_ERROR.format(
            selection=selection,
            type='table',
            choices=', '.join(tables)
        ))

    def get_columns(self):
        columns = local('echo "SELECT * FROM {} WHERE false;" | psql -d {}'.format(
            self.active_table,
            self.database,
        ), capture=True)

        return [column.strip() for column in columns.split('\n')[0].split('|')]

    def get_local_columns(self):
        with connection.cursor() as cursor:
            cursor.execute('SELECT * FROM {} WHERE false'.format(self.active_table))
            return [col[0] for col in cursor.description]

    def validate_column(self, selection):
        columns = self.get_columns()

        if selection in columns:
            return selection

        raise KeyError(VALIDATION_ERROR.format(
            selection=selection,
            type='column',
            choices=', '.join(columns)
        ))

    def validate_local_column(self, selection):
        columns = self.get_local_columns()

        if selection in columns:
            return selection

        raise KeyError(VALIDATION_ERROR.format(
            selection=selection,
            type='column',
            choices=', '.join(columns)
        ))

    # Disabling some of the "too many" pylint rules for now. May need to break parts out.
    def build_table_data(self, *args, **options):  # pylint: disable=too-complex, too-many-locals, too-many-statements, too-many-branches
        self.database = None

        if options['database']:
            if self.validate_database(options['database']):
                self.database = options['database']
        else:
            print 'Available databases: {}\n'.format(', '.join(self.get_databases()))

        if not self.database:
            self.database = prompt('Which database would you like to import from?', validate=self.validate_database)

        self.table_data = {}
        tables_selected = False

        while not tables_selected:
            if len(self.table_data) > 0:
                continue_selecting = confirm('Would you like to select another table?')

                if not continue_selecting:
                    tables_selected = True
                    continue

            table = []
            table.append(prompt('Please select a table to migrate data from:', validate=self.validate_table))
            table.append(prompt('Please select a table to map to:', default=table[0], validate=self.validate_local_table))

            self.table_data[table[0]] = {
                'map_to': table[1]
            }

        print
        print 'Database: {}'.format(self.database)
        print 'Table mapping(s):'
        for table in self.table_data:
            print '{} -> {}'.format(
                table,
                self.table_data[table]['map_to']
            )

        for table in self.table_data:
            map_to = self.table_data[table]['map_to']

            self.active_table = table

            print '\nNow working on {} -> {}'.format(table, map_to)
            # print 'Available columns: {}'.format(', '.join(self.get_columns()))

            # Select which columns you want to map.
            # Select which table it needs to map against.
            # Select how columns from the old DB map against the new DB.
            # Work out if we need to put any FKs into a temp table.
            #  - This allows us to migrate media etc without losing relations.

            columns = []
            columns_selected = False

            # Try to auto-map columns (if they have the same name)
            for old_column in self.get_columns():
                self.active_table = map_to
                for new_column in self.get_local_columns():
                    if old_column == new_column or old_column == 'url_title' and new_column == 'slug':
                        if confirm('Would you like to map {} to {}?'.format(old_column, new_column)):
                            columns.append((old_column, new_column))
                        continue

                self.active_table = table

            while not columns_selected:
                if len(columns) > 0:
                    # Perhaps show a list of columns still not mapped?
                    print '\nAlready mapped: {}'.format(
                        ', '.join(column[0] for column in columns)
                    )
                    print 'Still mappable: {}'.format(
                        ', '.join(
                            old_column for old_column in self.get_columns()
                            if old_column not in [column[0] for column in columns]
                        )
                    )

                    continue_selecting = confirm('Would you like to select another column?')

                    if not continue_selecting:
                        columns_selected = True
                        continue

                column = []

                # Ask if they're trying to provide a value for a non-nullable field.

                self.active_table = table
                column.append(prompt(
                    'Please select a column to migrate data from:',
                    validate=self.validate_column
                ))

                self.active_table = map_to
                column.append(prompt(
                    'Please select a column to map to:',
                    validate=self.validate_local_column
                ))

                columns.append(tuple(column))

            self.table_data[table]['columns'] = columns

            # How many columns with a not-null constraint still exist?
            # Disabling pylint on the next line as it thinks the PSQL command is RegEx, it's not.
            self.active_table = map_to
            null_columns = local('echo "\d {};" | psql -d {}'.format(  # pylint: disable=anomalous-backslash-in-string
                table,
                connection.settings_dict['NAME'],
            ), capture=True).split('\n')[3:len(self.get_local_columns()) + 3]

            null_columns = [
                null_column.split('|')[0].strip() for null_column in null_columns
                if 'not null' in null_column.split('|')[2].strip() and
                null_column.split('|')[0].strip() not in [inner_column[1] for inner_column in columns]
            ]

            # Are there any other fields on the new table which we might want to populate?
            new_columns = [
                new_column for new_column in self.get_local_columns()
                if new_column not in [inner_column[1] for inner_column in self.table_data[table]['columns']]
            ]

            print '\nThere are {} column(s) which are in the new table, but are missing a value: {}:'.format(
                len(new_columns),
                ', '.join(new_columns)
            )
            self.table_data[table]['other_columns'] = []

            for new_column in new_columns:
                if confirm('Would you like to provide a default value for {}{}?'.format(
                        new_column,
                        ' (not nullable)' if new_column in null_columns else ''
                )):
                    self.table_data[table]['other_columns'].append(
                        (new_column, prompt('What value would you like to use? (use single quotes around strings)', default="''"))
                    )

            self.table_data[table]['export_conditional'] = None
            if confirm('\nWould you like to provide a conditional to the data exporter?'):
                self.table_data[table]['export_conditional'] = prompt('What would you like it to be? (include the WHERE)')

    def handle(self, *args, **options):
        if options['file']:
            with open(options['file']) as f:
                json_data = json.load(f)
                self.table_data = json_data['table_data']
                self.database = json_data['database']
        else:
            self.build_table_data(*args, **options)

            # Dump the current state of affairs to a JSON file.
            if confirm('Would you like to dump your current settings to a JSON file?'):
                json_filename = prompt('What filename would you like to use?', default=JSON_FILENAME)

                with open(json_filename, 'w') as f:
                    json.dump({
                        'table_data': self.table_data,
                        'database': self.database
                    }, f, indent=2)

        # Confirm all of the actions before executing.
        print
        print 'To confirm, this is the mapping you have configured:\n'

        for table in self.table_data:
            print '{} -> {}'.format(table, self.table_data[table]['map_to'])

            for column in self.table_data[table]['columns']:
                print ' - {} -> {}'.format(*column)

            for null_column, null_value in self.table_data[table]['other_columns']:
                print u' - Set `{}` to {}'.format(
                    null_column,
                    null_value,
                )

            print

        if not confirm('Are you happy to proceed?'):
            print 'Ok, bye.'
            exit()

        for table in self.table_data:
            command = """
                psql -d {old_database} -c 'copy(SELECT {old_fields}{null_values} FROM {old_table} {conditional}) to stdout' \
                | \
                psql -d {new_database} -c 'COPY {new_table} ({new_fields}{other_columns}) from stdin'
            """.format(
                old_database=self.database,
                old_fields=', '.join(['"{}"'.format(pair[0]) for pair in self.table_data[table]['columns']]),
                old_table=table,
                null_values='' if not self.table_data[table].get('other_columns', []) else '{}{}'.format(
                    ', ',
                    ', '.join([pair[1].replace("'", "'\\''") for pair in self.table_data[table]['other_columns']])
                ),
                conditional=self.table_data[table]['export_conditional'] if self.table_data[table]['export_conditional'] else '',
                new_database=connection.settings_dict['NAME'],
                new_fields=', '.join(['"{}"'.format(pair[1]) for pair in self.table_data[table]['columns']]),
                new_table=self.table_data[table]['map_to'],
                other_columns='' if not self.table_data[table].get('other_columns', []) else '{}{}'.format(
                    ', ',
                    ', '.join(['"{}"'.format(pair[0]) for pair in self.table_data[table]['other_columns']])
                ),
            )

            print "\n[{}] Running: {}".format(table, " ".join(command.split()))
            local(command)
