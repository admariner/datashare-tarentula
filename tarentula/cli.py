import logging
import click

from tarentula.config_file_reader import ConfigFileReader
from tarentula.logger import add_syslog_handler, add_stdout_handler
from tarentula.metadata_fields import MetadataFields
from tarentula.tag_cleaning_by_query import TagsCleanerByQuery
from tarentula.tagging import Tagger
from tarentula.tagging_by_query import TaggerByQuery
from tarentula.download import Download
from tarentula.export_by_query import ExportByQuery
from tarentula.count import Count
from tarentula.aggregate import AggCount, GeneralStats, DateHistogram, NumUnique
from tarentula import __version__


def validate_loglevel(ctx, param, value):
    # pylint: disable=unused-argument
    try:
        if isinstance(value, str):
            return getattr(logging, value)
        return int(value)
    except (AttributeError, ValueError) as exc:
        raise click.BadParameter('must be a valid log level (CRITICAL, ERROR, WARNING, INFO, DEBUG or NOTSET)') from exc


def validate_progressbar(ctx, param, value):
    # pylint: disable=unused-argument
    # If no value given, we activate the progress bar only when the
    # stdout_loglevel value is higher than INFO (20)
    return value if value is not None else ctx.obj['stdout_loglevel'] > 20


@click.group()
@click.pass_context
@click.version_option(message='v%(version)s', version=__version__)
@click.option('--syslog-address', help='Syslog address',
              default=ConfigFileReader('syslog_address', 'localhost', 'logger'))
@click.option('--syslog-port', help='Syslog port',
              default=ConfigFileReader('syslog_port', 514, 'logger'))
@click.option('--syslog-facility', help='Syslog facility',
              default=ConfigFileReader('syslog_facility', 'local7', 'logger'))
@click.option('--stdout-loglevel', help='Change the default log level for stdout error handler',
              default=ConfigFileReader('stdout_loglevel', 'ERROR', 'logger'),
              callback=validate_loglevel)
def cli(ctx, **options):
    # Configure Syslog handler
    add_syslog_handler(options['syslog_address'], int(options['syslog_port']), options['syslog_facility'])
    add_stdout_handler(options['stdout_loglevel'])
    # Pass all option to context
    ctx.ensure_object(dict)
    ctx.obj.update(options)


@click.command()
@click.option('--apikey', help='Datashare authentication apikey', default=ConfigFileReader('apikey'))
@click.option('--datashare-url', help='Datashare URL',
              default=ConfigFileReader('datashare_url', 'http://localhost:8080'))
@click.option('--datashare-project', help='Datashare project',
              default=ConfigFileReader('datashare_project', 'local-datashare'))
@click.option('--throttle', help='Request throttling (in ms)', default=0)
@click.option('--cookies', help='Key/value pair to add a cookie to each request to the API. You can separate'
                                'semicolons: key1=val1;key2=val2;...', default='')
@click.option('--traceback/--no-traceback', help='Display a traceback in case of error', default=False)
@click.option('--progressbar/--no-progressbar', help='Display a progressbar', default=None,
              callback=validate_progressbar)
@click.argument('csv-path', type=click.Path(exists=True))
def tagging(**options):
    # Instantiate a Tagger class with all the options
    tagger = Tagger(**options)
    # Proceed to tagging
    tagger.start()


@click.command()
@click.option('--apikey', help='Datashare authentication apikey', default=ConfigFileReader('apikey'))
@click.option('--datashare-project', help='Datashare project',
              default=ConfigFileReader('datashare_project', 'local-datashare'))
@click.option('--elasticsearch-url', help='Elasticsearch URL which is used to perform update by query',
              default='http://localhost:9200')
@click.option('--throttle', help='Request throttling (in ms)', default=0)
@click.option('--cookies', help='Key/value pair to add a cookie to each request to the API. You can separate'
                                'semicolons: key1=val1;key2=val2;...', default='')
@click.option('--traceback/--no-traceback', help='Display a traceback in case of error', default=False)
@click.option('--progressbar/--no-progressbar', help='Display a progressbar', default=None,
              callback=validate_progressbar)
@click.option('--wait-for-completion/--no-wait-for-completion', help='Create a Elasticsearch task to perform the update'
                                                                     'asynchronously', default=True)
@click.option('--scroll-size', help='Size of the scroll request that powers the operation.', default=1000)
@click.argument('json-path', type=click.Path(exists=True))
def tagging_by_query(**options):
    # Instantiate a TaggerByQuery class with all the options
    tagger = TaggerByQuery(**options)
    tagger.start()


@click.command()
@click.option('--apikey', help='Datashare authentication apikey', default=ConfigFileReader('apikey'))
@click.option('--datashare-project', help='Datashare project',
              default=ConfigFileReader('datashare_project', 'local-datashare'))
@click.option('--elasticsearch-url', help='Elasticsearch URL which is used to perform update by query',
              default='http://localhost:9200')
@click.option('--cookies', help='Key/value pair to add a cookie to each request to the API. You can separate'
                                'semicolons: key1=val1;key2=val2;...', default='')
@click.option('--wait-for-completion/--no-wait-for-completion', help='Create a Elasticsearch task to perform the update'
                                                                     'asynchronously', default=True)
@click.option('--query', help='Give a JSON query to filter documents that will have their tags cleaned. It can be a'
                              'file with @path/to/file. Default to all.', default=None)
def clean_tags_by_query(**options):
    tagger = TagsCleanerByQuery(**options)
    tagger.start()


@click.command()
@click.option('--apikey', help='Datashare authentication apikey', default=ConfigFileReader('apikey'))
@click.option('--datashare-url', help='Datashare URL',
              default=ConfigFileReader('datashare_url', 'http://localhost:8080'))
@click.option('--datashare-project', help='Datashare project',
              default=ConfigFileReader('datashare_project', 'local-datashare'))
@click.option('--elasticsearch-url', help='You can additionally pass the Elasticsearch URL in order to use scrolling'
                                          'capabilities of Elasticsearch (useful when dealing with a lot of results)',
              default=None)
@click.option('--query', help='The query string to filter documents', default='*')
@click.option('--destination-directory', help='Directory documents will be downloaded', default='./tmp')
@click.option('--throttle', help='Request throttling (in ms)', default=0)
@click.option('--cookies', help='Key/value pair to add a cookie to each request to the API. You can separate'
                                'semicolons: key1=val1;key2=val2;...', default='')
@click.option('--path-format', help='Downloaded document path template', default='{id_2b}/{id_4b}/{id}')
@click.option('--scroll', help='Scroll duration', default=None)
@click.option('--source', help='A comma-separated list of field to include in the downloaded document from the index',
              default=None)
@click.option('--limit', '-l', type=int, help='Limit the total results to return', default=0)
@click.option('--from', '-f', 'from_', type=int, help='Passed to the search it will bypass the first n documents',
              default=0)
@click.option('--size', help='Size of the scroll request that powers the operation.', default=1000)
@click.option('--sort-by', help='Field to use to sort results', default='_score')
@click.option('--order-by', help='Order to use to sort results', default='desc',
              type=click.Choice(['asc', 'desc']))
@click.option('--once/--not-once', help='Download file only once', default=False)
@click.option('--traceback/--no-traceback', help='Display a traceback in case of error', default=False)
@click.option('--progressbar/--no-progressbar', help='Display a progressbar', default=None,
              callback=validate_progressbar)
@click.option('--raw-file/--no-raw-file', help='Download raw file from Datashare', default=True)
@click.option('--type', help='Type of indexed documents to download', default='Document',
              type=click.Choice(['Document', 'NamedEntity'], case_sensitive=True))
def download(**options):
    # Instantiate a Download class with all the options
    downl = Download(**options)
    downl.start()


@click.command()
@click.option('--apikey', help='Datashare authentication apikey', default=ConfigFileReader('apikey'))
@click.option('--datashare-url', help='Datashare URL',
              default=ConfigFileReader('datashare_url', 'http://localhost:8080'))
@click.option('--datashare-project', help='Datashare project',
              default=ConfigFileReader('datashare_project', 'local-datashare'))
@click.option('--elasticsearch-url', help='You can additionally pass the Elasticsearch URL in order to use scrolling'
                                          'capabilities of Elasticsearch (useful when dealing with a lot of results)',
              default=None)
@click.option('--query', help='The query string to filter documents', default='*')
@click.option('--output-file', help='Path to the CSV file', default='tarentula_documents.csv')
@click.option('--throttle', help='Request throttling (in ms)', default=0)
@click.option('--cookies', help='Key/value pair to add a cookie to each request to the API. You can separate'
                                'semicolons: key1=val1;key2=val2;...', default='')
@click.option('--scroll', help='Scroll duration', default=None)
@click.option('--source', help='A comma-separated list of field to include in the export',
              default='contentType,contentLength:0,extractionDate,path')
@click.option('--sort-by', help='Field to use to sort results', default='_score')
@click.option('--order-by', help='Order to use to sort results', default='desc',
              type=click.Choice(['asc', 'desc']))
@click.option('--traceback/--no-traceback', help='Display a traceback in case of error', default=False)
@click.option('--progressbar/--no-progressbar', help='Display a progressbar', default=None,
              callback=validate_progressbar)
@click.option('--type', help='Type of indexed documents to download', default='Document',
              type=click.Choice(['Document', 'NamedEntity', 'Duplicate'], case_sensitive=True))
@click.option('--size', help='Size of the scroll request that powers the operation.', default=1000)
@click.option('--from', '-f', 'from_', type=int, help='Passed to the search it will bypass the first n documents',
              default=0)
@click.option('--limit', '-l', type=int, help='Limit the total results to return', default=0)
@click.option('--query-field/--no-query-field', help='Add the query to the export CSV', default=True)
def export_by_query(**options):
    # Instantiate an ExportByQuery class with all the options
    export = ExportByQuery(**options)
    export.start()


@click.command()
@click.option('--apikey', help='Datashare authentication apikey', default=ConfigFileReader('apikey'))
@click.option('--datashare-url', help='Datashare URL',
              default=ConfigFileReader('datashare_url', 'http://localhost:8080'))
@click.option('--datashare-project', help='Datashare project',
              default=ConfigFileReader('datashare_project', 'local-datashare'))
@click.option('--elasticsearch-url', help='You can additionally pass the Elasticsearch URL in order to use scrolling'
                                          'capabilities of Elasticsearch (useful when dealing with a lot of results)',
              default=None)
@click.option('--query', help='The query string to filter documents', default='*')
@click.option('--cookies', help='Key/value pair to add a cookie to each request to the API. You can separate'
                                'semicolons: key1=val1;key2=val2;...', default='')
@click.option('--traceback/--no-traceback', help='Display a traceback in case of error', default=False)
@click.option('--type', help='Type of indexed documents to download', default='Document',
              type=click.Choice(['Document', 'NamedEntity'], case_sensitive=True))
def count(**options):
    # Instantiate a Count class with all the options
    cnt = Count(**options)
    cnt.start()


@click.command()
@click.option('--datashare-url', help='Datashare URL',
              default=ConfigFileReader('datashare_url', 'http://localhost:8080'))
@click.option('--datashare-project', help='Datashare project',
              default=ConfigFileReader('datashare_project', 'local-datashare'))
@click.option('--elasticsearch-url', help='You can additionally pass the Elasticsearch URL in order to use scrolling'
                                          'capabilities of Elasticsearch (useful when dealing with a lot of results)',
              default=None)
@click.option('--apikey', help='Datashare authentication apikey', default=ConfigFileReader('apikey'))
@click.option('--cookies', help='Key/value pair to add a cookie to each request to the API. You can separate'
                                'semicolons: key1=val1;key2=val2;...', default='')
@click.option('--traceback/--no-traceback', help='Display a traceback in case of error', default=False)
@click.option('--type', help='Type of indexed documents to get metadata', default='Document',
              type=click.Choice(['Document', 'NamedEntity'], case_sensitive=True))
@click.option('--filter_by',
              help='Filter documents by pairs concatenated by coma of field names and values separated by =.'
                   'Example "contentType=message/rfc822,contentType=message/rfc822"', default='')
@click.option('--count/--no-count', help='Count or not the number of docs for each property found', default=False)
def list_metadata(**options):
    metadata = MetadataFields(**options)
    metadata.start()


@click.command()
@click.option('--apikey', help='Datashare authentication apikey', default=ConfigFileReader('apikey'))
@click.option('--datashare-url', help='Datashare URL',
              default=ConfigFileReader('datashare_url', 'http://localhost:8080'))
@click.option('--datashare-project', help='Datashare project',
              default=ConfigFileReader('datashare_project', 'local-datashare'))
@click.option('--elasticsearch-url', help='You can additionally pass the Elasticsearch URL in order to use scrolling'
                                          'capabilities of Elasticsearch (useful when dealing with a lot of results)',
              default=None)
@click.option('--query', help='The query string to filter documents', default='*')
@click.option('--cookies', help='Key/value pair to add a cookie to each request to the API. You can separate'
                                'semicolons: key1=val1;key2=val2;...', default='')
@click.option('--traceback/--no-traceback', help='Display a traceback in case of error', default=False)
@click.option('--type', help='Type of indexed documents to download', default='Document',
              type=click.Choice(['Document', 'NamedEntity'], case_sensitive=True))
@click.option('--group_by', help='Field to use to aggregate results', default=None)
@click.option('--operation_field', help='Field to run the operation on', default=None)
@click.option('--run', help='Operation to run ', default='count',
              type=click.Choice(
                  ['count', 'nunique', 'date_histogram', 'sum', 'stats', 'string_stats', 'min', 'max', 'avg']))
@click.option('--calendar_interval', help='Calendar interval for date histogram aggregation', default='year',
              type=click.Choice(['year', 'month']))
def aggregate(**options):
    agg_operation = options['run']

    if agg_operation == 'count':
        agg = AggCount(**options)
    elif agg_operation == 'nunique':
        agg = NumUnique(**options)
    elif agg_operation == 'date_histogram':
        agg = DateHistogram(**options)
    elif agg_operation in ['sum', 'stats', 'string_stats', 'min', 'max', 'avg']:
        agg = GeneralStats(**options)

    agg.start()


cli.add_command(tagging)
cli.add_command(download)
cli.add_command(tagging_by_query)
cli.add_command(clean_tags_by_query)
cli.add_command(export_by_query)
cli.add_command(count)
cli.add_command(list_metadata)
cli.add_command(aggregate)

if __name__ == '__main__':
    cli(None) # ctx from @cli.pass_context
