#!/usr/bin/env python

from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
import argparse
import sys


parser = argparse.ArgumentParser()
parser.add_argument('-a', '--address', action='store', default='localhost', help='Elasticsearch address')
parser.add_argument('-p', '--port',    action='store', default=9200,        help='Elasticsearch port')
parser.add_argument('-P', '--period',  action='store', default=14,          help='Rotation period',      type=int)
parser.add_argument('-n', '--name',    action='store', default="logstash-", help='Index prefix')
args = vars(parser.parse_args())


# Create elasticsearch client
es = Elasticsearch('http://{}:{}'.format(args['address'], args['port']))


def get_indexes_list():

    """
    Function obtains list of present logstash indexes
    """

    indexes_status = es.indices.status()
    indexes = [ i for i in indexes_status['indices'] if 'logstash' in i ]
    return indexes


def filter_dates():

    """
    Function sorts dates and return obsolete indexes
    """

    if len(get_indexes_list()) > args['period']:

        # Get all indexes from local fs and sort them
        all_indexes = [ datetime.strptime(k.split('-')[1], '%Y.%m.%d') for k in get_indexes_list() ]
        all_indexes.sort()

        # Get obsolete indexes
        obsolete_dates = [ item for item in all_indexes[0:args['period']] ]
        obsolete_indexes = [ '{0}{1}'.format(args['name'], i.strftime('%Y.%m.%d')) for i in obsolete_dates ]

        return obsolete_indexes

    else:
        print('Indexes count less than {}'.format(args['period']))
        sys.exit(0)


def clean_obsolete():

    """
    Function purge obsolete indexes
    """

    obsolete_indexes = filter_dates()
    for i in obsolete_indexes:
        try:
            es.indices.delete(i)
        except NotFoundError as e:
            print(e)


if __name__ == '__main__':
    clean_obsolete()
