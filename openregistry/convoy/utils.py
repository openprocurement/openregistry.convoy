# -*- coding: utf-8 -*-
from logging import getLogger
from pkg_resources import get_distribution
from time import sleep
from munch import Munch

from openprocurement_client.clients import APIResourceClient as APIClient
from openprocurement_client.resources.assets import AssetsClient
from openprocurement_client.resources.lots import LotsClient

from couchdb import Server, Session


PKG = get_distribution(__package__)
LOGGER = getLogger(PKG.project_name)

FILTER_DOC_ID = '_design/auction_filters'
FILTER_CONVOY_FEED_DOC = """
function(doc, req) {
    if (doc.doc_type == 'Auction') {
        if (doc.status == 'pending.verification') {
            return true;
        } else if (['complete', 'cancelled', 'unsuccessful'].indexOf(doc.status) >= 0 && doc.merchandisingObject) {
            return true;
        };
    }
    return false;
}
"""

CONTINUOUS_CHANGES_FEED_FLAG = True  # Need for testing


def push_filter_doc(db):
    filters_doc = db.get(FILTER_DOC_ID, {'_id': FILTER_DOC_ID, 'filters': {}})
    if (filters_doc and filters_doc['filters'].get('convoy_feed') !=
        FILTER_CONVOY_FEED_DOC):
        filters_doc['filters']['convoy_feed'] = \
            FILTER_CONVOY_FEED_DOC
        db.save(filters_doc)
        LOGGER.info('Filter doc \'convoy_feed\' saved.')
    else:
        LOGGER.info('Filter doc \'convoy_feed\' exist.')


def continuous_changes_feed(db, timeout=10, limit=100,
                            filter_doc='auction_filters/convoy_feed'):
    last_seq_id = 0
    while CONTINUOUS_CHANGES_FEED_FLAG:
        data = db.changes(include_docs=True, since=last_seq_id, limit=limit,
                          filter=filter_doc)
        last_seq_id = data['last_seq']
        if len(data['results']) != 0:
            for row in data['results']:
                item = Munch({
                    'id': row['doc']['_id'],
                    'status': row['doc']['status'],
                    'merchandisingObject': row['doc']['merchandisingObject']
                })
                yield item
        else:
            sleep(timeout)


def init_clients(config, is_check=False):
    exceptions = []
    clients_from_config = {
        'api_client': {'section': 'cdb', 'client_instance': APIClient},
        'lots_client': {'section': 'lots_db', 'client_instance': LotsClient},
        'assets_client': {'section': 'assets_db', 'client_instance': AssetsClient},
    }
    result = ''

    for key, item in clients_from_config.items():
        try:
            client = item['client_instance'](**config[item['section']])
            clients_from_config[key] = client
            result = 'ok'
        except Exception as e:
            exceptions.append(e)
            result = 'failed {} - {}'.format(repr(e.args), e.message)
        LOGGER.info('{} - {}'.format(key, result))
    if not hasattr(clients_from_config['api_client'], 'ds_client'):
        LOGGER.warning("Document Service configuration is not available.")

    try:
        user = config['couchdb'].get('user', '')
        password = config['couchdb'].get('password', '')
        url = "http://{host}:{port}".format(**config['couchdb'])
        result = 'couchdb without user'
        if user and password:
            url = "http://{user}:{password}@{host}:{port}".format(**config['couchdb'])
            result = 'couchdb - authorized'
        LOGGER.info(result)
        server = Server(url, session=Session(retry_delays=range(10)))
        db = server[config['couchdb']['db']] if \
            config['couchdb']['db'] in server else \
            server.create(config['couchdb']['db'])
        clients_from_config['db'] = db
        result = 'ok'
        push_filter_doc(db)
        LOGGER.info('Added filters doc to db.')
    except Exception as e:
        exceptions.append(e)
        result = 'failed {}'.format(type(e))
    LOGGER.info('couchdb - {}'.format(result))

    if not is_check and exceptions:
        raise exceptions[0]

    return clients_from_config