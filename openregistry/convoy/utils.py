# -*- coding: utf-8 -*-
from logging import getLogger, addLevelName, Logger
from socket import error

from time import sleep

from couchdb import Server, Session
from munch import Munch
from pkg_resources import get_distribution

from openprocurement_client.exceptions import (
    Forbidden,
    RequestFailed,
    ResourceNotFound,
    UnprocessableEntity,
    Conflict,
    PreconditionFailed,
)
from openprocurement_client.resources.assets import AssetsClient
from openprocurement_client.resources.auctions import AuctionsClient
from openprocurement_client.resources.lots import LotsClient
from openprocurement_client.resources.contracts import ContractingClient

from openregistry.convoy.loki.constants import (
    CONTRACT_TYPE,
    CONTRACT_REQUIRED_FIELDS,
    CONTRACT_NOT_REQUIRED_FIELDS,
)


addLevelName(25, 'CHECK')


def check(self, msg, exc=None, *args, **kwargs):
    self.log(25, msg)
    if exc:
        self.error(exc, exc_info=True)


Logger.check = check

PKG = get_distribution(__package__)
LOGGER = getLogger(PKG.project_name)

EXCEPTIONS = (Forbidden, RequestFailed, ResourceNotFound, UnprocessableEntity, PreconditionFailed, Conflict)
FILTER_DOC_ID = '_design/auction_filters'
FILTER_CONVOY_FEED_DOC = """
function(doc, req) {
    if (doc.doc_type == 'Auction') {
    
        // basic lots auctions
        if (%s.indexOf(doc.procurementMethodType) >= 0) {
    
            if (doc.status == 'pending.verification') {
                return true;
            } else if (['complete', 'cancelled', 'unsuccessful'].indexOf(doc.status) >= 0 && doc.merchandisingObject) {
                return true;
            };
            
        // loki lots auctions
        } else if (%s.indexOf(doc.procurementMethodType) >= 0) {
        
            if (['TODO_complete', 'cancelled', 'unsuccessful'].indexOf(doc.status) >= 0  && doc.merchandisingObject) {
                return true;
            };
        
        };
        
    }
    return false;
}
"""

CONTINUOUS_CHANGES_FEED_FLAG = True  # Need for testing


class ConfigError(Exception):
    pass


def prepare_couchdb(couch_url, db_name):
    server = Server(couch_url, session=Session(retry_delays=range(10)))
    try:
        if db_name not in server:
            db = server.create(db_name)
        else:
            db = server[db_name]

    except error as e:
        LOGGER.error('Database error: {}'.format(e.message))
        raise ConfigError(e.strerror)
    return db


def push_filter_doc(db, auctions_types):
    filter_convoy_feed_doc = FILTER_CONVOY_FEED_DOC % (
        auctions_types.get('basic', []), auctions_types.get('loki', [])
    )
    filters_doc = db.get(FILTER_DOC_ID, {'_id': FILTER_DOC_ID, 'filters': {}})
    if (filters_doc and filters_doc['filters'].get('convoy_feed') !=
            filter_convoy_feed_doc):
        filters_doc['filters']['convoy_feed'] = \
            filter_convoy_feed_doc
        db.save(filters_doc)
        LOGGER.info('Filter doc \'convoy_feed\' saved.')
    else:
        LOGGER.info('Filter doc \'convoy_feed\' exist.')
    LOGGER.info('Added filters doc to db.')


def continuous_changes_feed(db, killer, timeout=10, limit=100,
                            filter_doc='auction_filters/convoy_feed'):
    last_seq_id = 0
    while CONTINUOUS_CHANGES_FEED_FLAG:
        data = db.changes(include_docs=True, since=last_seq_id, limit=limit,
                          filter=filter_doc)
        last_seq_id = data['last_seq']
        if len(data['results']) != 0:
            for row in data['results']:
                item = Munch(row['doc'])
                yield item
            if killer.kill_now:
                break

        else:
            if killer.kill_now:
                break
            sleep(timeout)


def init_clients(config):
    clients_from_config = {
        'auctions_client': {'section': 'auctions', 'client_instance': AuctionsClient},
        'lots_client': {'section': 'lots', 'client_instance': LotsClient},
        'assets_client': {'section': 'assets', 'client_instance': AssetsClient},
        'contracts_client': {'section': 'contracts', 'client_instance': ContractingClient},
    }
    exceptions = []

    for key, item in clients_from_config.items():
        section = item['section']
        try:
            client = item['client_instance'](
                key=config[section]['api']['token'],
                host_url=config[section]['api']['url'],
                api_version=config[section]['api']['version'],
                ds_config=config[section].get('ds', None)
            )
            clients_from_config[key] = client
            result = ('ok', None)
        except Exception as e:
            exceptions.append(e)
            result = ('failed', e)
        LOGGER.check('{} - {}'.format(key, result[0]), result[1])
    if not hasattr(clients_from_config['auctions_client'], 'ds_client'):
        LOGGER.warning("Document Service configuration is not available.")

    try:
        if config['db'].get('login', '') \
                and config['db'].get('password', ''):
            db_url = "http://{login}:{password}@{host}:{port}".format(
                **config['db']
            )
            LOGGER.info('couchdb - authorized')
        else:
            db_url = "http://{host}:{port}".format(**config['db'])
            LOGGER.info('couchdb without user')

        clients_from_config['db'] = prepare_couchdb(
            db_url, config['db']['name']
        )
        result = ('ok', None)
    except Exception as e:
        exceptions.append(e)
        result = ('failed', e)
    LOGGER.check('couchdb - {}'.format(result[0]), result[1])

    if exceptions:
        raise exceptions[0]

    return clients_from_config


def retry_on_error(exception):
    if isinstance(exception, EXCEPTIONS) and (
            exception.status_code >= 500 or
            exception.status_code in [409, 412, 429]
    ):
        return True
    return False


def get_client_from_resource_type(processing, resource_type):
    """
    :param processing: processing object
    :param resource_type: type of resource to get client for
    :type processing: openregistry.convoy.basic.processing.ProcessingBasic
    :type resource_type: str
    :return: client for passed type of resource
    :rtype: openprocurement_client.clients.APIResourceClient
    """
    client_name = '{}s_client'.format(resource_type)
    client = getattr(processing, client_name)
    return client


def make_contract(auction):
    contract = auction.contracts[-1]
    contract_object = {
        'merchandisingObject': auction.merchandisingObject,
        'contractType': CONTRACT_TYPE
    }

    for key in CONTRACT_REQUIRED_FIELDS:
        contract_object[key] = contract.get(key)

    for key in CONTRACT_NOT_REQUIRED_FIELDS:
        value = contract.get(key, None)
        if value:
            contract_object[key] = value

    return contract_object
