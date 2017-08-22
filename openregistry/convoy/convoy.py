# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_all()

import logging
import logging.config
import os
import time
import argparse
from yaml import load
from uuid import uuid4
from openprocurement_client.client import TendersClient as APIClient
from openregistry.convoy.utils import continuous_changes_feed, push_filter_doc
from openprocurement_client.document_service_client import (
    DocumentServiceClient as DSClient
)
from openprocurement_client.registry_client import LotsClient, AssetsClient
from openprocurement_client.exceptions import (
    InvalidResponse,
    Forbidden,
    RequestFailed
)
from gevent.queue import Queue, Empty
from gevent import spawn, sleep
from couchdb import Server, Session

LOGGER = logging.getLogger(__name__)


class Convoy(object):
    """ Convoy """

    def __init__(self, convoy_conf):
        LOGGER.info('Init Convoy...')
        self.convoy_conf = convoy_conf
        self.stop_transmitting = False
        self.transmitter_timeout = self.convoy_conf.get('transmitter_timeout',
                                                        10)
        self.documents_transfer_queue = Queue()
        self.timeout = self.convoy_conf.get('timeout', 10)
        self.ds_client = DSClient(**self.convoy_conf['cdb_ds'])
        self.api_client = APIClient(**self.convoy_conf['cdb'])
        self.lots_client = LotsClient(**self.convoy_conf['lots_db'])
        self.assets_client = AssetsClient(**self.convoy_conf['assets_db'])
        user = self.convoy_conf['couchdb'].get('user', '')
        password = self.convoy_conf['couchdb'].get('password', '')
        if user and password:
            self.db = Server(
                "http://{user}:{password}@{host}:{port}".format(
                    **self.convoy_conf['couchdb']),
                session=Session(retry_delays=range(10)))[
                    self.convoy_conf['couchdb']['db']]
        else:
            self.db = Server(
                "http://{host}:{port}".format(
                    **self.convoy_conf['couchdb']),
                session=Session(retry_delays=range(10)))[
                    self.convoy_conf['couchdb']['db']]
        push_filter_doc(self.db)
        LOGGER.info('Added filters doc to db.')

    def _create_items_from_assets(self, assets_ids):
        items = []
        documents = []
        keys = ['classification', 'additionalClassifications', 'address',
                'unit', 'quantity', 'location', 'id']
        document_keys = ['hash', 'description', 'title', 'url', 'format',
                         'documentType']
        for asset_id in assets_ids:
            asset = self.assets_client.get_asset(asset_id)
            LOGGER.info('Received asset {} with status {}'.format(
                asset.data.id, asset.data.status))
            item = {k: asset.data[k] for k in keys if k in asset.data}
            item['description'] = asset.data.title
            # TODO: Fix CAV <--> CPV
            item['classification']['scheme'] = 'CAV'
            items.append(item)
            if 'documents' not in asset.data:
                LOGGER.debug('Asset {} without documents'.format(asset_id))
                continue
            for doc in asset.data.documents:
                item_document = {
                    k: doc[k] for k in document_keys if k in doc
                }
                registered_doc = self.ds_client.register_document_upload(
                    doc['hash'])
                LOGGER.info('Registered document upload for item {} with hash'
                            ' {}'.format(asset_id, doc['hash']))
                transfer_item = {
                    'get_url': doc['url'],
                    'upload_url': registered_doc.upload_url
                }
                self.documents_transfer_queue.put(transfer_item)
                item_document['url'] = registered_doc.data.url
                item_document['documentOf'] = 'item'
                item_document['relatedItem'] = asset.data.id
                documents.append(item_document)

        return items, documents

    def prepare_auction_data(self, auction_doc):
        lot_id = auction_doc.get('lotID')

        # Get lot
        lot = self.lots_client.get_lot(lot_id)
        LOGGER.info('Received lot {} from CDB'.format(lot_id))
        if lot.data.status != u'active.salable':
            # lot['data']['status'] = 'active.salable'
            # self.lots_client.patch_resource_item(lot)
            LOGGER.warning(
                'Lot status \'{}\' not equal \'active.salable\''.format(
                    lot.data.status),
                extra={'MESSAGE_ID': 'invalid_lot_status'})
            return

        # Lock lot
        lot.data['status'] = 'active.awaiting'
        LOGGER.info('Lock lot {}'.format(lot.data.id),
                    extra={'MESSAGE_ID': 'lock_lot'})
        self.lots_client.patch_resource_item(lot)

        # Convert assets to items
        items, documents = self._create_items_from_assets(lot.data.assets)

        api_auction_doc = self.api_client.get_resource_item(auction_doc['id'])
        LOGGER.info('Received auction {} from CDB'.format(auction_doc['id']))
        api_auction_doc.data['items'] = items

        # Add items to CDB
        self.api_client.patch_resource_item(api_auction_doc)
        LOGGER.info('Added {} items to auction {}'.format(len(items),
                                                          auction_doc['id']))

        # Add documents to CDB
        for document in documents:
            self.api_client.create_thin_document(api_auction_doc, document)
            LOGGER.info(
                'Added document with hash {} to auction id: {} item id:'
                ' {}'.format(document['hash'], auction_doc['id'],
                             document['relatedItem'])
            )

        lot['data']['status'] = 'active.auction'
        self.lots_client.patch_resource_item(lot)
        LOGGER.info('Switch lot {} to \'{}\''.format(
            lot['data']['id'], lot['data']['status']))
        return api_auction_doc

    def switch_auction_to_active_tendering(self, auction):
        auction['data']['status'] = 'active.tendering'
        self.api_client.patch_resource_item(auction)
        LOGGER.info('Switch auction {} to status {}'.format(
            auction['data']['id'], auction['data']['status']))

    def file_bridge(self):
        while not self.stop_transmitting:
            try:
                transfer_item = self.documents_transfer_queue.get(timeout=2)
                try:
                    file_, _ = self.api_client.get_file(
                        transfer_item['get_url'])
                    LOGGER.debug('Received document file from asset DS')
                    # TODO: Fill headers valid data if needed
                    headers = {}
                    self.ds_client.document_upload_not_register(file_, headers)
                    LOGGER.debug('Uploaded document file to auction DS')
                except:
                    LOGGER.error('While receiving or uploading document '
                                 'something went wrong :(')
                    self.documents_transfer_queue.put(transfer_item)
                    sleep(1)
                    continue
            except Empty:
                sleep(self.transmitter_timeout)

    def run(self):
        self.transmitter = spawn(self.file_bridge)
        sleep(1)
        for auction_info in continuous_changes_feed(self.db):
            LOGGER.info('Received auction {}'.format(repr(auction_info)))
            auction_doc = self.prepare_auction_data(auction_info)
            if not auction_doc:
                continue
            self.switch_auction_to_active_tendering(auction_doc)


def main():
    parser = argparse.ArgumentParser(description='--- OpenRegistry Convoy ---')
    parser.add_argument('config', type=str, help='Path to configuration file')
    params = parser.parse_args()
    if os.path.isfile(params.config):
        with open(params.config) as config_file_obj:
            config = load(config_file_obj.read())
            logging.config.dictConfig(config)
            Convoy(config).run()


###############################################################################

if __name__ == "__main__":  # pragma: no cover
    main()
