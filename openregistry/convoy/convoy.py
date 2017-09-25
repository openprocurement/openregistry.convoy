# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_all()

import logging
import logging.config
import os
import argparse
from yaml import load
from openprocurement_client.clients import APIResourceClient as APIClient
from openprocurement_client.exceptions import ResourceNotFound
from openprocurement_client.constants import DOCUMENTS
from openprocurement_client.resources.assets import AssetsClient
from openprocurement_client.resources.lots import LotsClient
from openregistry.convoy.utils import continuous_changes_feed, push_filter_doc

from gevent.queue import Queue, Empty
from gevent import spawn, sleep
from couchdb import Server, Session

LOGGER = logging.getLogger(__name__)


class Convoy(object):
    """
        Convoy worker object.
        Worker that get assets and transform them to item's, than
        he patch lot and auction to specified statuses
    """
    def __init__(self, convoy_conf):
        LOGGER.info('Init Convoy...')
        self.convoy_conf = convoy_conf
        self.stop_transmitting = False
        self.transmitter_timeout = self.convoy_conf.get('transmitter_timeout',
                                                        10)
        self.documents_transfer_queue = Queue()
        self.timeout = self.convoy_conf.get('timeout', 10)
        self.api_client = APIClient(**self.convoy_conf['cdb'])
        self.lots_client = LotsClient(**self.convoy_conf['lots_db'])
        self.assets_client = AssetsClient(**self.convoy_conf['assets_db'])
        self.keys = ['classification', 'additionalClassifications', 'address',
                'unit', 'quantity', 'location', 'id']
        self.document_keys = ['hash', 'description', 'title', 'url', 'format',
                         'documentType']
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

    def _get_documents(self, item):
        documents = []
        for doc in item.documents:
            item_document = {
                k: doc[k] for k in self.document_keys if k in doc
            }
            registered_doc = self.api_client.ds_client.register_document_upload(doc['hash'])
            LOGGER.info('Registered document upload for item {} with hash'
                        ' {}'.format(item.id, doc['hash']))
            transfer_item = {
                'get_url': doc.url,
                'upload_url': registered_doc['upload_url']
            }
            self.documents_transfer_queue.put(transfer_item)
            item_document['url'] = registered_doc['data']['url']
            item_document['documentOf'] = 'item'
            item_document['relatedItem'] = item.id
            documents.append(item_document)
        return documents

    def _create_items_from_assets(self, assets_ids):
        items = []
        documents = []
        for index, asset_id in enumerate(assets_ids):
            asset = self.assets_client.get_asset(asset_id).data
            LOGGER.info('Received asset {} with status {}'.format(
                asset.id, asset.status))
            
            # Convert asset to item
            item = {k: asset[k] for k in self.keys if k in asset}
            item['description'] = asset.title
            items.append(item)
            
            # Get items and items documents from complex asset
            for item in asset.get('items', []):
                items.append(item)
                for doc in self._get_documents(item):
                    documents.append(doc)

            # Get documents from asset
            if 'documents' not in asset:
                LOGGER.debug('Asset {} without documents'.format(asset_id))
                continue
            documents = self._get_documents(asset)  # from asset

        return items, documents

    def invalidate_auction(self, auction_id):
        self.api_client.patch_resource_item(
            auction_id, {"data": {"status": "invalid"}}
        )
        LOGGER.info('Switch auction {} status to invalid'.format(auction_id))

    def prepare_auction(self, auction_doc):
        LOGGER.info('Prepare auction {}'.format(auction_doc.id))

        lot_id = auction_doc.merchandisingObject

        # Get lot
        try:
            lot = self.lots_client.get_lot(lot_id).data
        except ResourceNotFound:
            self.invalidate_auction(auction_doc.id)
            return

        LOGGER.info('Received lot {} from CDB'.format(lot_id))
        if lot.status != u'active.salable':
            # lot['status'] = 'active.salable'
            # self.lots_client.patch_resource_item(lot)
            LOGGER.warning(
                'Lot status \'{}\' not equal \'active.salable\''.format(
                    lot.status),
                extra={'MESSAGE_ID': 'invalid_lot_status'})
            self.invalidate_auction(auction_doc.id)
            return

        # Lock lot
        auctions_list = lot.get('auctions', [])
        auctions_list.append(auction_doc.id)
        lot_patch_data = {'data': {'status': 'active.awaiting', 'auctions': auctions_list}}
        self.lots_client.patch_resource_item(lot.id, lot_patch_data)
        LOGGER.info('Lock lot {}'.format(lot.id),
                    extra={'MESSAGE_ID': 'lock_lot'})

        # Convert assets to items
        items, documents = self._create_items_from_assets(lot.assets)

        if not items:
            self.lots_client.patch_resource_item(lot.id, {'data': {'status': 'active.salable'}})
            LOGGER.info('Switch lot {} status to active.salable'.format(lot.id))
            self.invalidate_auction(auction_doc.id)
            return

        api_auction_doc = self.api_client.get_resource_item(auction_doc['id']).data
        LOGGER.info('Received auction {} from CDB'.format(auction_doc['id']))

        # Add items to CDB
        auction_patch_data = {'data': {'items': items}}
        self.api_client.patch_resource_item(
            api_auction_doc.id, auction_patch_data
        )
        LOGGER.info('Added {} items to auction {}'.format(len(items),
                                                          auction_doc['id']))

        # Add documents to CDB
        for document in documents:
            self.api_client.create_resource_item_subitem(
                api_auction_doc.id, {'data': document}, DOCUMENTS
            )
            LOGGER.info(
                'Added document with hash {} to auction id: {} item id:'
                ' {} in CDB'.format(document['hash'],
                                    auction_doc['id'],
                                    document['relatedItem'])
            )

        # Switch lot
        self.lots_client.patch_resource_item(lot['id'], {'data': {'status': 'active.auction'}})
        LOGGER.info('Switch lot {} to (active.auction) status'.format(lot['id']))

        # Switch auction
        self.api_client.patch_resource_item(auction_doc['id'], {'data': {'status': 'active.tendering'}})
        LOGGER.info('Switch auction {} to (active.tendering) status'.format(auction_doc['id']))

    def report_results(self, auction_doc):
        LOGGER.info('Report auction results {}'.format(auction_doc.id))

        lot_id = auction_doc.merchandisingObject

        # Get lot
        try:
            lot = self.lots_client.get_lot(lot_id).data
        except ResourceNotFound:
            LOGGER.warning('Lot {} not found when report auction {} results'.format(lot_id, auction_doc.id))
            return

        if lot.status != 'active.auction' and lot.auctions[-1] == auction_doc.id:
            LOGGER.info('Auction {} results already reported to lot {}'.format(auction_doc.id, lot_id))
            return

        LOGGER.info('Received lot {} from CDB'.format(lot_id))

        if auction_doc.status == 'complete':
            next_lot_status = 'sold'
        else:
            next_lot_status = 'active.salable'

        # Report results
        self.lots_client.patch_resource_item(lot['id'], {'data': {'status': next_lot_status}})
        LOGGER.info('Switch lot {} to ({}) status'.format(lot['id'], next_lot_status))

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
                    self.api_client.ds_client.document_upload_not_register(
                        file_, headers
                    )
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
            if auction_info['status'] == 'pending.verification':
                self.prepare_auction(auction_info)
            else:
                self.report_results(auction_info)


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
