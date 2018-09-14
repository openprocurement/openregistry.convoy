# -*- coding: utf-8 -*-
from retrying import retry

from openprocurement_client.constants import DOCUMENTS
from openprocurement_client.exceptions import (
    ResourceNotFound,
)

from openregistry.convoy.utils import retry_on_error, get_client_from_resource_type, LOGGER
from openregistry.convoy.basic.constants import (
    AUCTION_SWITCH_STATUS_MESSAGE_ID,
    LOT_SWITCH_STATUS_MESSAGE_ID
)


class ProcessingBasic(object):

    def __init__(self, config, clients, keys, document_keys, documents_transfer_queue):
        """
        Args:
            config: dictionary with configuration data
        """
        self.config = config
        self.allowed_auctions_types = []
        self.handled_lot_types = []
        self.allowed_pmt = []
        self.keys = keys
        self.document_keys = document_keys
        self.documents_transfer_queue = documents_transfer_queue

        self._register_allowed_auctions()
        self._register_handled_lot_types()

        for key, item in clients.items():
            setattr(self, key, item)

    def _register_allowed_auctions(self):
        for _, auction_aliases in self.config.get('auctions', {}).items():
            self.allowed_auctions_types += auction_aliases

    def _register_handled_lot_types(self):
        self.handled_lot_types += self.config.get('aliases', [])

    def process_auction(self, auction):
        if auction['status'] == 'pending.verification':
            self.prepare_auction(auction)
        else:
            self.report_results(auction)

    def prepare_auction(self, auction_doc):
        LOGGER.info('Prepare auction {}'.format(auction_doc.id))
        lot = self._receive_lot(auction_doc)
        if lot:
            auction_formed = self._form_auction(lot, auction_doc)
            if auction_formed:
                self._activate_auction(lot, auction_doc)

    def report_results(self, auction_doc):
        LOGGER.info('Report auction results {}'.format(auction_doc.id))

        lot_id = auction_doc.merchandisingObject

        # Get lot
        try:
            lot = self.lots_client.get_lot(lot_id).data
        except ResourceNotFound:
            LOGGER.warning('Lot {} not found when report auction {} results'.format(lot_id, auction_doc.id))
            return

        if lot.status != 'active.auction':
            LOGGER.info('Auction {} results already reported to lot {}'.format(auction_doc.id, lot_id))
            return

        LOGGER.info('Received lot {} from CDB'.format(lot_id))

        if auction_doc.status == 'complete':
            next_lot_status = 'pending.sold'
        else:
            next_lot_status = 'active.salable'

        # Report results
        try:
            self.switch_lot_status(lot['id'], next_lot_status)
        except Exception as e:
            LOGGER.error('Failed update lot info {}. {}'.format(lot_id, e.message))

    def _receive_lot(self, auction_doc):
        lot_id = auction_doc.merchandisingObject

        # Get lot
        try:
            lot = self.lots_client.get_lot(lot_id).data
        except ResourceNotFound:
            self.invalidate_auction(auction_doc.id)
            return
        LOGGER.info('Received lot {} from CDB'.format(lot_id))
        is_lot_unusable = bool(
            (lot.status == u'active.awaiting' and auction_doc.id != lot.auctions[-1]) or
            lot.status not in [u'active.salable', u'active.awaiting', u'active.auction']
        )
        if is_lot_unusable:
            # lot['status'] = 'active.salable'
            # self.lots_client.patch_resource_item(lot)
            LOGGER.warning(
                'Lot status \'{}\' not equal \'active.salable\''.format(
                    lot.status),
                extra={'MESSAGE_ID': 'invalid_lot_status'})
            self.invalidate_auction(auction_doc.id)
            return
        elif lot.status == u'active.auction' and auction_doc.id == lot.auctions[-1]:
            # Switch auction
            self.switch_auction_status(auction_doc['id'], 'active.tendering')
            return
        elif lot.status == u'active.awaiting' and auction_doc.id == lot.auctions[-1]:
            return lot

        # Lock lot
        auctions_list = lot.get('auctions', [])
        auctions_list.append(auction_doc.id)
        lot_patch_data = {'data': {'status': 'active.awaiting', 'auctions': auctions_list}}
        self._patch_resource_item(
            self.lots_client, lot.id, lot_patch_data,
            'Lock lot {}'.format(lot.id), {'MESSAGE_ID': 'lock_lot'}
        )
        return lot

    def _form_auction(self, lot, auction_doc):
        # Convert assets to items
        items, documents = self._create_items_from_assets(lot.assets)

        if not items:
            self.switch_lot_status(lot.id, 'active.salable')
            self.invalidate_auction(auction_doc.id)
            return False

        api_auction_doc = self.auctions_client.get_resource_item(auction_doc['id']).data
        LOGGER.info('Received auction {} from CDB'.format(auction_doc['id']))

        # Add items to CDB
        patch_data = {'data': {'items': items, 'dgfID': lot.lotIdentifier}}
        message = 'Auction: {} was formed from lot: {}'.format(auction_doc['id'], lot.id)
        self._patch_resource_item(
            self.auctions_client, api_auction_doc.id, patch_data, message
        )

        # Add documents to CDB
        for document in documents:
            self.auctions_client.create_resource_item_subitem(
                api_auction_doc.id, {'data': document}, DOCUMENTS
            )
            LOGGER.info(
                'Added document with hash {} to auction id: {} item id:'
                ' {} in CDB'.format(document['hash'],
                                    auction_doc['id'],
                                    document['relatedItem'])
            )
        return True

    def _activate_auction(self, lot, auction_doc):
        # Switch lot
        self.switch_lot_status(lot['id'], 'active.auction')

        # Switch auction
        self.switch_auction_status(auction_doc['id'], 'active.tendering')

    def invalidate_auction(self, auction_id):
        self.switch_auction_status(auction_id, 'invalid')

    def switch_auction_status(self, auction_id, status):
        self._switch_resource_status('auction', auction_id, status)

    def switch_lot_status(self, lot_id, status):
        self._switch_resource_status('lot', lot_id, status)

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

            # Get documents from asset
            for doc in self._get_documents(asset):
                documents.append(doc)

            # Get items and items documents from complex asset
            for item in asset.get('items', []):
                items.append(item)
                for doc in self._get_documents(item):
                    documents.append(doc)

        return items, documents

    def _get_documents(self, item):
        if not hasattr(self.auctions_client, 'ds_client'):
            return []
        documents = []
        for doc in item.get('documents', []):
            item_document = {
                k: doc[k] for k in self.document_keys if k in doc
            }
            try:
                registered_doc = self.auctions_client.ds_client.register_document_upload(doc['hash'])
                LOGGER.info('Registered document upload for item {} with hash'
                            ' {}'.format(item.id, doc['hash']))
            except:
                LOGGER.error('While registering document upload '
                             'something went wrong :(')
                continue
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

    def _switch_resource_status(self, resource_type, resource_id, status):
        log_extra = {
            'MESSAGE_ID': '',
            'STATUS': ''
        }
        if resource_type == 'lot':
            log_extra['MESSAGE_ID'] = LOT_SWITCH_STATUS_MESSAGE_ID
            log_extra['STATUS'] = status
        elif resource_type == 'auction':
            log_extra['MESSAGE_ID'] = AUCTION_SWITCH_STATUS_MESSAGE_ID
            log_extra['STATUS'] = status

        message = 'Switch {} {} status to {} '.format(resource_type, resource_id, status)
        client = get_client_from_resource_type(self, resource_type)
        patch_data = {'data': {'status': status}}

        resource = self._patch_resource_item(client, resource_id, patch_data, message, log_extra)
        return resource

    @retry(stop_max_attempt_number=5, retry_on_exception=retry_on_error, wait_fixed=2000)
    def _patch_resource_item(self, client, resource_id, patch_data, message, extra=None):
        resource = client.patch_resource_item(resource_id, patch_data)
        LOGGER.info(message, extra=extra)
        return resource
