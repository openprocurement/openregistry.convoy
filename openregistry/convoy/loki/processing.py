# -*- coding: utf-8 -*-
import logging.config

from openprocurement_client.exceptions import (
    ResourceNotFound,
)

LOGGER = logging.getLogger('openregistry.convoy.convoy')


class ProcessingLoki(object):

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
        self.report_results(auction)

    def report_results(self, auction_doc):
        LOGGER.info('Report auction results {}'.format(auction_doc.id))

        lot_id = auction_doc.merchandisingObject

        # Get lot
        try:
            lot = self.lots_client.get_lot(lot_id).data
        except ResourceNotFound:
            LOGGER.warning(
                'Lot {} not found when report auction {} results'.format(
                    lot_id, auction_doc.id
                )
            )
            return

        lot_auction = None
        for lot_auction in lot.auctions:
            if lot_auction.id == lot.relatedProcessID:
                break

        if lot_auction['status'] != 'active':
            LOGGER.info('Auction {} results already reported to lot {}'.format(
                auction_doc.id, lot_id)
            )
            return

        LOGGER.info('Received lot {} from CDB'.format(lot_id))

        # Report results
        self.lots_client.patch_resource_item_subitem(
            resource_item_id=lot['id'],
            patch_data={'data': {'status': auction_doc.status}},
            subitem_name='auctions',
            subitem_id=auction_doc.id
        )
        LOGGER.info('Switch lot\'s {} auction {} to ({}) status'.format(
            lot['id'], auction_doc.id, auction_doc.status)
        )
