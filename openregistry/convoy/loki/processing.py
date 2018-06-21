# -*- coding: utf-8 -*-
import logging.config

from retrying import retry

from openprocurement_client.exceptions import (
    ResourceNotFound,
)

from openregistry.convoy.utils import retry_on_error

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

        lot = self._get_lot(auction_doc)
        if not lot:
            return

        lot_auction_is_available = self._check_lot_auction(lot, auction_doc)
        if not lot_auction_is_available:
            return

        self._switch_auction_status(auction_doc.status, lot.id, auction_doc.id)

    @retry(stop_max_attempt_number=5, retry_on_exception=retry_on_error, wait_fixed=2000)
    def _switch_auction_status(self, status, lot_id, auction_id):
        self.lots_client.patch_resource_item_subitem(
            resource_item_id=lot_id,
            patch_data={'data': {'status': status}},
            subitem_name='auctions',
            subitem_id=auction_id
        )
        LOGGER.info('Switch lot\'s {} auction {} to ({}) status'.format(
            lot_id, auction_id, status)
        )

    def _check_lot_auction(self, lot, auction_doc):
        lot_auction = next((auction for auction in lot.auctions
                            if auction.id == lot.relatedProcessID), None)
        if not lot_auction:
            LOGGER.warning(
                'Auction object {} not found in lot {}'.format(
                    lot.relatedProcessID, lot.id
                )
            )
            return
        if lot_auction['status'] != 'active':
            LOGGER.info('Auction {} results already reported to lot {}'.format(
                auction_doc.id, lot.id)
            )
            return
        return True

    def _get_lot(self, auction_doc):
        lot_id = auction_doc.merchandisingObject
        try:
            lot = self.lots_client.get_lot(lot_id).data
        except ResourceNotFound:
            LOGGER.warning(
                'Lot {} not found when report auction {} results'.format(
                    lot_id, auction_doc.id
                )
            )
            return

        LOGGER.info('Received lot {} from CDB'.format(lot_id))
        return lot
