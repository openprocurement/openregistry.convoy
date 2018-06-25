# -*- coding: utf-8 -*-
import logging.config

from retrying import retry

from openprocurement_client.exceptions import (
    ResourceNotFound,
)

from openregistry.convoy.loki.constants import (
    SUCCESSFUL_TERMINAL_STATUSES, UNSUCCESSFUL_TERMINAL_STATUSES
)
from openregistry.convoy.utils import retry_on_error, make_contract

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

        if auction_doc.status in UNSUCCESSFUL_TERMINAL_STATUSES:
            self._switch_auction_status(auction_doc.status, lot.id, auction_doc.id)

        elif auction_doc.status in SUCCESSFUL_TERMINAL_STATUSES:
            contract_data = make_contract(auction_doc)
            contract = self._post_contract({'data': contract_data}, lot.id)
            self._switch_auction_status(auction_doc.status, lot.id, auction_doc.id)
            self.update_lot_contract(lot, contract)

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

    @retry(stop_max_attempt_number=5, retry_on_exception=retry_on_error, wait_fixed=2000)
    def _patch_lot_contract(self, contract_data, lot_id, contract_id):
        self.lots_client.patch_resource_item_subitem(
            resource_item_id=lot_id,
            patch_data={'data': contract_data},
            subitem_name='contracts',
            subitem_id=contract_id
        )
        LOGGER.info('Update lot\'s {} contract data'.format(lot_id))

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
        if 'merchandisingObject' not in auction_doc:
            LOGGER.warning(
                'merchandisingObject is not provided in auction {}'.format(
                    auction_doc.id
                )
            )
            return
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

    @retry(stop_max_attempt_number=5, retry_on_exception=retry_on_error, wait_fixed=2000)
    def _post_contract(self, data, lot_id):
        contract = self.contracts_client.create_contract(data).data
        LOGGER.info("Successfully created contract {} from lot {}".format(contract.id, lot_id))
        return contract

    def update_lot_contract(self, lot, contract):
        contract_id = lot.contracts[0].id
        contract_data = {
            'contractID': contract.contractID,
            'relatedProcessID': contract.id,
        }
        self._patch_lot_contract(contract_data, lot.id, contract_id)
