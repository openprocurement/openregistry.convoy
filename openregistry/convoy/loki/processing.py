# -*- coding: utf-8 -*-
from retrying import retry

from openprocurement_client.exceptions import (
    Forbidden,
    RequestFailed,
    ResourceNotFound,
    UnprocessableEntity,
    Conflict,
    PreconditionFailed,
)

from openregistry.convoy.loki.constants import (
    SUCCESSFUL_TERMINAL_STATUSES, UNSUCCESSFUL_TERMINAL_STATUSES,
    CREATE_CONTRACT_MESSAGE_ID, UPDATE_CONTRACT_MESSAGE_ID, SWITCH_LOT_AUCTION_STATUS_MESSAGE_ID
)
from openregistry.convoy.utils import retry_on_error, make_contract, LOGGER

EXCEPTIONS = (Forbidden, RequestFailed, ResourceNotFound, UnprocessableEntity, PreconditionFailed, Conflict)


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
        if not self.auctions_mapping.has(auction.id):
            self.report_results(auction)

    def report_results(self, auction_doc):
        LOGGER.info('Report auction results {}'.format(auction_doc.id))

        lot_processing = 'merchandisingObject' in auction_doc
        contract_processing = 'contractTerms' in auction_doc

        if lot_processing:

            lot = self._get_lot(auction_doc)
            if not lot:
                return

            lot_auction = self._check_lot_auction(lot, auction_doc)
            if not lot_auction:
                return

        if auction_doc.status in UNSUCCESSFUL_TERMINAL_STATUSES:
            if lot_processing:
                self._switch_auction_status(auction_doc.status, lot.id, lot_auction.id)
            self.auctions_mapping.put(str(auction_doc.id), True)

        elif auction_doc.status in SUCCESSFUL_TERMINAL_STATUSES:
            if contract_processing:
                contract_data = make_contract(auction_doc)
                try:
                    contract_data['transfer_token'] = self._extract_transfer_token(auction_doc['id'])
                except EXCEPTIONS as e:
                    message = 'Server error: {}'.format(e.status_code) if e.status_code >= 500 else e.message
                    LOGGER.error(
                        "Failed to extract transfer token from auction {} ({})".format(auction_doc.id, message)
                    )
                    return
                contract = self._post_contract({'data': contract_data})
            if lot_processing:
                self._switch_auction_status(auction_doc.status, lot.id, lot_auction.id)
            if lot_processing and contract_processing:
                self.update_lot_contract(lot, contract)
            self.auctions_mapping.put(str(auction_doc.id), True)

    @retry(stop_max_attempt_number=5, retry_on_exception=retry_on_error, wait_fixed=2000)
    def _switch_auction_status(self, status, lot_id, auction_id):
        self.lots_client.patch_resource_item_subitem(
            resource_item_id=lot_id,
            patch_data={'data': {'status': status}},
            subitem_name='auctions',
            subitem_id=auction_id
        )
        LOGGER.info(
            'Switch lot\'s {} auction {} to ({}) status'.format(lot_id, auction_id, status),
            extra={
                'MESSAGE_ID': SWITCH_LOT_AUCTION_STATUS_MESSAGE_ID,
                'STATUS': status
            }
        )

    @retry(stop_max_attempt_number=5, retry_on_exception=retry_on_error, wait_fixed=2000)
    def _patch_lot_contract(self, contract_data, lot_id, contract_id):
        self.lots_client.patch_resource_item_subitem(
            resource_item_id=lot_id,
            patch_data={'data': contract_data},
            subitem_name='contracts',
            subitem_id=contract_id
        )
        LOGGER.info(
            'Update lot\'s {} contract data'.format(lot_id),
            extra={
                'MESSAGE_ID': UPDATE_CONTRACT_MESSAGE_ID
            }
        )

    @retry(stop_max_attempt_number=5, retry_on_exception=retry_on_error, wait_fixed=2000)
    def _extract_transfer_token(self, auction_id):
        credentials = self.auctions_client.extract_credentials(resource_item_id=auction_id)
        LOGGER.info("Successfully extracted tranfer_token from auction {})".format(auction_id))
        return credentials['data']['transfer_token']

    def _check_lot_auction(self, lot, auction_doc):
        lot_auction = next((auction for auction in lot.auctions
                            if auction_doc.id == auction.get('relatedProcessID')), None)
        if not lot_auction:
            LOGGER.warning(
                'Auction object {} not found in lot {}'.format(
                    auction_doc.id, lot.id
                )
            )
            return
        if lot_auction['status'] != 'active':
            LOGGER.info('Auction {} results already reported to lot {}'.format(auction_doc.id, lot.id))
            return
        return lot_auction

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

    @retry(stop_max_attempt_number=5, retry_on_exception=retry_on_error, wait_fixed=2000)
    def _post_contract(self, contract_data):
        contract = self.contracts_client.create_contract(contract_data).data
        log_msg = "Successfully created contract {}".format(contract.id)
        if 'merchandisingObject' in contract_data['data']:
            log_msg += " from lot {}".format(contract_data['data']['merchandisingObject'])
        LOGGER.info(
            log_msg,
            extra={
                'MESSAGE_ID': CREATE_CONTRACT_MESSAGE_ID
            }
        )
        return contract

    def update_lot_contract(self, lot, contract):
        contract_id = lot.contracts[0].id
        contract_data = {
            'contractID': contract.contractID,
            'relatedProcessID': contract.id,
            'status': 'active',
        }
        self._patch_lot_contract(contract_data, lot.id, contract_id)
