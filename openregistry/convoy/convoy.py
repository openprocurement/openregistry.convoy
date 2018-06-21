# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_all()

import signal
import logging
import logging.config
import os

import argparse
from gevent.queue import Queue, Empty
from gevent import spawn, sleep
from yaml import load

from openregistry.convoy.utils import (
    continuous_changes_feed, init_clients, push_filter_doc
)
from openregistry.convoy.constants import DEFAULTS, DOCUMENT_KEYS, KEYS
from openregistry.convoy.loki.processing import ProcessingLoki
from openregistry.convoy.basic.processing import ProcessingBasic


LOGGER = logging.getLogger(__name__)


class GracefulKiller(object):
    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self,signum, frame):
        self.kill_now = True


class Convoy(object):
    """
        Convoy worker object.
        Worker that get assets and transform them to item's, than
        he patch lot and auction to specified statuses
    """
    def __init__(self, convoy_conf):
        LOGGER.info('Init Convoy...')
        self.auction_type_processing_configurator = {}
        self.auction_types_for_filter = {}
        self.convoy_conf = convoy_conf
        self.killer = GracefulKiller()

        self.stop_transmitting = False

        self.transmitter_timeout = self.convoy_conf.get('transmitter_timeout',
                                                        10)

        created_clients = init_clients(convoy_conf)

        for key, item in created_clients.items():
            setattr(self, key, item)
        self.documents_transfer_queue = Queue()
        self.timeout = self.convoy_conf.get('timeout', 10)
        self.keys = KEYS
        self.document_keys = DOCUMENT_KEYS

        if convoy_conf['lots'].get('loki'):

            process_loki = ProcessingLoki(
                convoy_conf['lots']['loki'], created_clients,
                self.keys, self.document_keys, self.documents_transfer_queue
            )
            self._register_aliases(process_loki, 'loki')
        if convoy_conf['lots'].get('basic'):
            process_basic = ProcessingBasic(
                convoy_conf['lots']['basic'], created_clients,
                self.keys, self.document_keys, self.documents_transfer_queue
            )
            self._register_aliases(process_basic, 'basic')

        push_filter_doc(self.db, self.auction_types_for_filter)

    def _register_aliases(self, processing, lot_type):
        self.auction_types_for_filter[lot_type] = []
        for auction_type in processing.allowed_auctions_types:
            self.auction_type_processing_configurator[auction_type] = processing
            self.auction_types_for_filter[lot_type].append(auction_type)

    def file_bridge(self):
        while not self.stop_transmitting:
            try:
                transfer_item = self.documents_transfer_queue.get(timeout=2)
                try:
                    file_, _ = self.auctions_client.get_file(
                        transfer_item['get_url'])
                    LOGGER.debug('Received document file from asset DS')
                    # TODO: Fill headers valid data if needed
                    headers = {}
                    self.auctions_client.ds_client.document_upload_not_register(
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
        LOGGER.info('Getting auctions')
        for auction in continuous_changes_feed(self.db, self.timeout):
            LOGGER.info('Received auction {}'.format(repr(auction)))

            if auction['procurementMethodType'] not in self.auction_type_processing_configurator:
                LOGGER.warning(
                    'Such procurementMethodType %s is not supported by this'
                    ' convoy configuration' % auction['procurementMethodType']
                )
                continue

            self.auction_type_processing_configurator[auction['procurementMethodType']].process_auction(auction)

            if self.killer.kill_now:
                break


def main():
    parser = argparse.ArgumentParser(description='--- OpenRegistry Convoy ---')
    parser.add_argument('config', type=str, help='Path to configuration file')
    parser.add_argument('-t', dest='check', action='store_const',
                        const=True, default=False,
                        help='Clients check only')
    params = parser.parse_args()
    config = {}
    if os.path.isfile(params.config):
        with open(params.config) as config_file_obj:
            config = load(config_file_obj.read())
        logging.config.dictConfig(config)
    DEFAULTS.update(config)
    convoy = Convoy(DEFAULTS)
    if params.check:
        exit()
    convoy.run()


###############################################################################

if __name__ == "__main__":  # pragma: no cover
    main()
