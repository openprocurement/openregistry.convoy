# -*- coding: utf-8 -*-
from gevent import monkey
from openregistry.convoy.tests.test_utils import AlmostAlwaysTrue
from openregistry.convoy.utils import make_contract

monkey.patch_all()

import unittest
import json
import mock
import os
from copy import deepcopy
from random import choice
from yaml import safe_load as load
from gevent.queue import Queue
from munch import munchify, Munch
from couchdb import Server, Session, Database
from lazydb import Db
from openprocurement_client.exceptions import ResourceNotFound
from openprocurement_client.resources.assets import AssetsClient
from openprocurement_client.resources.lots import LotsClient
from openprocurement_client.clients import APIResourceClient
from openregistry.convoy.convoy import Convoy, main as convoy_main
from openregistry.convoy.constants import DEFAULTS
from uuid import uuid4

# Absolute path to file, dropping 'openregistry/convoy/tests' part
# os.getcwd() is not suitable for run_test.py script
ROOT = '/'.join(os.path.dirname(__file__).split('/')[:-3])



class MockedArgumentParser(mock.MagicMock):

    def __init__(self, description):
        super(MockedArgumentParser, self).__init__()
        self.description = description

    def add_argument(self, *args, **kwargs):
        for key, item in kwargs.items():
            setattr(self, key, item)

    def parse_args(self):
        return munchify({
            'config': '{}/{}'.format(ROOT, '/convoy.yaml'),
            'check': False,
            'auction_id': None
        })


class TestConvoySuite(unittest.TestCase):
    """ TestCase Convoy functionality """

    def setUp(self):
        self.test_files_path = '{}/{}'.format(os.path.dirname(__file__),
                                              'files/')
        with open('{}/convoy.yaml'.format(ROOT)) as config_file_obj:
            self.config = load(config_file_obj.read())
        user = self.config['db'].get('name', '')
        password = self.config['db'].get('password', '')
        if user and password:
            self.server = Server(
                "http://{user}:{password}@{host}:{port}".format(
                    **self.config['couchdb']),
                session=Session(retry_delays=range(10)))
        else:
            self.server = Server(
                "http://{host}:{port}".format(
                    **self.config['db']),
                session=Session(retry_delays=range(10)))
        if self.config['db']['name'] not in self.server:
            self.server.create(self.config['db']['name'])

    def tearDown(self):
        del self.server[self.config['db']['name']]
        test_mapping_name = self.config.get('auctions_mapping', {}).get('name', 'auctions_mapping')
        Db(test_mapping_name).destroy(test_mapping_name)

    @mock.patch('requests.Response.raise_for_status')
    @mock.patch('requests.Session.request')
    def test_init(self, mock_raise, mock_request):
        convoy = Convoy(self.config)
        self.assertEqual(convoy.stop_transmitting, False)
        self.assertEqual(convoy.transmitter_timeout,
                         self.config['transmitter_timeout'])
        self.assertEqual(convoy.timeout, self.config['timeout'])
        self.assertIsInstance(convoy.documents_transfer_queue, Queue)
        self.assertIsInstance(convoy.auctions_client, APIResourceClient)
        self.assertIsInstance(convoy.lots_client, LotsClient)
        self.assertIsInstance(convoy.assets_client, AssetsClient)
        self.assertIsInstance(convoy.db, Database)
        self.assertEqual(convoy.db.name, self.config['db']['name'])

        convoy = Convoy(DEFAULTS)
        self.assertEqual(convoy.stop_transmitting, False)
        self.assertEqual(convoy.transmitter_timeout,
                         DEFAULTS['transmitter_timeout'])
        self.assertEqual(convoy.timeout, DEFAULTS['timeout'])
        self.assertIsInstance(convoy.documents_transfer_queue, Queue)
        self.assertIsInstance(convoy.auctions_client, APIResourceClient)
        self.assertIsInstance(convoy.lots_client, LotsClient)
        self.assertIsInstance(convoy.assets_client, AssetsClient)
        self.assertIsInstance(convoy.db, Database)
        self.assertEqual(convoy.db.name, DEFAULTS['db']['name'])

    def fake_response(self):
        return None

    @mock.patch('requests.Response.raise_for_status')
    @mock.patch('requests.Session.request')
    def test__create_items_from_assets(self, mock_raise, mock_request):
        items_keys = ['classification', 'additionalClassifications', 'address',
                      'unit', 'quantity', 'location', 'id']
        documents_keys = ['hash', 'description', 'title', 'format',
                          'documentType']
        with open('{}/asset.json'.format(self.test_files_path), 'r') as af:
            asset_dict = json.loads(af.read())
        with open('{}/document.json'.format(self.test_files_path), 'r') as df:
            document_dict = json.loads(df.read())
        with open('{}/register_response.json'.format(
                self.test_files_path), 'r') as rf:
            register_response_dict = json.loads(rf.read())

        # Prepare mocked clients
        mock_rc = mock.MagicMock()
        mock_rc.get_asset.return_value = munchify(asset_dict)
        mock_rc.ds_client.register_document_upload.return_value = \
            munchify(register_response_dict)
        asset_ids = ['580d38b347134ac6b0ee3f04e34b9770']
        convoy = Convoy(self.config)
        basic_processing = convoy.auction_type_processing_configurator['rubble']
        convoy.assets_client = basic_processing.assets_client = mock_rc
        convoy.auctions_client = basic_processing.auctions_client = mock_rc
        items, documents = basic_processing._create_items_from_assets(asset_ids)
        self.assertEqual(convoy.documents_transfer_queue.qsize(), 2)
        transfer_item = convoy.documents_transfer_queue.get()
        self.assertEqual(transfer_item['get_url'],
                         document_dict['data']['url'])
        self.assertNotEqual(transfer_item['get_url'],
                            transfer_item['upload_url'])
        self.assertEqual(len(items), 2)
        self.assertEqual(len(documents), 2)
        for k in items_keys:
            self.assertEqual(asset_dict['data'].get(k), items[0].get(k))
        for k in documents_keys:
            self.assertEqual(documents[0].get(k),
                             asset_dict['data']['documents'][0].get(k))
        self.assertNotEqual(documents[0]['url'],
                            asset_dict['data']['documents'][0]['url'])
        self.assertEqual(documents[0]['url'],
                         register_response_dict['data']['url'])

        # Test with asset without documents
        del asset_dict['data']['documents']
        del asset_dict['data']['items'][0]['documents']
        mock_rc.get_asset.return_value = munchify(asset_dict)
        items, documents = basic_processing._create_items_from_assets(asset_ids)
        self.assertEqual(len(items), 2)
        self.assertEqual(len(documents), 0)
        self.assertEqual(convoy.documents_transfer_queue.qsize(), 1)

    @mock.patch('requests.Response.raise_for_status')
    @mock.patch('requests.Session.request')
    def test_prepare_auction(self, mock_raise, mock_request):
        a_doc = Munch({
            'id': uuid4().hex,  # this is auction id
            'merchandisingObject': uuid4().hex
        })
        api_auction_doc = {
            'data': {
                'id': a_doc['id'],
                'status': 'pending.verifcation',
                'merchandisingObject': a_doc['merchandisingObject']
            }
        }
        lc = mock.MagicMock()
        lc.get_lot.return_value = munchify({
            'data': {
                'id': a_doc['merchandisingObject'],
                'lotIdentifier': u'Q81318b19827',
                'status': u'verification',
                'assets': ['580d38b347134ac6b0ee3f04e34b9770']
            }
        })
        auctions_client = mock.MagicMock()
        auctions_client.get_resource_item.return_value = munchify(api_auction_doc)
        convoy = Convoy(self.config)
        basic_processing = convoy.auction_type_processing_configurator['rubble']
        convoy.auctions_client = basic_processing.auctions_client = auctions_client
        convoy.lots_client = basic_processing.lots_client = lc
        auction_doc = basic_processing.prepare_auction(a_doc)
        self.assertEqual(None, auction_doc)
        convoy.lots_client.get_lot.assert_called_with(
            a_doc['merchandisingObject'])

        # Preparing for test with valid status and documents
        with open('{}/asset.json'.format(self.test_files_path), 'r') as af:
            asset_dict = json.loads(af.read())
        with open('{}/register_response.json'.format(
                self.test_files_path), 'r') as rf:
            register_response_dict = json.loads(rf.read())

        # Prepare mocked clients
        mock_rc = mock.MagicMock()
        mock_rc.get_asset.return_value = munchify(asset_dict)
        mock_rc.ds_client.register_document_upload.return_value = \
            munchify(register_response_dict)
        asset_ids = ['580d38b347134ac6b0ee3f04e34b9770']

        convoy.assets_client = basic_processing.assets_client = mock_rc
        lc.get_lot.return_value = munchify({
            'data': {
                'id': a_doc['merchandisingObject'],
                'lotIdentifier': u'Q81318b19827',
                'status': u'active.salable',
                'assets': ['580d38b347134ac6b0ee3f04e34b9770']
            }
        })
        items, documents = basic_processing._create_items_from_assets(asset_ids)
        basic_processing._create_items_from_assets = mock.MagicMock(return_value=(
            items, documents))

        # Needed to call mock function before prepare_auction, to
        # check if parameted of this call and call from prepare_auction is equal
        expected = {'data': {
            'items': items,
            'dgfID': u'Q81318b19827'
            }
        }
        # convoy.api_client.patch_resource_item(expected)

        # convoy.prepare_auction(a_doc)
        lot = basic_processing._receive_lot(a_doc)
        basic_processing._form_auction(lot, a_doc)
        convoy.auctions_client.patch_resource_item.assert_called_with(a_doc['id'], expected)
        basic_processing._activate_auction(lot, a_doc)
        self.assertEqual(convoy.documents_transfer_queue.qsize(), 2)
        convoy.lots_client.get_lot.assert_called_with(
            a_doc['merchandisingObject'])
        convoy.lots_client.patch_resource_item.assert_called_with(
            a_doc['merchandisingObject'],
            {
                'data': {
                    'status': 'active.auction'
                }
            }
        )
        basic_processing._create_items_from_assets.assert_called_with(asset_ids)
        patched_api_auction_doc = {'data': {'status': 'active.tendering'}}
        convoy.auctions_client.get_resource_item.assert_called_with(a_doc['id'])
        convoy.auctions_client.patch_resource_item.assert_called_with(
            api_auction_doc['data']['id'],
            patched_api_auction_doc)
        convoy.auctions_client.create_resource_item_subitem.assert_called_with(
            a_doc['id'], {'data': documents[1]}, 'documents')

        # convoy.prepare_auction(a_doc) with active.awaiting lot
        lc.get_lot.return_value = munchify({
            'data': {
                'id': a_doc['merchandisingObject'],
                'lotIdentifier': u'Q81318b19827',
                'status': u'active.awaiting',
                'assets': ['580d38b347134ac6b0ee3f04e34b9770'],
                'auctions': [a_doc['id']]
            }
        })
        items, documents = basic_processing._create_items_from_assets(asset_ids)
        basic_processing._create_items_from_assets = mock.MagicMock(return_value=(
            items, documents))

        lot = basic_processing._receive_lot(a_doc)
        self.assertEqual(len(lot.auctions), 1)
        basic_processing._form_auction(lot, a_doc)
        convoy.auctions_client.patch_resource_item.assert_called_with(a_doc['id'], expected)
        basic_processing._activate_auction(lot, a_doc)
        self.assertEqual(convoy.documents_transfer_queue.qsize(), 2)
        convoy.lots_client.get_lot.assert_called_with(
            a_doc['merchandisingObject'])
        convoy.lots_client.patch_resource_item.assert_called_with(
            a_doc['merchandisingObject'],
            {
                'data': {
                    'status': 'active.auction'
                }
            }
        )
        basic_processing._create_items_from_assets.assert_called_with(asset_ids)
        convoy.auctions_client.get_resource_item.assert_called_with(a_doc['id'])
        convoy.auctions_client.patch_resource_item.assert_called_with(
            api_auction_doc['data']['id'],
            patched_api_auction_doc)
        convoy.auctions_client.create_resource_item_subitem.assert_called_with(
            a_doc['id'], {'data': documents[1]}, 'documents')

        # convoy.prepare_auction(a_doc) with active.auction lot
        lc.get_lot.return_value = munchify({
            'data': {
                'id': a_doc['merchandisingObject'],
                'lotIdentifier': u'Q81318b19827',
                'status': u'active.auction',
                'assets': ['580d38b347134ac6b0ee3f04e34b9770'],
                'auctions': [a_doc['id']]
            }
        })
        basic_processing._receive_lot(a_doc)
        patched_api_auction_doc = {'data': {'status': 'active.tendering'}}
        convoy.auctions_client.patch_resource_item.assert_called_with(
            api_auction_doc['data']['id'],
            patched_api_auction_doc)

    @mock.patch('requests.Response.raise_for_status')
    @mock.patch('requests.Session.request')
    def test_file_bridge(self, mock_raise, mock_request):
        convoy = Convoy(self.config)
        for i in xrange(0, 2):
            convoy.documents_transfer_queue.put({
                'get_url': 'http://fs.com/item_{}'.format(i),
                'upload_url': 'http://fex.com/item_{}'.format(i)
            })
        convoy.auctions_client = mock.MagicMock()
        convoy.auctions_client.get_file.side_effect = [
            ('this is a file content', 'filename'),
            Exception('Something went wrong.'),
            ('this is a file content', 'filename')]
        convoy.stop_transmitting = mock.MagicMock()
        convoy.stop_transmitting.__nonzero__.side_effect = [
            False, False, False, False, True, True]
        self.assertEqual(convoy.documents_transfer_queue.qsize(), 2)
        convoy.file_bridge()
        self.assertEqual(convoy.documents_transfer_queue.qsize(), 0)
        self.assertEqual(convoy.auctions_client.get_file.call_count, 3)
        self.assertEqual(
            convoy.auctions_client.ds_client.document_upload_not_register.
            call_count, 2)

    @mock.patch('requests.Session.request')
    @mock.patch('requests.Response.raise_for_status')
    @mock.patch('openregistry.convoy.convoy.spawn')
    @mock.patch('openregistry.convoy.convoy.continuous_changes_feed')
    def test_run(self, mock_changes, mock_spawn, mock_raise, mock_request):

        mock_changes.return_value = [
            munchify({'status': 'pending.verification',
                      'id': uuid4().hex,
                      'merchandisingObject': uuid4().hex,
                      'procurementMethodType': 'rubble'}),
            munchify({'status': 'pending.verification',
                      'id': uuid4().hex,
                      'merchandisingObject': uuid4().hex,
                      'procurementMethodType': 'rubble'})
        ]

        convoy = Convoy(self.config)
        basic_processing = convoy.auction_type_processing_configurator['rubble']

        basic_processing.prepare_auction = mock.MagicMock(side_effect=[
            None, {'data': {'id': mock_changes.return_value[1]['id'],
                            'status': 'pending.verification'}}])
        convoy.run()
        auction_to_switch = {
            'data': {
                'id': mock_changes.return_value[1]['id'],
                'status': 'pending.verification'
            }
        }
        mock_spawn.assert_called_with(convoy.file_bridge)
        self.assertEqual(basic_processing.prepare_auction.call_count, 2)

    @mock.patch('logging.Logger.warning')
    @mock.patch('logging.Logger.info')
    @mock.patch('requests.Session.request')
    @mock.patch('requests.Response.raise_for_status')
    @mock.patch('openregistry.convoy.loki.processing.ProcessingLoki.process_auction')
    def test_process_single_auction(self, mock_loki_process, mock_raise, mock_request, mock_info, mock_warning):

        auction_id = uuid4().hex
        auction_doc = munchify({
            "data": {
                'status': 'unsuccessful',
                'id': auction_id,
                'merchandisingObject': uuid4().hex,
                'procurementMethodType': 'sellout.english'
            }
        })
        convoy = Convoy(self.config)
        convoy.auctions_client = mock.MagicMock()
        convoy.auctions_client.get_auction.return_value = auction_doc

        convoy.process_single_auction(auction_id)

        mock_loki_process.assert_called_with(auction_doc['data'])
        mock_info.assert_called_with(
            'Received auction {} in status {}'.format(
                auction_id, auction_doc['data'].status
            )
        )

        # Auction can not be found
        convoy.auctions_client.get_auction.side_effect = ResourceNotFound

        convoy.process_single_auction(auction_id)

        assert mock_loki_process.call_count == 1
        mock_warning.assert_called_with(
            'Auction object {} not found'.format(auction_id)
        )

    @mock.patch('requests.Response.raise_for_status')
    @mock.patch('requests.Session.request')
    def test_report_result_basic(self, mock_raise, mock_request):
        auction_doc = Munch({
            'id': uuid4().hex,  # this is auction id
            'status': 'complete',
            'merchandisingObject': uuid4().hex
        })
        lc = mock.MagicMock()
        lot = munchify({
            'data': {
                'id': auction_doc['merchandisingObject'],
                'lotIdentifier': u'Q81318b19827',
                'status': u'active.auction',
                'assets': ['580d38b347134ac6b0ee3f04e34b9770']
            }
        })
        lc.get_lot.return_value = lot
        convoy = Convoy(self.config)
        basic_processing = convoy.auction_type_processing_configurator['rubble']
        convoy.lots_client = basic_processing.lots_client = lc
        convoy.assets_client = basic_processing.assets_client = mock.MagicMock()
        basic_processing.report_results(auction_doc)
        convoy.lots_client.patch_resource_item.assert_called_with(
            auction_doc.merchandisingObject,
            {'data': {'status': 'pending.sold'}}
        )

        auction_doc.status = 'pending.verification'
        basic_processing.report_results(auction_doc)
        convoy.lots_client.patch_resource_item.assert_called_with(
            auction_doc.merchandisingObject,
            {'data': {'status': 'active.salable'}}
        )

    @mock.patch('openregistry.convoy.loki.processing.make_contract')
    @mock.patch('requests.Response.raise_for_status')
    @mock.patch('requests.Session.request')
    def test_report_result_loki_success(self, mock_raise, mock_request, mock_make_contract):

        terminal_loki_auction_statuses = ['complete', 'cancelled', 'unsuccessful']
        auction_docs = []
        lots = []
        lc = mock.MagicMock()

        for status in terminal_loki_auction_statuses:
            auction_docs.append(Munch({
                'id': uuid4().hex,  # this is auction id
                'status': status,
                'merchandisingObject': uuid4().hex,
                'procurementMethodType': choice(['sellout.insider', 'sellout.english'])
            }))

        lot_auctions = []
        for auction in auction_docs:
            lot_auctions.append(Munch({
                'id': uuid4().hex,
                'status': 'active',
                'relatedProcessID': auction.id
            }))

        for auction_doc in auction_docs:
            lots.append(munchify({
                'data': {
                    'id': auction_doc.merchandisingObject,
                    'status': u'active.auction',
                    'auctions': lot_auctions
                }
            }))
        lc.get_lot.side_effect = lots
        convoy = Convoy(self.config)
        for auction_doc in auction_docs:
            loki_processing = convoy.auction_type_processing_configurator[auction_doc.procurementMethodType]
            convoy.lots_client = loki_processing.lots_client = lc
            loki_processing._post_contract = mock.MagicMock()
            loki_processing.update_lot_contract = mock.MagicMock()
            tt = lambda l: 'transfer_token'
            loki_processing._extract_transfer_token = mock.MagicMock(side_effect=tt)
            loki_processing.report_results(auction_doc)
            convoy.lots_client.patch_resource_item_subitem.assert_called_with(
                resource_item_id=auction_doc.merchandisingObject,
                patch_data={'data': {'status': auction_doc.status}},
                subitem_name='auctions',
                subitem_id=next(auction.id for auction in lot_auctions
                                if auction_doc.id == auction.relatedProcessID)
            )
            assert loki_processing._post_contract.call_count == 0
            assert loki_processing.update_lot_contract.call_count == 0

    @mock.patch('logging.Logger.warning')
    @mock.patch('requests.Response.raise_for_status')
    @mock.patch('requests.Session.request')
    def test_report_result_loki_not_found(self, mock_raise, mock_request, mock_logger):

        lc = mock.MagicMock()

        auction_doc = Munch({
            'id': uuid4().hex,  # this is auction id
            'status': 'complete',
            'merchandisingObject': uuid4().hex,
            'procurementMethodType': choice(['sellout.insider', 'sellout.english'])
        })

        lc.get_lot.side_effect = [ResourceNotFound]
        convoy = Convoy(self.config)
        loki_processing = convoy.auction_type_processing_configurator[auction_doc.procurementMethodType]
        convoy.lots_client = loki_processing.lots_client = lc
        loki_processing.report_results(auction_doc)
        convoy.lots_client.get_lot.assert_called_with(
            auction_doc.merchandisingObject
        )

        mock_logger.assert_called_with(
            'Lot {} not found when report auction {} results'.format(
                auction_doc.merchandisingObject, auction_doc.id
            )
        )

    @mock.patch('logging.Logger.info')
    @mock.patch('requests.Response.raise_for_status')
    @mock.patch('requests.Session.request')
    def test_report_result_loki_already_reported_auction(self, mock_raise, mock_request, mock_logger):

        lc = mock.MagicMock()

        auction_doc = Munch({
            'id': uuid4().hex,  # this is auction id
            'status': 'complete',
            'merchandisingObject': uuid4().hex,
            'procurementMethodType': choice(['sellout.insider', 'sellout.english'])
        })

        lot_auction = Munch({
            'id': uuid4().hex,
            'status': 'complete',  # not 'active' status
            'relatedProcessID': auction_doc.id  # this is auction id
        })

        lot = munchify({
            'data': {
                'id': auction_doc.merchandisingObject,
                'status': 'active.auction',
                'auctions': [lot_auction]
            }
        })
        lc.get_lot.return_value = lot
        convoy = Convoy(self.config)
        loki_processing = convoy.auction_type_processing_configurator[auction_doc.procurementMethodType]
        convoy.lots_client = loki_processing.lots_client = lc
        loki_processing.report_results(auction_doc)
        convoy.lots_client.get_lot.assert_called_with(
            auction_doc.merchandisingObject
        )
        mock_logger.assert_called_with(
            'Auction {} results already reported to lot {}'.format(
                auction_doc.id, auction_doc.merchandisingObject
            )
        )

    @mock.patch('logging.Logger.warning')
    @mock.patch('requests.Response.raise_for_status')
    @mock.patch('requests.Session.request')
    def test_report_result_loki_related_auction_not_found(self, mock_raise, mock_request, mock_logger):

        lc = mock.MagicMock()

        auction_doc = Munch({
            'id': uuid4().hex,  # this is auction id
            'status': 'complete',
            'merchandisingObject': uuid4().hex,
            'procurementMethodType': choice(['sellout.insider', 'sellout.english'])
        })

        invalid_id = uuid4().hex

        lot_auction = Munch({
            'id': uuid4().hex,
            'status': 'active',
            'relatedProcessID': invalid_id  # this is (invalid) auction id
        })

        lot_auction_without_related_process_id = deepcopy(lot_auction)
        del lot_auction_without_related_process_id['relatedProcessID']

        lot = munchify({
            'data': {
                'id': auction_doc.merchandisingObject,
                'status': u'active.auction',
                'auctions': [lot_auction, lot_auction_without_related_process_id]
            }
        })
        lc.get_lot.return_value = lot
        convoy = Convoy(self.config)
        loki_processing = convoy.auction_type_processing_configurator[auction_doc.procurementMethodType]
        convoy.lots_client = loki_processing.lots_client = lc
        loki_processing.report_results(auction_doc)
        convoy.lots_client.get_lot.assert_called_with(
            auction_doc.merchandisingObject
        )
        mock_logger.assert_called_with(
                'Auction object {} not found in lot {}'.format(
                    auction_doc.id, auction_doc.merchandisingObject
                )
            )

    @mock.patch('logging.Logger.info')
    @mock.patch('requests.Response.raise_for_status')
    @mock.patch('requests.Session.request')
    def test_report_result_loki_contract_lot_created(self, mock_raise, mock_request, mock_logger):

        lc = mock.MagicMock()
        cc = mock.MagicMock()
        with open('{}/contract.json'.format(self.test_files_path), 'r') as cf:
            contract_dict = munchify(json.loads(cf.read()))

        auction_doc = Munch({
            'id': uuid4().hex,  # this is auction id
            'status': 'complete',
            'merchandisingObject': uuid4().hex,
            'contractTerms': {'type': 'test'},
            'procurementMethodType': choice(['sellout.insider', 'sellout.english']),
            'contracts': [contract_dict]
        })

        lot = munchify({
            'data': {
                'id': auction_doc.merchandisingObject,
                'status': u'active.auction',
                'auctions': [munchify({"relatedProcessID": auction_doc.id,
                                       "id": uuid4().hex,
                                       "status": "active"})],
                'contracts': [munchify({'id': uuid4().hex})]
            }
        })

        contract = munchify({'data': {
            'id': uuid4().hex,
            'contractID': 'contract_id'
        }})
        lc.get_lot.return_value = lot
        cc.create_contract.return_value = contract
        convoy = Convoy(self.config)
        loki_processing = convoy.auction_type_processing_configurator[auction_doc.procurementMethodType]
        convoy.lots_client = loki_processing.lots_client = lc
        convoy.contracts_client = loki_processing.contracts_client = cc
        tt = lambda l: 'transfer_token'
        loki_processing._extract_transfer_token = mock.MagicMock(side_effect=tt)
        loki_processing.report_results(auction_doc)
        convoy.lots_client.get_lot.assert_called_with(
            auction_doc.merchandisingObject
        )
        contract_data = make_contract(auction_doc)
        contract_data['transfer_token'] = 'transfer_token'
        convoy.contracts_client.create_contract.assert_called_with(
            {"data": contract_data}
        )
        mock_logger.assert_any_call(
            'Successfully created contract {} from lot {}'.format(
                contract.data.id, auction_doc.merchandisingObject
            )
        )

    @mock.patch('logging.Logger.info')
    @mock.patch('requests.Response.raise_for_status')
    @mock.patch('requests.Session.request')
    def test_report_result_loki_contract_only_created(self, mock_raise, mock_request, mock_logger):

        lc = mock.MagicMock()
        cc = mock.MagicMock()
        with open('{}/contract.json'.format(self.test_files_path), 'r') as cf:
            contract_dict = munchify(json.loads(cf.read()))

        auction_doc = Munch({
            'id': uuid4().hex,  # this is auction id
            'status': 'complete',
            'procurementMethodType': choice(['sellout.insider', 'sellout.english']),
            'contractTerms': {'type': 'test'},
            'contracts': [contract_dict]
        })

        contract = munchify({'data': {
            'id': uuid4().hex,
            'contractID': 'contract_id'
        }})
        # lc.get_lot.return_value = lot
        cc.create_contract.return_value = contract
        convoy = Convoy(self.config)
        loki_processing = convoy.auction_type_processing_configurator[auction_doc.procurementMethodType]
        convoy.lots_client = loki_processing.lots_client = lc
        convoy.contracts_client = loki_processing.contracts_client = cc
        tt = lambda l: 'transfer_token'
        loki_processing._extract_transfer_token = mock.MagicMock(side_effect=tt)
        loki_processing.report_results(auction_doc)

        assert convoy.lots_client.get_lot.call_count == 0

        contract_data = make_contract(auction_doc)
        contract_data['transfer_token'] = 'transfer_token'

        convoy.contracts_client.create_contract.assert_called_with(
            {"data": contract_data}
        )
        mock_logger.assert_called_with(
            'Successfully created contract {}'.format(
                contract.data.id
            )
        )
        assert lc.patch_resource_item_subitem.call_count == 0

    @mock.patch('requests.Response.raise_for_status')
    @mock.patch('requests.Session.request')
    def test_auctions_mapping_filtering(self, mock_raise, mock_request):
        changes_return_value = {
            'last_seq': 2,
            'results': [
                {'doc': {'id': uuid4().hex,
                         'status': 'unsuccessful',
                         'merchandisingObject': uuid4().hex,  # to get into lot_processing section
                         'procurementMethodType': choice(['sellout.insider', 'sellout.english'])}} for _ in range(3)
            ]
        }

        # to ensure that unicode strings are handled properly
        changes_return_value['results'][2]['doc']['id'] = unicode(uuid4().hex)

        processed_auction = deepcopy(changes_return_value['results'][0])
        mock_changes = mock.MagicMock()
        mock_changes.side_effect = [
            changes_return_value,  # 3 unprocessed auctions
            {'last_seq': 3, 'results': [processed_auction]}  # auction which already processed
        ]
        convoy = Convoy(self.config)
        convoy.db.changes = mock_changes

        loki_processing = convoy.auction_type_processing_configurator[
            processed_auction['doc']['procurementMethodType']
        ]
        loki_processing._get_lot = mock.MagicMock()
        loki_processing._check_lot_auction = mock.MagicMock()
        mock_switch = mock.MagicMock(return_value=True)
        loki_processing._switch_auction_status = mock_switch

        with mock.patch(
            'openregistry.convoy.utils.CONTINUOUS_CHANGES_FEED_FLAG',
            AlmostAlwaysTrue(3)
        ):
            convoy.timeout = 0.1
            convoy.run()

        # only 3 first auctions were processed, skipping one which already in mapping
        self.assertEqual(mock_switch.call_count, 3)
        for i in range(0, 2):
            self.assertEqual(
                convoy.auctions_mapping.has(
                    changes_return_value['results'][i]['doc']['id']
                ), True
            )

    @mock.patch('requests.Session.request')
    @mock.patch('openregistry.convoy.convoy.argparse.ArgumentParser',
                MockedArgumentParser)
    @mock.patch('openregistry.convoy.convoy.Convoy')
    def test__main(self, mock_convoy, mock_request):
        convoy_main()
        config_dict = deepcopy(DEFAULTS)
        with open('{}/{}'.format(ROOT, 'convoy.yaml'), 'r') as cf:
            config_dict.update(load(cf.read()))
        mock_convoy.assert_called_once_with(config_dict)
        self.assertEqual(mock_convoy().run.call_count, 1)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestConvoySuite))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
