# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_all()

import unittest
import json
import mock
import os
from copy import deepcopy
from yaml import load
from gevent.queue import Queue
from munch import munchify
from couchdb import Server, Session, Database
from openprocurement_client.document_service_client import (
    DocumentServiceClient)
from openprocurement_client.registry_client import LotsClient, AssetsClient
from openprocurement_client.client import TendersClient
from openregistry.convoy.convoy import Convoy, main as convoy_main
from uuid import uuid4
from yaml import load


class MockedArgumentParser(mock.MagicMock):

    def __init__(self, description):
        super(MockedArgumentParser, self).__init__()
        self.description = description

    def add_argument(self, argument_name, type, help):
        self.argument_name = argument_name
        self.help = help

    def parse_args(self):
        return munchify({'config': '{}/{}'.format(os.getcwd(),
                                                  '/convoy.yaml')})


class TestConvoySuite(unittest.TestCase):
    """ TestCase Convoy functionality """

    def setUp(self):
        self.test_files_path = '{}/{}'.format(os.path.dirname(__file__),
                                              'files/')
        with open('{}/convoy.yaml'.format(os.getcwd())) as config_file_obj:
            self.config = load(config_file_obj.read())
        user = self.config['couchdb'].get('user', '')
        password = self.config['couchdb'].get('password', '')
        if user and password:
            self.server = Server(
                "http://{user}:{password}@{host}:{port}".format(
                    **self.config['couchdb']),
                session=Session(retry_delays=range(10)))
        else:
            self.server = Server(
                "http://{host}:{port}".format(
                    **self.config['couchdb']),
                session=Session(retry_delays=range(10)))
        if self.config['couchdb']['db'] not in self.server:
            self.server.create(self.config['couchdb']['db'])

    def tearDown(self):
        del self.server[self.config['couchdb']['db']]

    def test_init(self):
        convoy = Convoy(self.config)
        self.assertEqual(convoy.stop_transmitting, False)
        self.assertEqual(convoy.transmitter_timeout,
                         self.config['transmitter_timeout'])
        self.assertEqual(convoy.timeout, self.config['timeout'])
        self.assertIsInstance(convoy.documents_transfer_queue, Queue)
        self.assertIsInstance(convoy.ds_client, DocumentServiceClient)
        self.assertIsInstance(convoy.api_client, TendersClient)
        self.assertIsInstance(convoy.lots_client, LotsClient)
        self.assertIsInstance(convoy.assets_client, AssetsClient)
        self.assertIsInstance(convoy.db, Database)
        self.assertEqual(convoy.db.name, self.config['couchdb']['db'])

    def test__create_items_from_assets(self):
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
        mock_ds = mock.MagicMock()
        mock_rc.get_asset.return_value = munchify(asset_dict)
        mock_ds.register_document_upload.return_value = \
            munchify(register_response_dict)
        asset_ids = ['580d38b347134ac6b0ee3f04e34b9770']

        convoy = Convoy(self.config)
        convoy.ds_client = mock_ds
        convoy.assets_client = mock_rc
        items, documents = convoy._create_items_from_assets(asset_ids)
        self.assertEqual(convoy.documents_transfer_queue.qsize(), 1)
        transfer_item = convoy.documents_transfer_queue.get()
        self.assertEqual(transfer_item['get_url'],
                         document_dict['data']['url'])
        self.assertNotEqual(transfer_item['get_url'],
                            transfer_item['upload_url'])
        self.assertEqual(len(items), 1)
        self.assertEqual(len(documents), 1)
        for k in items_keys:
            # Skip this key because hard code changing from CPVS to CAV
            if k == 'classification':
                continue
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
        mock_rc.get_asset.return_value = munchify(asset_dict)
        items, documents = convoy._create_items_from_assets(asset_ids)
        self.assertEqual(len(items), 1)
        self.assertEqual(len(documents), 0)
        self.assertEqual(convoy.documents_transfer_queue.qsize(), 0)

    def test_prepare_auction_data(self):
        a_doc = {
            'id': uuid4().hex,  # this is auction id
            'lotID': uuid4().hex
        }
        api_auction_doc = {
            'data': {
                'id': uuid4().hex,
                'status': 'pending.verifcation',
                'lotID': a_doc['lotID']
            }
        }
        lc = mock.MagicMock()
        lc.get_lot.return_value = munchify({
            'data': {
                'id': a_doc['lotID'],
                'status': u'verification',
                'assets': ['580d38b347134ac6b0ee3f04e34b9770']
            }
        })
        api_client = mock.MagicMock()
        api_client.get_resource_item.return_value = munchify(api_auction_doc)
        convoy = Convoy(self.config)
        convoy.api_client = api_client
        convoy.lots_client = lc
        auction_doc = convoy.prepare_auction_data(a_doc)
        self.assertEqual(None, auction_doc)
        convoy.lots_client.get_lot.assert_called_with(a_doc['lotID'])

        # Preparing for test with valid status and documents
        with open('{}/asset.json'.format(self.test_files_path), 'r') as af:
            asset_dict = json.loads(af.read())
        with open('{}/document.json'.format(self.test_files_path), 'r') as df:
            document_dict = json.loads(df.read())
        with open('{}/register_response.json'.format(
                self.test_files_path), 'r') as rf:
            register_response_dict = json.loads(rf.read())

        # Prepare mocked clients
        mock_rc = mock.MagicMock()
        mock_ds = mock.MagicMock()
        mock_rc.get_asset.return_value = munchify(asset_dict)
        mock_ds.register_document_upload.return_value = \
            munchify(register_response_dict)
        asset_ids = ['580d38b347134ac6b0ee3f04e34b9770']

        convoy.ds_client = mock_ds
        convoy.assets_client = mock_rc
        lc.get_lot.return_value = munchify({
            'data': {
                'id': a_doc['lotID'],
                'status': u'active.salable',
                'assets': ['580d38b347134ac6b0ee3f04e34b9770']
            }
        })
        items, documents = convoy._create_items_from_assets(asset_ids)
        convoy._create_items_from_assets = mock.MagicMock(return_value=(
            items, documents))
        auction_doc = convoy.prepare_auction_data(a_doc)
        self.assertEqual(convoy.documents_transfer_queue.qsize(), 1)
        convoy.lots_client.get_lot.assert_called_with(a_doc['lotID'])
        convoy.lots_client.patch_resource_item.assert_called_with({'data': {
            'status': 'active.auction',
            'id': a_doc['lotID'],
            'assets': ['580d38b347134ac6b0ee3f04e34b9770']
        }})
        convoy._create_items_from_assets.assert_called_with(asset_ids)
        patched_api_auction_doc = deepcopy(api_auction_doc)
        patched_api_auction_doc['data']['items'] = items
        convoy.api_client.get_resource_item.assert_called_with(a_doc['id'])
        convoy.api_client.patch_resource_item.assert_called_with(
            patched_api_auction_doc)
        convoy.api_client.create_thin_document.assert_called_with(
            patched_api_auction_doc, documents[0])
        self.assertEqual(auction_doc, patched_api_auction_doc)

    def test_switch_auction_to_active_tendering(self):
        convoy = Convoy(self.config)
        convoy.api_client = mock.MagicMock()
        auction = {
            'data': {
                'id': uuid4().hex,
                'status': 'pending.verification'
            }
        }
        patched_auction = deepcopy(auction)
        patched_auction['data']['status'] = 'active.tendering'
        convoy.switch_auction_to_active_tendering(auction)
        convoy.api_client.patch_resource_item.assert_called_with(
            patched_auction)

    def test_file_bridge(self):
        convoy = Convoy(self.config)
        for i in xrange(0, 2):
            convoy.documents_transfer_queue.put({
                'get_url': 'http://fs.com/item_{}'.format(i),
                'upload_url': 'http://fex.com/item_{}'.format(i)
            })
        convoy.api_client = mock.MagicMock()
        convoy.api_client.get_file.side_effect = [
            ('this is a file content', 'filename'),
            Exception('Something went wrong.'),
            ('this is a file content', 'filename')]
        convoy.ds_client = mock.MagicMock()
        convoy.stop_transmitting = mock.MagicMock()
        convoy.stop_transmitting.__nonzero__.side_effect = [
            False, False, False, False, True, True]
        self.assertEqual(convoy.documents_transfer_queue.qsize(), 2)
        convoy.file_bridge()
        self.assertEqual(convoy.documents_transfer_queue.qsize(), 0)
        self.assertEqual(convoy.api_client.get_file.call_count, 3)
        self.assertEqual(
            convoy.ds_client.document_upload_not_register.call_count, 2)

    @mock.patch('openregistry.convoy.convoy.spawn')
    @mock.patch('openregistry.convoy.convoy.continuous_changes_feed')
    def test_run(self, mock_changes, mock_spawn):
        mock_changes.return_value = [{'id': uuid4().hex, 'lotID': uuid4().hex},
                                     {'id': uuid4().hex, 'lotID': uuid4().hex}]
        convoy = Convoy(self.config)
        convoy.prepare_auction_data = mock.MagicMock(side_effect=[
            None, {'data': {'id': mock_changes.return_value[1]['id'],
                            'status': 'pending.verification'}}])
        convoy.switch_auction_to_active_tendering = mock.MagicMock()
        convoy.run()
        auction_to_switch = {
            'data': {
                'id': mock_changes.return_value[1]['id'],
                'status': 'pending.verification'
            }
        }
        mock_spawn.assert_called_with(convoy.file_bridge)
        self.assertEqual(convoy.prepare_auction_data.call_count, 2)
        convoy.switch_auction_to_active_tendering.assert_called_once_with(
            auction_to_switch)

    @mock.patch('openregistry.convoy.convoy.argparse.ArgumentParser',
                MockedArgumentParser)
    @mock.patch('openregistry.convoy.convoy.Convoy')
    def test__main(self, mock_convoy):
        convoy_main()
        with open('{}/{}'.format(os.getcwd(), 'convoy.yaml'), 'r') as cf:
            config_dict = load(cf.read())
        mock_convoy.assert_called_once_with(config_dict)
        self.assertEqual(mock_convoy().run.call_count, 1)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestConvoySuite))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
