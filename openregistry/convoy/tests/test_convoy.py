# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_all()

import unittest
import json
import mock
import os
from yaml import load
from gevent.queue import Queue
from munch import munchify, Munch
from couchdb import Server, Session, Database
from openprocurement_client.resources.assets import AssetsClient
from openprocurement_client.resources.lots import LotsClient
from openprocurement_client.clients import APIResourceClient
from openregistry.convoy.convoy import Convoy, main as convoy_main
from uuid import uuid4

# Absolute path to file, dropping 'openregistry/convoy/tests' part
# os.getcwd() is not suitable for run_test.py script
ROOT = '/'.join(os.path.dirname(__file__).split('/')[:-3])


class MockedArgumentParser(mock.MagicMock):

    def __init__(self, description):
        super(MockedArgumentParser, self).__init__()
        self.description = description

    def add_argument(self, argument_name, type, help):
        self.argument_name = argument_name
        self.help = help

    def parse_args(self):
        return munchify({'config': '{}/{}'.format(ROOT,
                                                  '/convoy.yaml')})


class TestConvoySuite(unittest.TestCase):
    """ TestCase Convoy functionality """

    def setUp(self):
        self.test_files_path = '{}/{}'.format(os.path.dirname(__file__),
                                              'files/')
        with open('{}/convoy.yaml'.format(ROOT)) as config_file_obj:
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

    @mock.patch('requests.Response.raise_for_status')
    def test_init(self, mock_raise):
        convoy = Convoy(self.config)
        self.assertEqual(convoy.stop_transmitting, False)
        self.assertEqual(convoy.transmitter_timeout,
                         self.config['transmitter_timeout'])
        self.assertEqual(convoy.timeout, self.config['timeout'])
        self.assertIsInstance(convoy.documents_transfer_queue, Queue)
        self.assertIsInstance(convoy.api_client, APIResourceClient)
        self.assertIsInstance(convoy.lots_client, LotsClient)
        self.assertIsInstance(convoy.assets_client, AssetsClient)
        self.assertIsInstance(convoy.db, Database)
        self.assertEqual(convoy.db.name, self.config['couchdb']['db'])

    def fake_response(self):
        return None

    @mock.patch('requests.Response.raise_for_status')
    def test__create_items_from_assets(self, mock_raise):
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
        convoy.assets_client = mock_rc
        convoy.api_client = mock_rc
        items, documents = convoy._create_items_from_assets(asset_ids)
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
        items, documents = convoy._create_items_from_assets(asset_ids)
        self.assertEqual(len(items), 2)
        self.assertEqual(len(documents), 0)
        self.assertEqual(convoy.documents_transfer_queue.qsize(), 1)

    @mock.patch('requests.Response.raise_for_status')
    def test_prepare_auction(self, mock_raise):
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
        api_client = mock.MagicMock()
        api_client.get_resource_item.return_value = munchify(api_auction_doc)
        convoy = Convoy(self.config)
        convoy.api_client = api_client
        convoy.lots_client = lc
        auction_doc = convoy.prepare_auction(a_doc)
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

        convoy.assets_client = mock_rc
        lc.get_lot.return_value = munchify({
            'data': {
                'id': a_doc['merchandisingObject'],
                'lotIdentifier': u'Q81318b19827',
                'status': u'active.salable',
                'assets': ['580d38b347134ac6b0ee3f04e34b9770']
            }
        })
        items, documents = convoy._create_items_from_assets(asset_ids)
        convoy._create_items_from_assets = mock.MagicMock(return_value=(
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
        lot = convoy._receive_lot(a_doc)
        convoy._form_auction(lot, a_doc)
        convoy.api_client.patch_resource_item.assert_called_with(a_doc['id'], expected)
        convoy._activate_auction(lot, a_doc)
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
        convoy._create_items_from_assets.assert_called_with(asset_ids)
        patched_api_auction_doc = {'data': {'status': 'active.tendering'}}
        convoy.api_client.get_resource_item.assert_called_with(a_doc['id'])
        convoy.api_client.patch_resource_item.assert_called_with(
            api_auction_doc['data']['id'],
            patched_api_auction_doc)
        convoy.api_client.create_resource_item_subitem.assert_called_with(
            a_doc['id'], {'data': documents[1]}, 'documents')

    @mock.patch('requests.Response.raise_for_status')
    def test_file_bridge(self, mock_raise):
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
        convoy.stop_transmitting = mock.MagicMock()
        convoy.stop_transmitting.__nonzero__.side_effect = [
            False, False, False, False, True, True]
        self.assertEqual(convoy.documents_transfer_queue.qsize(), 2)
        convoy.file_bridge()
        self.assertEqual(convoy.documents_transfer_queue.qsize(), 0)
        self.assertEqual(convoy.api_client.get_file.call_count, 3)
        self.assertEqual(
            convoy.api_client.ds_client.document_upload_not_register.
            call_count, 2)

    @mock.patch('requests.Response.raise_for_status')
    @mock.patch('openregistry.convoy.convoy.spawn')
    @mock.patch('openregistry.convoy.convoy.continuous_changes_feed')
    def test_run(self, mock_changes, mock_spawn, mock_raise):
        mock_changes.return_value = [
            munchify({'status': 'pending.verification', 'id': uuid4().hex, 'merchandisingObject': uuid4().hex}),
            munchify({'status': 'pending.verification', 'id': uuid4().hex, 'merchandisingObject': uuid4().hex})
        ]
        convoy = Convoy(self.config)
        convoy.prepare_auction = mock.MagicMock(side_effect=[
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
        self.assertEqual(convoy.prepare_auction.call_count, 2)

    @mock.patch('requests.Response.raise_for_status')
    def test_report_result(self, mock_raise):
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
        convoy.lots_client = lc
        convoy.assets_client = mock.MagicMock()
        convoy.report_results(auction_doc)
        convoy.lots_client.patch_resource_item.assert_called_with(
            auction_doc.merchandisingObject,
            {'data': {'status': 'sold'}}
        )
        convoy.assets_client.patch_asset.assert_called_with(
            lot['data']['assets'][0],
            {'data': {'status': 'complete'}}
        )

    @mock.patch('openregistry.convoy.convoy.argparse.ArgumentParser',
                MockedArgumentParser)
    @mock.patch('openregistry.convoy.convoy.Convoy')
    def test__main(self, mock_convoy):
        convoy_main()
        with open('{}/{}'.format(ROOT, 'convoy.yaml'), 'r') as cf:
            config_dict = load(cf.read())
        mock_convoy.assert_called_once_with(config_dict)
        self.assertEqual(mock_convoy().run.call_count, 1)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestConvoySuite))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
