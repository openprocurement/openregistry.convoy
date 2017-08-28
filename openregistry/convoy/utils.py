# -*- coding: utf-8 -*-
from logging import getLogger
from pkg_resources import get_distribution
from time import sleep

PKG = get_distribution(__package__)
LOGGER = getLogger(PKG.project_name)

FILTER_DOC_ID = '_design/auction_filters'
FILTER_PENDING_VERIFICATION_DOC = """
function(doc, req) {
    if (doc.doc_type == 'Auction' && doc.status== 'pending.verification') {
        return true;
    }
    return false;
}
"""

CONTINUOUS_CHANGES_FEED_FLAG = True  # Need for testing


def push_filter_doc(db):
    filters_doc = db.get(FILTER_DOC_ID, {'_id': FILTER_DOC_ID, 'filters': {}})
    if (filters_doc and filters_doc['filters'].get('pending_verification') !=
            FILTER_PENDING_VERIFICATION_DOC):
        filters_doc['filters']['pending_verification'] = \
            FILTER_PENDING_VERIFICATION_DOC
        db.save(filters_doc)
        LOGGER.info('Filter doc \'pending_verification\' saved.')
    else:
        LOGGER.info('Filter doc \'pending_verification\' exist.')


def continuous_changes_feed(db, timeout=10, limit=100,
                            filter_doc='auction_filters/pending_verification'):
    last_seq_id = 0
    while CONTINUOUS_CHANGES_FEED_FLAG:
        data = db.changes(include_docs=True, since=last_seq_id, limit=limit,
                          filter=filter_doc)
        last_seq_id = data['last_seq']
        if len(data['results']) != 0:
            for row in data['results']:
                item = {
                    'id': row['doc']['_id'],
                    'lotID': row['doc']['merchandisingObject']
                }
                yield item
        else:
            sleep(timeout)
