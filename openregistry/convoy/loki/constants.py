# -*- coding: utf-8 -*-
SUCCESSFUL_TERMINAL_STATUSES = ('complete', )
UNSUCCESSFUL_TERMINAL_STATUSES = ('cancelled', 'unsuccessful')

SUCCESSFUL_PRE_TERMINAL_STATUSES = ('pending.complete', )
UNSUCCESSFUL_PRE_TERMINAL_STATUSES = ('pending.cancelled', 'pending.unsuccessful')

PRE_TERMINAL_MAPPING = {
    'pending.complete': 'complete',
    'pending.cancelled': 'cancelled',
    'pending.unsuccessful': 'unsuccessful',
}

CONTRACT_REQUIRED_FIELDS = [
    'awardID', 'contractID', 'items', 'suppliers',
    'value', 'dateSigned',
]
CONTRACT_NOT_REQUIRED_FIELDS = [
    'contractNumber', 'title', 'title_en', 'title_ru',
    'description', 'description_en', 'description_ru',
    'documents'
]

CREATE_CONTRACT_MESSAGE_ID = 'create_contract'
UPDATE_CONTRACT_MESSAGE_ID = 'update_contract'
SWITCH_LOT_AUCTION_STATUS_MESSAGE_ID = 'switch_lot_auction_status'
