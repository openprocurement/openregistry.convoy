# -*- coding: utf-8 -*-
SUCCESSFUL_TERMINAL_STATUSES = ('complete', )
CONTRACT_TYPE = 'ceasefire'
CONTRACT_REQUIRED_FIELDS = [
    'awardID', 'contractID', 'items', 'suppliers',
    'value', 'dateSigned', 'documents'
]
CONTRACT_NOT_REQUIRED_FIELDS = [
    'contractNumber', 'title', 'title_en', 'title_ru',
    'description', 'description_en', 'description_ru'
]
