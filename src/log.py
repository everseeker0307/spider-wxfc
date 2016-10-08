#!/usr/local/bin/python3
# coding:utf-8

import logging

logger = logging.getLogger('log')
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler('wxfc.log', encoding='utf-8')
fh.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(ch)
