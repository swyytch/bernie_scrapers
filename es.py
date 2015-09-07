#!/usr/bin/env python2

from __future__ import print_function
import logging
import pymongo
import sys
import yaml

from elasticsearch import Elasticsearch
from pymongo import MongoClient
from time import time
from pymongo.cursor import CursorType


logging.basicConfig(format="%(asctime)s - %(levelname)s : %(message)s",
                    level=logging.INFO)


def config():
    configfile = "/opt/bernie/config.yml"
    try:
        with open(configfile, 'r') as f:
            conf = yaml.load(f)
    except IOError:
        # msg = "Could not open config file: {0}"
        # logging.info(msg.format(configfile))
        sys.exit(1)
    else:
        return conf


def connect_mongo():
    conf = config()
    c = conf["elasticsearch"]
    db = MongoClient(c["mongohost"], c["mongoport"])
    db.admin.authenticate(
        c["mongouser"],
        c["mongopass"],
        mechanism='SCRAM-SHA-1'
    )
    return db

if __name__ == '__main__':
    db = connect_mongo()
    conf = config()["elasticsearch"]
    es = Elasticsearch(conf["host"])
    oplog = db.local.oplog.rs
    first = next(oplog.find().sort('$natural', pymongo.DESCENDING).limit(-1))
    ts = first['ts']

    while True:
        cursor = oplog.find(
            {'ts': {'$gt': ts}},
            cursor_type=CursorType.TAILABLE_AWAIT,
            oplog_replay=True
        )
        while cursor.alive:
            for doc in cursor:
                ts = doc['ts']
                dbname = doc['ns'].split(".")[0]
                if dbname != "bernie":
                    continue
                index = doc['ns'].split(".")[-1]
                if doc['op'] == 'i':
                    doc["o"]["_id"] = str(doc["o"]["_id"])
                    doc['o']['timestamp'] = time()
                    try:
                        name = doc["o"]["name"]
                    except KeyError:
                        name = doc["o"]["title"]
                    msg = "Inserting {0} for {1} '{2}'"
                    logging.info(msg.format(
                        doc["o"]["_id"],
                        index,
                        name))
                    es.index(
                        index="_".join((index, "test")),
                        doc_type=doc['o']['site'],
                        id=doc['o']['_id'],
                        body=doc["o"]
                    )
                elif doc['op'] == 'u':
                    query = {"_id": doc["o2"]["_id"]}
                    cur = db.bernie[index].find(query)[0]
                    cur["timestamp"] = time()
                    try:
                        name = cur["name"]
                    except KeyError:
                        name = cur["title"]
                    msg = "Inserting {0} for {1} '{2}'"
                    logging.info(msg.format(
                        str(cur["_id"]),
                        index,
                        name))
                    es.index(
                        index="_".join((index, "test")),
                        doc_type=cur['site'],
                        id=str(cur["_id"]),
                        body=cur
                    )
