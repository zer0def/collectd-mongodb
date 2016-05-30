#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import collections
import datetime
import functools
import json
import operator
import pprint
import random
import sys

try:
    from urllib import urlencode
    from urlparse import urlunparse
except ImportError:  # py3
    from urllib.parse import urlencode, urlunparse

import pymongo

import collectd


SERVER_STATS = {
    "asserts": "asserts",
    #"commands": "metrics.commands",
    "connections": "connections",
    "cursors": "cursors",
    "extra_info": "extra_info",
    "opcounters": "opcounters",
    "rss": "mem.resident",
    "uptime": "uptimeMillis",
}

SERVER_STATS_MONGOD = {
    "backgroundFlushing": "backgroundFlushing",
    "journal": "dur",
    "global_lock": "globalLock",
    "locks": "locks",
    "opcountersRepl": "opcountersRepl",
    #"repl": "repl",
}

_FLAG_FIRST = random.randint(-sys.maxsize-1, sys.maxsize)

INSTANCES = []


def flatten_dict(d, join=operator.add, lift=lambda x: x):
    results = []
    def visit(subdict, results, partialKey):
        for k,v in subdict.items():
            newKey = lift(k) if partialKey == _FLAG_FIRST else join(partialKey, lift(k))
            if isinstance(v, collections.Mapping):
                visit(v, results, newKey)
            else:
                results.append((newKey, v))
    visit(d, results, _FLAG_FIRST)
    return dict(results)


def get_metrics(_dict, metric_prefix, mapping):
    try:
        val = functools.reduce(lambda x, y: x[y], mapping.split("."), _dict)
    except KeyError:
        #collectd.error("Value for {} could not be fetched.".format(mapping))
        return {}
    else:
        if isinstance(val, collections.Mapping):
            return {
                ".".join([metric_prefix, k]): v for k, v in flatten_dict(
                    val, join=lambda x, y: ".".join([x, y])
                ).items() if is_num(v)
            }
        else:
            return {metric_prefix: val}


def is_num(val):
    num_types = [int, float]
    try:
        num_types.append(long)
    except NameError:  # long exists only in py2
        pass
    return any([isinstance(val, t) for t in num_types])


def dispatch_stat(plugin_instance, type_instance, value, host=None):
    val = collectd.Values(plugin="mongodb", plugin_instance=plugin_instance,
        type_instance=type_instance, type="gauge",  # you sure?
    )

    if host:
        val.host = host

    if is_num(value):
        val.values = [value]
        val.dispatch()
    # there are a few time values we'd really like to monitor
    elif isinstance(value, datetime.datetime):
        val.values = [int(value.strftime("%s"))]
        val.dispatch()


class MongoDB(object):
    def __init__(self):
        self.hosts = ["127.0.0.1:27017"]
        self.authdb = "admin"
        self.username = ""
        self.password = ""
        self.options = {}
        self.dblist = []
        self.conn = None
        self.replication = False
        self.sharding = False

    def init(self):
        self.conn = pymongo.MongoClient(host=urlunparse([
            "mongodb",  # scheme (always "mongodb")
            "{}:{}@{}".format(  # netloc (comma-separated list of <host>[:<port>])
                self.username, self.password, ",".join(self.hosts)
            ) if self.username and self.password else ",".join(self.hosts),
            self.authdb,  # path (auth db)
            "",  # params (always empty)
            urlencode(self.options),  # query string (list of connection options)
            "",  # fragment
        ]))

    def read(self):
        self.get_server_status()
        self.get_db_stats()
        if self.replication:
            self.get_repl_status()
        if self.sharding:
            self.get_shard_status()

    # https://docs.mongodb.org/manual/reference/command/serverStatus/
    def get_server_status(self):
        metrics = {}
        output = self.conn[self.authdb].command("serverStatus")
        if output.get("ok", 0):
            output.pop("$gleStats", 0)
            for metric_prefix, mapping in SERVER_STATS.items():
                metrics.update(get_metrics(output, metric_prefix, mapping))

            if output["process"] == "mongod":
                for metric_prefix, mapping in SERVER_STATS_MONGOD.items():
                    metrics.update(get_metrics(output, metric_prefix, mapping))

            for m, v in metrics.items():
                dispatch_stat(output["host"], m, v, host=output["host"])

    # https://docs.mongodb.org/manual/reference/command/dbStats/
    def get_db_stats(self):
        for db in self.dblist:
            output = self.conn[db].command("dbStats")
            if output.get("ok", 0):
                # pop out keys that aren't shared between sharded, replicated or standalone
                [output.pop(key, 0) for key in ["collections", "nsSize", "dataFileVersion", "raw", "$gleStats"]]
                for k, v in output.items():
                    dispatch_stat(db, "db.{}".format(k), v)

    # https://docs.mongodb.org/manual/reference/command/collStats/
    def get_coll_stats(self):  # implement it sometime
        pass

    # https://docs.mongodb.org/manual/reference/command/replSetGetStatus/
    def get_repl_status(self):
        output = self.conn[self.authdb].command("replSetGetStatus")
        primary = {}
        if output.get("ok", 0):
            output.pop("$gleStats", 0)

            # find primary member
            for member in output.get("members", []):
                # https://docs.mongodb.org/manual/reference/replica-states/
                if member["state"] == 1:
                    primary = member

            for member in output.get("members", []):
                # pop out useless fields
                [member.pop(key, 0) for key in ["_id", "self"]]

                for k, v in member.items():
                    dispatch_stat(output["set"], "replication.{}".format(k), v, host=member["name"])

                # only primaries and secondaries
                if member.get("state") in range(1, 3):
                    dispatch_stat(output["set"], "replication.lag",
                        (primary["optimeDate"]-member["optimeDate"]).total_seconds(),
                        host=member["name"])

    # https://docs.mongodb.org/manual/reference/config-database/
    def get_shard_status(self):
        cluster_id = str(self.conn["config"]["version"].find_one()["clusterId"])

        # send balancer lock state
        dispatch_stat(cluster_id, "shard.balancer_lock",
            int(self.conn["config"]["locks"].find_one({"_id": "balancer"})["state"]))

        # check collections' balances
        chunkCount = self.conn["config"]["chunks"].count()
        shardCount = self.conn["config"]["shards"].count()

        threshold = 8
        if chunkCount > 20 and chunkCount < 80:
            threshold = 4
        elif chunkCount <= 20:
            threshold = 2

        chunks = collections.defaultdict(lambda: collections.defaultdict(lambda: 1))
        nss = collections.defaultdict(lambda: 1)

        for chunk in self.conn["config"]["chunks"].find():
            if "ns" in chunk.keys():
                chunks[chunk["ns"]][chunk["shard"]] += 1
                nss[chunk["ns"]] += 1

        for namespace, total_chunks in nss.items():
            average = total_chunks / shardCount

            balanced = True
            for shard, shard_chunks in chunks[namespace].items():
                if chunks[namespace][shard] > average - threshold and chunks[namespace][shard] < average + threshold:
                    balanced = False

            dispatch_stat(namespace, "shard.collection_balanced", int(balanced))


def configure_callback(conf):
    instance = MongoDB()

    for node in conf.children:
        if node.key.lower() == "hosts":
            instance.hosts = node.values
        elif node.key.lower() == "authdb":
            instance.authdb = str(node.values[0])
        elif node.key.lower() == "username":
            instance.username = str(node.values[0])
        elif node.key.lower() == "password":
            instance.password = str(node.values[0])
        elif node.key.lower() == "dblist":
            instance.dblist = node.values
        elif node.key.lower() == "replicationstats":
            instance.replication = bool(node.values[0])
        elif node.key.lower() == "shardingstats":
            instance.sharding = bool(node.values[0])
        else:  # ambiguous?
            instance.options[node.key] = str(node.values[0])

    INSTANCES.append(instance)


def init_callback():
    [instance.init() for instance in INSTANCES]


def read_callback():
    [instance.read() for instance in INSTANCES]


collectd.register_config(configure_callback)
collectd.register_init(init_callback)
collectd.register_read(read_callback)
