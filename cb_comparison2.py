#_author__ = 'Yiran', thanks for David sharing his balancerer code

#!/usr/bin/python

from collections import Counter, defaultdict, OrderedDict
from itertools import chain
import re
import sys
import random


import rawusage

NUM_PARTS = 4096
NUM_PERC = 0.7


def get_shard_utilization(partition_table):
    shard_util = dict()
    shard_values = list(chain(*partition_table.values()))
    shard_list = list(set(shard_values))
    # Example: shard alln01h01s16 belongs to cloudlet alln01h01, cloudlet alln01h01 belongs to master site alln01
    cloudlet_pattern = re.compile('[a-z]{3,4}\d{1,2}([a-z])\d{2}')
    # site_pattern = re.compile('[a-z]{3,4}\d+')
    cloudlet_util = rawusage.CloudletUsage
    for vshard in shard_list:
        cloudlet_index = cloudlet_pattern.match(vshard).group(0)
        # site_index = site_pattern.match(vshard).group(0)
        # cloudlet_util = get_cloudlet_utilization(site_index)
        shard_util[vshard] = cloudlet_util[cloudlet_index]
    return shard_util


def recalculate(partition_table, shard_util):
    partlist_to_shift = list()
    write_candname = list()
    shard_to_partlist = OrderedDict()
    for partnum, partlist in partition_table.iteritems():
        util_list=[shard_util[candname] for candname in partlist]
        min_util_shard = util_list.index(min(util_list))
        # If the write target is not at Position 0, record the partition that need to shift (partlist_to_shift), and the
        # the shard index to write target candidate list (write_candname)
        if min_util_shard:
            # Shard with minmum utilization is not at Position 0
            partlist_to_shift.append(partnum)
            write_candname.append(partlist[min_util_shard])

    # For the shard that need to shift, how many partitions in this shard
    shard_to_partnum = OrderedDict()
    for k, v in zip(write_candname, partlist_to_shift):
        shard_to_partnum.setdefault(k,[]).append(v)

    # Randomly pick int(NUM_PERC*len(vp)) partitions in each shard that will shift the order of shards, and keep the left
    # partitions unaltered.
    shift_pt = dict(zip(partlist_to_shift, write_candname))
    candname_count = Counter(write_candname)
    parts_shift = list()
    for kv, vp in shard_to_partnum.iteritems():
        if len(vp)-1:
            shift_parts = random.sample(vp,int(NUM_PERC*len(vp)))
            for item in shift_parts:
                parts_shift.append(item)

    # Based on the partitions that need to shift (parts_shift), we shift the partition lists to re-balance the
    # partition table
    for partnum in parts_shift:
        shard_usage=[shard_util[i] for i in partition_table[partnum]]
        shift_shard = shard_usage.index(min(shard_usage))
        # move the shard with most free capacity to Position 0
        partition_table[partnum].insert(0, partition_table[partnum].pop(shift_shard))


    return partition_table


