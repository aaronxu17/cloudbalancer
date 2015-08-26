#_author__ = 'Yiran'

#!/usr/bin/python

from collections import Counter, defaultdict, OrderedDict
from itertools import chain
import re
import sys
import random
import argparse
import subprocess
import salt.client
import yaml



NUM_PARTS = 4096
NUM_PERC = 1

def _detect_nodes(prefix):
    p1 = subprocess.Popen(['salt-key', '--output=yaml'], stdout=subprocess.PIPE)
    all_minions = yaml.load(p1.communicate()[0])['minions']
    atmos_pattern = re.compile(re.escape(prefix) + r'([a-z])\d{1,2}-is1-001')
    master_nodes = [x for x in all_minions if atmos_pattern.match(x)]
    return master_nodes

def print_od(od):
    for k in od:
        print "  {0}: {1}".format(k, od[k])


def get_cloudlet_utilization(prefix):
    """
    mauifs                        18P   13P  4.2P  76% /mnt/mauipfs
    """
    cloudlet_masters = _detect_nodes(prefix)
    util_dict = dict()
    salt_client = salt.client.LocalClient()
    df_out = salt_client.cmd(prefix + '*-is1-001', 'cmd.run', ['df -h | grep mauipfs'], timeout=30)
    # sanity check to make sure we got results from all cloudlet masters
    if set(cloudlet_masters) != set(df_out.keys()):
        print "Not all minions responding.   Fix {0} and try again.".format(set(cloudlet_masters) - set(df_out.keys()))
        sys.exit(1)
    df_pattern = re.compile(r'^mauifs\s*[\d.]+\w\s+[\d.]+\w\s+[\d.]+\w\s+(\d+)%\s/mnt/mauipfs$')
    host_pattern = re.compile(r'[A-Za-z]{3,4}\d+([a-z])\d+-is1-001')
    for host in df_out:
        cloudlet = host_pattern.match(host).group(1)
        util_dict[cloudlet] = int(df_pattern.match(df_out[host]).group(1))

    return util_dict


def get_shard_utilization(partition_table):
    shard_util = dict()
    shard_values = list(chain(*partition_table.values()))
    shard_list = list(set(shard_values))
    # Example: shard alln01h01s16 belongs to cloudlet alln01h01
    cloudlet_pattern = re.compile('[a-z]{3,4}\d{1,2}([a-z])\d{2}')
    for vshard in shard_list:
        shard_util[vshard] = get_cloudlet_utilization[cloudlet_pattern.match(vshard).group(0)]
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


if __name__ == '__main__':

    ShardUtilization = get_shard_utilization(PartitionTable)
    partition_table_updated = recalculate(PartitionTable, ShardUtilization)
    print 'updated partition table'
    print_od(partition_table_updated)

