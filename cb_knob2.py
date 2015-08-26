#_author__ = 'Yiran'

#!/usr/bin/python

from collections import Counter, defaultdict, OrderedDict
from itertools import chain
import re
import sys
import random
# import argparse
# import subprocess
# import salt.client
# import yaml
import cb_comparison2
import partition_table

NUM_PARTS = 4096



def print_od(od):
    for k in od:
        print "  {0}: {1}".format(k, od[k])



if __name__ == '__main__':
    pt = partition_table.PartitionTable
    print 'original partition table'
    print_od(OrderedDict(sorted(pt.items())))
    shard_util = cb_comparison2.get_shard_utilization(pt)
    partition_table_updated = cb_comparison2.recalculate(pt, shard_util)
    shard_bucket_map = OrderedDict(sorted(partition_table_updated.items()))
    print '============================================'
    print 'updated partition table'
    print_od(shard_bucket_map)

