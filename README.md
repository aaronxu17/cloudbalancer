per-partition comparison scheme
==============

load-balancer scheme for partition table

Input: Partition Table, Shard Utilization Table, Percentage that the Number of Partition Lists to be shift<br />
Output: Updated Partition Table<br />
<br />
Input Format:<br />
Partition Table: Type: Dict() <br />
Key: Partition Number<br />
Value: A list of Shards<br />
<br />
Shard Utilization: Type: Dict()<br />
Key: Shard Name<br />
Value: Corresponding Cloudlet capacity usage<br />
NUM_PERC: Type: float 0~1<br />
NUM_PERC = 1, shift all the partition lists<br />
NUM_PERC = 0.5, shift half of the partition lists
