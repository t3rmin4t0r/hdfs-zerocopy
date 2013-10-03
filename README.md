HDFS Short-circuit testing
==========================

Compare non-checksummed short-circuiting with checksum-enabled version

For the read path, emulate what a SequenceFile or RCFile reader would do, by directly reading bytes off the FSDataInputStream.

	23871 read(168, "RCF", 3)               = 3
	23871 read(168, "\1", 1)                = 1
	23871 read(168, "\0", 1)                = 1
	23871 read(168, "\0", 1)                = 1
	23871 read(168, "\0", 1)                = 1
	23871 read(168, "\0", 1)                = 1
	23871 read(168, "\1", 1)                = 1
	23871 read(168, "\34", 1)               = 1
	23871 read(168, "hive.io.rcfile.column.number", 28) = 28
	23871 read(168, "\2", 1)                = 1
	23871 read(168, "22", 2)                = 2
	23871 read(168, "\266v\225\7\256\177\300\254w\320\32\336Gk)a", 16) = 16

By default the HDFS data stream is buffered when reading from datanode, so it would be wasteful to add a buffer here.

When checksums are disabled, we find performance degrades in HDFS data-client which is contrary to what is expected

To test

	$ hadoop jar target/zcr-reader-1.0-SNAPSHOT.jar -i /user/hive/warehouse/tpcds_bin_flat_rc_10.db/store_sales/000000_0 -zc

versus

	$ hadoop jar target/zcr-reader-1.0-SNAPSHOT.jar --nochecksum -i /user/hive/warehouse/tpcds_bin_flat_rc_10.db/store_sales/000000_0 -zc

For my tests, checksum enabled ran in 37 seconds & the checksum disabled version ran in 183 seconds.
