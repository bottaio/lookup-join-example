# lookup-join-example

An example of temporal event time join that shows poor performance of such query while working with RocksDB.
On my machine I'm getting 600k eps with Heap backend and 2.7k eps with RocksDB. 200x difference is definitely more than we'd expect.
