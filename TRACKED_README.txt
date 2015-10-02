This branch contains an experimental version of FixedBitSet. Iternally it maintains
a structure that tracks changes to the bitset, which means that many operations in
theory are a lot faster to perform than the non-tracked implementation.

In FixedBitSet, the bits are packed into longs (64 bit). For vanilla FixedBitSet, 
operations such as cardinality, union, intersection, andNot and xor all require all
longs to be iterated. The tracked FixedBitSet avoids this and only visits the longs
that have values relevant to the operation.

Iterating an array of longs and doing simple operations on them is very fast, so in
order to get even faster than that, the overhead of tracking must be very small.
Unfortunately it seems that this is not the case. With the current trackin code,
the sparsity of the bitset needs to be in 1/1000 or less to get any substantial
benefits.

Note: The current code does not have any switch-over-to-non-tracked logic, which
means that operations on dense bitsets are a lot slower than for non-tracked. If
the tracked FixedBitSet is deemes usable after all, such logic should be added 
(the approximateCardinality method or explicit counting of set bits in the level 1
tracker are prime candidates for ensuring fast checks).

To test the speed of the implementation, follow the steps below:

1) Checkout the project
git clone git@github.com:tokee/lucene-solr.git -b tracked_switch
cd lucene_solr

2) Compile lucene, solr and the Solr test package
cd lucene
ant compile
cd ../solr
ant jar-core
ant compile-test

3) Run a test
Tracked FixedBitSet:
java -cp ./build/solr-core/solr-core-6.0.0-SNAPSHOT.jar:build/solr-core/classes/test/:../lucene/build/core/lucene-core-6.0.0-SNAPSHOT.jar org.apache.solr.util.BitSetPerf 10000000 10 10000 cardinality 10
