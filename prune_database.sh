#!/bin/bash
#
#customer_header.csv
#custkey               name        address    city       nation     region        phone          mktsegment
#INTEGER               STRING:25   STRING:25  STRING:10  STRING:15  STRING:12     STRING:15      STRING:10
#
#date_header.csv
#datekey               date        dayofweek  month      year       yearmonthnum  yearmonth      daynuminweek  daynuminmonth  daynuminyear   monthnuminyear   weeknuminyear  sellingseason  lastdayinweekfl  lastdayinmonthfl  holidayfl   weekdayfl
#INTEGER               STRING:18   STRING:9   STRING:9   SHORTINT   INTEGER       STRING:7       TINYINT       TINYINT        SHORTINT       TINYINT          TINYINT        STRING:12      CHAR             CHAR              CHAR        CHAR
#
#lineorder_header.csv
#orderkey              linenumber  custkey    partkey    suppkey    orderdate     orderpriority  shippriority  quantity       extendedprice  ordertotalprice  discount       revenue        supplycost       tax               commitdate  shipmode
#INTEGER               TINYINT     INTEGER    INTEGER    INTEGER    INTEGER       STRING:15      CHAR          TINYINT        INTEGER        INTEGER          TINYINT        INTEGER        INTEGER          TINYINT           INTEGER     STRING:10
#
#part_header.csv
#partkey               name        mfgr       category   brand      color         type           size          container
#INTEGER               STRING:22   STRING:6   STRING:7   STRING:9   STRING:11     STRING:25      TINYINT       STRING:10
#
#supplier_header.csv
#suppkey               name        address    city       nation     region        phone
#INTEGER               STRING:25   STRING:25  STRING:10  STRING:15  STRING:12     STRING:15


(for f in customer_header.csv date_header.csv lineorder_header.csv part_header.csv supplier_header.csv ; do echo $f; cat $f | sed 's/|/ /g'; done) | column -t

pushd database

for sf in $(seq 2 4); do
	pushd sf-$sf

	# customer
	for col in name address city nation region phone mktsegment; do rm -f customerAN_${col}.ahead && ln -s customer_${col}.ahead customerAN_${col}.ahead; done
	rm -f customer*.tbl

	# date
	for col in date dayofweek month yearmonth sellingseason; do rm -f dateAN_${col}.ahead && ln -s date_${col}.ahead dateAN_${col}.ahead; done
	rm -f date*.tbl

	# lineorder
	for col in orderpriority shipmode; do rm -f lineorderAN_${col}.ahead && ln -s lineorder_${col}.ahead lineorderAN_${col}.ahead; done

	# part
	for col in name mfgr category brand color type container; do rm -f partAN_${col}.ahead && ln -s part_${col}.ahead partAN_${col}.ahead; done

	#supplier
	for col in name address city nation region phone; do rm -f supplierAN_${col}.ahead && ln -s supplier_${col}.ahead supplierAN_${col}.ahead; done

	popd
done

popd

