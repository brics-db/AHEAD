#!/bin/bash

for sf in $(seq 1 10); do
	for tab in c d l p s; do
		./dbgen -s ${sf} -T ${tab} -v || (echo "Error generating table '$tab'"; exit 1)
	done
	mkdir -p sf-${sf}
	ret=$?
	if [[ $ret -ne 0 ]]; then
		echo "Error creating folder 'sf-${sf}'"
		exit 1
	fi
	mv *.tbl sf-${sf}
	ret=$?
	if [[ $ret -ne 0 ]]; then
		echo "Error moving files to subfolder 'sf-${sf}'"
		exit 1
	else
		echo "generated and moved tables for sf ${sf}"
	fi
	ret=$?
	if [[ $ret -ne 0 ]]; then
		echo "Error moving tables to subfolder"
		exit 1
	fi
	pushd sf-${sf}
	for tab in customer date lineorder part supplier; do
		if [[ ! -e ${tab}_header.csv ]]; then
			ln -s ../sf-1/${tab}_header.csv ${tab}_header.csv
			ret=$?
			if [[ $ret -ne 0 ]]; then
				echo "Error creating softlink ${tab}_header.csv -> ../sf-1/${tab}_header.csv"
				exit 1
			fi
		fi
		if [[ ! -e ${tab}AN_header.csv ]]; then
			ln -s ../sf-1/${tab}AN_header.csv ${tab}AN_header.csv
			ret=$?
			if [[ $ret -ne 0 ]]; then
				echo "Error creating softlink ${tab}AN_header.csv -> ../sf-1/${tab}AN_header.csv"
				exit 1
			fi
		fi
		if [[ ! -e ${tab}AN.tbl ]]; then
			ln -s ${tab}.tbl ${tab}AN.tbl
			ret=$?
			if [[ $ret -ne 0 ]]; then
				echo "Error creating softlink ${tab}AN.tbl -> ${tab}.tbl"
				exit 1
			fi
		fi
	done
	../../build/Release/ssbm-dbsize_scalar -d . 
	ret=$?
	if [[ $ret -ne 0 ]]; then
		echo "SF ${sf}: error executing ssbm-dbsize_scalar"
		exit 1
	fi
	rm -f *.tbl
	popd
done

