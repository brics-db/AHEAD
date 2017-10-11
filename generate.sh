#!/bin/bash

for sf in $(seq 5 10); do
	echo "Generating data for scale factor ${sf}"
	sync
	echo "  * synced all files (sync)"

	for tab in c d l p s; do
		echo -n "  * ${tab} "
		./dbgen -s ${sf} -T ${tab} -v 1>/dev/null 2>/dev/null
		ret=$?
		if [[ ! $ret -eq 0 ]]; then
			echo "Error!"
			exit $ret
		else
			echo "Success!"
		fi
	done
	mkdir -p sf-${sf}
	ret=$?
	if [[ $ret -ne 0 ]]; then
		echo "  * Error creating folder 'sf-${sf}'"
		exit $ret
	fi

	ls -lAh *.tbl
	mv *.tbl sf-${sf}/
	ret=$?
	if [[ $ret -ne 0 ]]; then
		echo "  * Error moving files to subfolder 'sf-${sf}'"
		exit $ret
	else
		echo "  * generated and moved tables for sf ${sf}"
	fi

	pushd sf-${sf}

	for tab in customer date lineorder part supplier; do
		if [[ ! -e "${tab}.tbl" ]]; then
			echo "  * Error: data file '${tab}.tbl' does not exist"
			exit 1
		fi
		if [[ ! -e ${tab}_header.csv ]]; then
			ln -s ../sf-1/${tab}_header.csv ${tab}_header.csv
			ret=$?
			if [[ $ret -ne 0 ]]; then
				echo "  * Error creating softlink ${tab}_header.csv -> ../sf-1/${tab}_header.csv"
				exit $ret
			fi
		else
			echo "  * ${tab}_header.csv exists"
		fi
		if [[ ! -e ${tab}AN_header.csv ]]; then
			ln -s ../sf-1/${tab}AN_header.csv ${tab}AN_header.csv
			ret=$?
			if [[ $ret -ne 0 ]]; then
				echo "  * Error creating softlink ${tab}AN_header.csv -> ../sf-1/${tab}AN_header.csv"
				exit $ret
			fi
			#sed -e '/TINYINT/RESTINY/G' -e '/SHORTINT/RESSHORT/g' -e '/INTEGER/RESINT/g' -i ${tab}AN_header.csv
		else
			echo "  * ${tab}AN_header.csv exists"
		fi
		if [[ ! -e ${tab}AN.tbl ]]; then
			ln -s ${tab}.tbl ${tab}AN.tbl
			ret=$?
			if [[ $ret -ne 0 ]]; then
				echo "  * Error creating softlink ${tab}AN.tbl -> ${tab}.tbl"
				exit $ret
			fi
		else
			echo "  * ${tab}AN.tbl exists"
		fi
	done

	../../build/Release/ssbm-dbsize_scalar -d . 
	ret=$?
	if [[ $ret -ne 0 ]]; then
		echo "  * Error executing ssbm-dbsize_scalar"
		exit $ret
	fi

	rm -f *.tbl
	ret=$?
	if [[ $ret -ne 0 ]]; then
		echo "  * Error removing table files"
		exit $ret
	fi

	popd

	/bin/bash -c "ls *.tbl" &>/dev/null
	ret2=$?
	if [[ $ret2 -eq 0 ]]; then
		echo "  * There are still some table files here!"
		rm -f *.tbl
	fi

	sync
	echo "  * synced all files (sync)"

done

