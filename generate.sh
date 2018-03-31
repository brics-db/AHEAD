#!/bin/bash

SCRIPT_DATABASE_GENERATED_FILE=generated

function untar_headers {
	tar -xaf headers.tgz
}

declare -A table_names
table_names=([c]='customer' [d]='date' [l]='lineorder' [p]='part' [s]='supplier')

if [[ ! -d database/headers ]]; then
	untar_headers
else
	for tab in customer date lineorder part supplier; do
		if [[ ! -f "${tab}_header.csv" || ! -f "${tab}AN_header.csv" ]]; then
			untar_headers;break
		fi
	done
fi

pushd database

for sf in $(seq 1 10); do
	echo "Generating data for scale factor ${sf}"
	sync
	echo "  * synced all files (sync)"

	if [[ -f "sf-${sf}/${SCRIPT_DATABASE_GENERATED_FILE}" ]]; then
		echo "  * I assume all files for scale factor ${sf} are already generated"
		continue
	fi

	all_existing=1
	for tab in c d l p s; do
		filename="${table_names[$tab]}.tbl"
		echo -n "  * ${tab}: ${filename} "
		if [[ -f "$filename" || -f "sf-${sf}/$filename" ]]; then
			echo "exists"
		else
			all_existing=0
			./dbgen -s ${sf} -T ${tab} -v 1>/dev/null 2>/dev/null
			ret=$?
			if [[ ! $ret -eq 0 ]]; then
				echo "Error"
				popd
				exit $ret
			fi
			sync
			echo "created"
		fi
	done

	if [[ ${all_existing} == 0 ]]; then
		sync
		echo "  * synced all files (sync)"
		mkdir -p sf-${sf}
		ret=$?
		if [[ $ret -ne 0 ]]; then
			popd
			echo "  * Error creating folder 'sf-${sf}'"
			exit $ret
		fi
#		ls -lAh *.tbl
		mv *.tbl sf-${sf}/
		ret=$?
		if [[ $ret -ne 0 ]]; then
			popd
			echo "  * Error moving files to subfolder 'sf-${sf}'"
			exit $ret
		fi
		echo "  * generated and moved tables for sf ${sf}"
	else
		echo "  * all tables already exists for sf ${sf}"
	fi

	pushd sf-${sf}

	for tab in customer date lineorder part supplier; do
		if [[ ! -f "${tab}.tbl" ]]; then
			popd;popd
			echo "  * Error: data file '${tab}.tbl' does not exist"
			exit 1
		fi
		if [[ ! -f ${tab}_header.csv ]]; then
			ln -s ../headers/${tab}_header.csv ${tab}_header.csv
			ret=$?
			if [[ $ret -ne 0 ]]; then
				popd;popd
				echo "  * Error creating softlink ${tab}_header.csv -> ../headers/${tab}_header.csv"
				exit $ret
			fi
		else
			echo "  * ${tab}_header.csv exists"
		fi
		if [[ ! -f ${tab}AN_header.csv ]]; then
			ln -s ../headers/${tab}AN_header.csv ${tab}AN_header.csv
			ret=$?
			if [[ $ret -ne 0 ]]; then
				echo "  * Error creating softlink ${tab}AN_header.csv -> ../headers/${tab}AN_header.csv"
				exit $ret
			fi
			#sed -e '/TINYINT/RESTINY/G' -e '/SHORTINT/RESSHORT/g' -e '/INTEGER/RESINT/g' -i ${tab}AN_header.csv
		else
			echo "  * ${tab}AN_header.csv exists"
		fi
		if [[ ! -f ${tab}AN.tbl ]]; then
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

	echo "========================================================"
	echo "=== GENERATING SMALLER DATABASE FILES. DO NOT ABORT! ==="
	echo "========================================================"
	../../build/Release/ssbm-dbsize_scalar -d . 
	ret=$?
	if [[ $ret -ne 0 ]]; then
		echo "  * Error executing ssbm-dbsize_scalar"
		exit $ret
	fi

	touch "${SCRIPT_DATABASE_GENERATED_FILE}"
	sync

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

popd

