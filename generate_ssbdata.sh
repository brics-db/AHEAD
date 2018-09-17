#!/bin/bash
SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd )"
SSB_DBGEN_SUBMODULE=ssb-dbgen
SSB_DBGEN_GITREPO=https://github.com/valco1994/ssb-dbgen
SSB_DBGEN_BUILDIR=build

# the following sourced file defines the following variables used across several scripts, so that you need to change them only in one place
# A) AHEAD_SCRIPT_BOOTSTRAP
# B)
# C)
# D)
# 1) AHEAD_SCALEFACTOR_MIN
# 2) AHEAD_SCALEFACTOR_MAX
# 3) AHEAD_DB_PATH

source "${SOURCE_DIR}/common.conf"

mkdir -p "${AHEAD_DB_PATH}"

# First, make sure that the cmake files are populated
echo -n " * Bootstrapping AHEAD build files using cmake into '${AHEAD_BUILD_RELEASE_DIR}'..."
AHEAD_run_hidden_output ${AHEAD_SCRIPT_BOOTSTRAP} || exit 1
pushd ${AHEAD_BUILD_RELEASE_DIR} &>/dev/null
echo -n " * Generating AHEAD executables for comapcting the database files..."
AHEAD_run_hidden_output "make" "-j$(nproc)" "${AHEAD_SCRIPT_GENSSB_EXECUTABLE}" "${AHEAD_SCRIPT_GENSSB_EXECUTABLE}${AHEAD_RESTINY32_SUFFIX}" || exit 1
popd &>/dev/null

# For the reproducibility, use the submodule ssb-dbgen to generate the ssb generator and push it to the database directory
git submodule sync "${SSB_DBGEN_SUBMODULE}"
git submodule update --init "${SSB_DBGEN_SUBMODULE}"
pushd "${SSB_DBGEN_SUBMODULE}" &>/dev/null
[[ ! -d "${SSB_DBGEN_BUILDIR}" ]] && mkdir -p "${SSB_DBGEN_BUILDIR}"
pushd "${SSB_DBGEN_BUILDIR}" &>/dev/null
echo " * ssb-dbgen"
echo -n "   * cmake..."
AHEAD_run_hidden_output "cmake" ".." || exit 1
echo -n "   * make..."
AHEAD_run_hidden_output "make" || exit 1

echo "   * Copying dbgen executable to '${AHEAD_DB_PATH}/dbgen'."
cp dbgen ${AHEAD_DB_PATH}

popd &>/dev/null ; popd &>/dev/null

# Now, the actual script
SCRIPT_DATABASE_GENERATED_FILE=generated

function untar_headers {
	tar -xaf headers.tgz
	AHEAD_sync
}

declare -A table_names
table_names=([c]='customer' [d]='date' [l]='lineorder' [p]='part' [s]='supplier')

if [[ ! -d ${AHEAD_DB_PATH}/headers ]]; then
	untar_headers
else
	for tab in customer date lineorder part supplier; do
		if [[ ! -f "${tab}_header.csv" || ! -f "${tab}AN_header.csv" ]]; then
			untar_headers ; break
		fi
	done
fi

pushd ${AHEAD_DB_PATH} &>/dev/null

# generate_ssb (
#                  sf    [required]    The scale factor as an integer
#         path_suffix    [optional]    Special suffix, e.g. "-minbfw1"
#             cmdargs    [optional]    Special cmd args for the .ahead file generator, e.g. "--minbfw 1"
#     executable_path    [optional]    Executable path for generating the smaller database files
# )
function generate_ssb {
	if [[ "$#" == 0 ]]; then
		echo "[ERROR] you must call bash function generate_ssb with at least the scale factor as parameter!" >/dev/stderr
		exit 1
	fi

	arguments="$@"
	sf="$1"
	shift
	path_suffix=
	if [[ "$#" > 0 ]]; then
		path_suffix="$1"
		shift
	fi
	cmdargs=
	if [[ "$#" > 0 ]]; then
		cmdargs="$1"
		shift
	fi
	sf_path="${AHEAD_SCALEFACTOR_PREFIX}${sf}${path_suffix}"
	executable_path="${AHEAD_SCRIPT_GENSSB_EXECUTABLE_PATH}"
	if [[ "$#" > 0 ]]; then
		executable_path="$1"
		shift
	fi

	echo " * Generating data for scale factor ${sf} in ${sf_path}"
	echo "   * arguments: '$arguments'"
	AHEAD_sync

	if [[ -f "${sf_path}/${SCRIPT_DATABASE_GENERATED_FILE}" ]]; then
		echo "   * It seems that all files for scale factor ${sf} are already generated"
		# We can return from the function here.
		return
	fi

	all_existing=1
	for tab in c d l p s; do
		filename="${table_names[$tab]}.tbl"
		echo -n "   * ${tab}: ${filename}..."
		if [[ -f "$filename" || -f "${sf_path}/$filename" ]]; then
			echo " Exists."
		else
			all_existing=0
			./dbgen -s ${sf} -T ${tab} -v 1>/dev/null 2>/dev/null
			ret=$?
			if [[ ! $ret -eq 0 ]]; then
				echo " Error!"
				exit $ret
			fi
			echo " Created."
		fi
	done

	mkdir -p "${sf_path}"
	ret=$?
	if ((ret != 0)); then
		echo "   * Error creating folder '${sf_path}'"
		exit $ret
	fi

	if ((all_existing == 0)); then
		AHEAD_sync
		mv *.tbl "${sf_path}/"
		ret=$?
		if [[ $ret -ne 0 ]]; then
			echo "   * Error moving files to subfolder '${sf_path}'"
			exit $ret
		fi
		echo "   * Generated and moved tables for SF ${sf}"
	else
		echo "   * All tables already exist for SF ${sf}"
	fi

	pushd "${sf_path}" &>/dev/null

	for tab in customer date lineorder part supplier; do
		if [[ ! -f "${tab}.tbl" ]]; then
			echo "   * Error: data file '${tab}.tbl' does not exist"
			exit 1
		fi
		if [[ ! -f ${tab}_header.csv ]]; then
			ln -s ../headers/${tab}_header.csv ${tab}_header.csv
			ret=$?
			if [[ $ret -ne 0 ]]; then
				echo "   * Error creating softlink ${tab}_header.csv -> ../headers/${tab}_header.csv"
				exit $ret
			fi
		else
			echo "  * ${tab}_header.csv exists"
		fi
		if [[ ! -f ${tab}AN_header.csv ]]; then
			ln -s ../headers/${tab}AN_header.csv ${tab}AN_header.csv
			ret=$?
			if [[ $ret -ne 0 ]]; then
				echo "   * Error creating softlink ${tab}AN_header.csv -> ../headers/${tab}AN_header.csv"
				exit $ret
			fi
		else
			echo "   * ${tab}AN_header.csv exists"
		fi
		if [[ ! -f ${tab}AN.tbl ]]; then
			ln -s ${tab}.tbl ${tab}AN.tbl
			ret=$?
			if [[ $ret -ne 0 ]]; then
				echo "   * Error creating softlink ${tab}AN.tbl -> ${tab}.tbl"
				exit $ret
			fi
		else
			echo "   * ${tab}AN.tbl exists"
		fi
	done

	echo -n "   * Generating smaller database files using '${executable_path} -d . ${cmdargs}'. DO NOT ABORT..."
	AHEAD_run_hidden_output "${executable_path}" "-d" "." "${cmdargs}"

	touch "${SCRIPT_DATABASE_GENERATED_FILE}"
	AHEAD_sync

	rm -f *.tbl
	ret=$?
	if ((ret != 0)); then
		echo "   * Error removing table files"
		exit $ret
	fi

	popd &>/dev/null

	/bin/bash -c "ls *.tbl" &>/dev/null
	ret2=$?
	if ((ret2 == 0)); then
		echo "   * There are still some table files here!"
		rm -f *.tbl
	fi

	AHEAD_sync
}

for sf in $(seq "${AHEAD_SCALEFACTOR_MIN}" "${AHEAD_SCALEFACTOR_MAX}"); do
	generate_ssb ${sf}
done

for minbfw in $(seq "${AHEAD_MINBFW_MIN}" "${AHEAD_MINBFW_MAX}"); do
	# Only generate 1 scale factor for the minbfw tests!
	generate_ssb "${AHEAD_MINBFW_SCALEFACTOR}" "${AHEAD_MINBFW_SUFFIX}${minbfw}" "${AHEAD_MINBFW_CMDARG} ${minbfw}"
done
# generate the minbfw=4 data with restiny=32bit
generate_ssb "${AHEAD_MINBFW_SCALEFACTOR}" "${AHEAD_MINBFW_SUFFIX}4" "${AHEAD_MINBFW_CMDARG} 4" "${AHEAD_SCRIPT_GENSSB_EXECUTABLE_PATH}${AHEAD_RESTINY32_SUFFIX}"

echo " * All database generated successfully."
exit 0
