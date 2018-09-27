#!/bin/bash

# the following sourced file defines the following variables used across several scripts, so that you need to change them only in one place
# A) AHEAD_SCRIPT_BOOTSTRAP
# B)
# C)
# D)
# 1) AHEAD_SCALEFACTOR_MIN
# 2) AHEAD_SCALEFACTOR_MAX
# 3) AHEAD_DB_PATH

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd )/common.conf"

mkdir -p "${AHEAD_DB_PATH}" || AHEAD_exit 1 "${AHEAD_SCRIPT_ECHO_INDENT}Could not create mandatory directory '${AHEAD_DB_PATH}'"

# First, make sure that the cmake files are populated
AHEAD_echo -n "Bootstrapping AHEAD build files using cmake into '${AHEAD_BUILD_RELEASE_DIR}'..."
AHEAD_run_hidden_output ${AHEAD_SCRIPT_BOOTSTRAP} || AHEAD_exit &?
AHEAD_pushd ${AHEAD_BUILD_RELEASE_DIR}
AHEAD_echo -n "Generating AHEAD executables for compacting the database files..."
AHEAD_run_hidden_output "make" "-j$(nproc)" "${AHEAD_SCRIPT_GENSSB_EXECUTABLE}" "${AHEAD_SCRIPT_GENSSB_EXECUTABLE}${AHEAD_RESTINY32_SUFFIX}" || AHEAD_exit &?
AHEAD_popd

# For the reproducibility, use the submodule ssb-dbgen to generate the ssb generator and push it to the database directory
AHEAD_echo -n "git submodule update --init --recursive ${SSB_DBGEN_SUBMODULE}..."
AHEAD_run_hidden_output git submodule update --init --recursive "${SSB_DBGEN_SUBMODULE}" || AHEAD_exit &?
AHEAD_echo -n "git submodule sync --recursive ${SSB_DBGEN_SUBMODULE}..."
AHEAD_run_hidden_output git submodule sync --recursive "${SSB_DBGEN_SUBMODULE}" || AHEAD_exit &?
AHEAD_echo -n "git submodule update --recursive ${SSB_DBGEN_SUBMODULE}..."
AHEAD_run_hidden_output git submodule update --recursive "${SSB_DBGEN_SUBMODULE}" || AHEAD_exit &?

AHEAD_echo "Compiling ssb-dbgen executable"
AHEAD_sub_begin
AHEAD_pushd "${SSB_DBGEN_SUBMODULE}"
if [[ ! -d "${SSB_DBGEN_BUILDIR}" ]]; then
	mkdir -p "${SSB_DBGEN_BUILDIR}" || AHEAD_exit 1 "Could not create mandatory directory '${SSB_DBGEN_BUILDIR}'!"
fi
AHEAD_pushd "${SSB_DBGEN_BUILDIR}"
AHEAD_echo -n "cmake..."
AHEAD_run_hidden_output "cmake" ".." || AHEAD_exit &?
AHEAD_echo -n "make..."
AHEAD_run_hidden_output "make" || AHEAD_exit $?
AHEAD_echo "Copying dbgen executable to '${AHEAD_DB_PATH}/dbgen'."
cp dbgen ${AHEAD_DB_PATH}
AHEAD_popd
AHEAD_popd
AHEAD_sub_end

# Now, the actual script
SCRIPT_DATABASE_GENERATED_FILE=generated
SCRIPT_DATABASE_HEADERS_SUBFOLDER=headers

AHEAD_echo "Checking table header files."
AHEAD_sub_begin
function untar_headers {
	tar -xaf headers.tgz
	AHEAD_sync
}
declare -A table_names
table_names=([c]='customer' [d]='date' [l]='lineorder' [p]='part' [s]='supplier')
AHEAD_pushd ${AHEAD_DB_PATH}
if [[ ! -d "${SCRIPT_DATABASE_HEADERS_SUBFOLDER}" ]]; then
	AHEAD_echo -n "Untar files..."
	AHEAD_run_hidden_output untar_headers
else
	allFilesPresent=1
	AHEAD_echo -n "Checking if all files are present..."
	for tab in customer date lineorder part supplier; do
		if [[ ! -f "${SCRIPT_DATABASE_HEADERS_SUBFOLDER}/${tab}_header.csv" || ! -f "${SCRIPT_DATABASE_HEADERS_SUBFOLDER}/${tab}AN_header.csv" ]]; then
			allFilesPresent=0
			echo " Some files are missing. Untar files..."
			AHEAD_run_hidden_output untar_headers
			break
		fi
	done
	((allFilesPresent == 1)) && echo " Yes :-)"
fi
AHEAD_sub_end

# generate_ssb (
#                  sf    [required]    The scale factor as an integer
#         path_suffix    [optional]    Special suffix, e.g. "-minbfw1"
#             cmdargs    [optional]    Special cmd args for the .ahead file generator, e.g. "--minbfw 1"
#     executable_path    [optional]    Executable path for generating the smaller database files
# )
function generate_ssb {
	[[ "$#" == 0 ]] && AHEAD_exit 1 "${AHEAD_SCRIPT_ECHO_INDENT}[ERROR] You must call bash function generate_ssb with at least the scale factor as parameter!" >&2
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

	AHEAD_echo "Generating data for scale factor ${sf} in ${sf_path}"
	AHEAD_sub_begin
	AHEAD_echo "arguments: '$arguments'"

	if [[ -f "${sf_path}/${SCRIPT_DATABASE_GENERATED_FILE}" ]]; then
		AHEAD_echo "It seems that all files for scale factor ${sf} are already generated :-)"
		AHEAD_sub_end
		# We can return from the function here.
		return
	fi

	AHEAD_echo "Generating data"
	AHEAD_sub_begin
	all_existing=1
	for tab in c d l p s; do
		filename="${table_names[$tab]}.tbl"
		AHEAD_echo -n "${filename}:"
		if [[ -f "$filename" || -f "${sf_path}/$filename" ]]; then
			echo " Exists."
		else
			all_existing=0
			echo -n " Creating..."
			AHEAD_run_hidden_output ./dbgen -s ${sf} -T ${tab} -v
		fi
	done
	AHEAD_sub_end

	AHEAD_echo "Moving files"
	mkdir -p "${sf_path}" || AHEAD_exit 1 "${AHEAD_SCRIPT_ECHO_INDENT}[ERROR] Could not create mandatory directoy '${sf_path}'"
	if ((all_existing == 0)); then
		AHEAD_sync
		mv *.tbl "${sf_path}/" || AHEAD_exit 1 "${AHEAD_SCRIPT_ECHO_INDENT}[ERROR] Could not move files to subfolder '${sf_path}'"
		AHEAD_echo "Generated and moved tables for SF ${sf}"
	else
		AHEAD_echo "All tables already exist for SF ${sf}"
	fi

	AHEAD_pushd "${sf_path}"

	AHEAD_echo "Creating softlinks for table header files"
	AHEAD_sub_begin
	for tab in customer date lineorder part supplier; do
		[[ ! -f "${tab}.tbl" ]] && AHEAD_exit "${AHEAD_SCRIPT_ECHO_INDENT}[ERROR] Data file '${tab}.tbl' does not exist"
		if [[ ! -f ${tab}_header.csv ]]; then
			ln -s "../${SCRIPT_DATABASE_HEADERS_SUBFOLDER}/${tab}_header.csv" "${tab}_header.csv" || AHEAD_exit $? "${AHEAD_SCRIPT_ECHO_INDENT}[ERROR] Could not create softlink ${tab}_header.csv -> ../${SCRIPT_DATABASE_HEADERS_SUBFOLDER}/${tab}_header.csv"
		else
			[[ ! -z ${VERBOSE+x} ]] && AHEAD_echo "${tab}_header.csv exists"
		fi
		if [[ ! -f ${tab}AN_header.csv ]]; then
			ln -s "../${SCRIPT_DATABASE_HEADERS_SUBFOLDER}/${tab}AN_header.csv" "${tab}AN_header.csv" || AHEAD_exit $? "${AHEAD_SCRIPT_ECHO_INDENT}[ERROR] Could not create softlink ${tab}AN_header.csv -> ../${SCRIPT_DATABASE_HEADERS_SUBFOLDER}/${tab}AN_header.csv"
		else
			[[ ! -z ${VERBOSE+x} ]] && AHEAD_echo "${tab}AN_header.csv exists"
		fi
		if [[ ! -f ${tab}AN.tbl ]]; then
			ln -s "${tab}.tbl" "${tab}AN.tbl" || AHEAD_exit $? "${AHEAD_SCRIPT_ECHO_INDENT}[ERROR] Could not create softlink ${tab}AN.tbl -> ${tab}.tbl"
		else
			[[ ! -z ${VERBOSE+x} ]] && AHEAD_echo "${tab}AN.tbl exists"
		fi
	done
	AHEAD_sub_end

	AHEAD_echo -n "Generating smaller database files using '${executable_path} -d . ${cmdargs}'. DO NOT ABORT..."
	AHEAD_run_hidden_output "${executable_path}" "-d" "." "${cmdargs}"
	AHEAD_sub_begin
	touch "${SCRIPT_DATABASE_GENERATED_FILE}"
	AHEAD_sync
	AHEAD_sub_end

	AHEAD_echo -n "Removing unneeded files..."
	AHEAD_run_hidden_output rm -f *.tbl || exit $?
	AHEAD_sub_begin
	AHEAD_sync
	AHEAD_sub_end

	AHEAD_popd
	AHEAD_sub_end
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

AHEAD_echo "All database generated successfully."
AHEAD_popd

exit 0
