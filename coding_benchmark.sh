#!/bin/bash

CB_SOURCEDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
CB_SUBMODULE=coding_benchmark
CB_GITREPO=https://github.com/brics-db/coding_benchmark
CB_BUILDDIR=build
CB_BUILDTYPE=Release
CB_EXEC=benchmark16
CB_OUTFILE=benchmark.out
CB_ERRFILE=benchmark.err

source "${CB_SOURCEDIR}/common.conf"

AHEAD_prepare_scalinggovernor_and_turboboost

# For the reproducibility, use the submodule coding_benchmark
if [[ -z "$(ls -A ${CB_SUBMODULE})" ]]; then
	AHEAD_echo "Submodule not present."
	AHEAD_sub_begin
	AHEAD_echo -n "Syncing..."
	AHEAD_run_hidden_output git submodule sync "${CB_SUBMODULE}" || AHEAD_exit $?
    AHEAD_echo -n "Fetching..."
	AHEAD_run_hidden_output git submodule update --init --recursive "${CB_SUBMODULE}" || AHEAD_exit $?
	AHEAD_sub_end
fi

AHEAD_echo "Compiling."
AHEAD_sub_begin
AHEAD_pushd "${CB_SOURCEDIR}/${CB_SUBMODULE}" || AHEAD_exit $?
AHEAD_echo -n "Invoking cmake..."
AHEAD_run_hidden_output ./bootstrap.sh || AHEAD_exit $?
AHEAD_pushd "${CB_BUILDDIR}/${CB_BUILDTYPE}" || AHEAD_exit $?
AHEAD_echo -n "Invoking make..."
AHEAD_run_hidden_output make -j$(nproc) || AHEAD_exit $?
AHEAD_sub_end

mkdir -p "${AHEAD_PAPER_RESULTS_CB}" || { ret=$?; AHEAD_echo "Could not create path '${AHEAD_PAPER_RESULTS_CB}'"; AHEAD_exit $ret; }
AHEAD_echo -n "Running benchmark..."
"./${CB_EXEC}" 1>"${CB_OUTFILE}" 2>"${CB_ERRFILE}" && echo " Done." || { ret=$?; echo " Error!"; cat ${CB_ERRFILE} >&2; exit $ret; }
mv "${CB_OUTFILE}" "${CB_ERRFILE}" "${AHEAD_PAPER_RESULTS_CB}" || AHEAD_exit $? "Could not move files '${CB_BUILDDIR}/${CB_BUILDTYPE}/benchmark.out' and '${CB_BUILDDIR}/${CB_BUILDTYPE}/benchmark.err' to destination '${AHEAD_PAPER_RESULTS_CB}'"

AHEAD_popd
AHEAD_popd
