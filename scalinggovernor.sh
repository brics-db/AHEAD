#!/bin/bash

if [[ -z $(which cpufreq-set) ]]; then
    echo "cpufrequtils is not installed. Run 'sudo apt-get install cpufrequtils' to install it." >&2
    exit 1
fi

##
## Legacy code
##
#cores=$(cat /proc/cpuinfo | grep processor | awk '{print $3}')
#case "$1" in
#    set)
#        if [[ $# -ne 2 ]]; then
#            echo $"Usage: $0 {set <governor>|list|show}"
#            exit 1
#        fi
#        for core in $cores; do
#            cpufreq-set -c ${core} -g $2;
#            if [[ $? -ne 0 ]]; then
#                exit 1
#            fi
#        done
#        ;;
#    list)
#        for core in $cores; do
#            echo -n "CPU ${core}: "
#            cpufreq-info -c ${core} -g $2;
#            ret=$?
#            if [[ ${ret} -ne 0 ]]; then
#                echo "Error! cpufreq-info returned status ${ret}"
#                exit 1
#            fi
#        done
#        exit 0
#        ;;
#    show)
#        ;;
#    *)
#        echo $"Usage: $0 {set <governor>|list|show}"
#        exit 1
#esac
#
#for core in $cores; do
#    echo -n "CPU ${core}: "
#    cat "/sys/devices/system/cpu/cpu${core}/cpufreq/scaling_governor"
#done

array_contains_any_substring_unique () {
    local seeking="$1"
    local array=()
    while [[ ! -z "$2" ]]; do
        array+=("$2"); shift
    done
    #echo "#${!array}: ${array[@]}" 1>&2
    local count=0
    local found=""
    for element in "${array[@]}"; do
        if [[ $element == "$seeking"* ]]; then
            found=$element
            ((++count))
        fi
    done
    if [[ $count == 1 ]]; then
        echo "$found"
    else
        echo ""
    fi
}

find_governors () {
    if (( $# == 0 )); then
        local governors=()
        for core in $cores; do
            local array=$(cpufreq-info -c ${core} -g $2)
            ret=$?
            if [[ ${ret} -ne 0 ]]; then
                echo "Error! cpufreq-info returned status ${ret}" >&2
                exit 1
            fi
            #echo -n "array=($array)" 1>&2
            IFS=', ' read -r -a govs <<< "${array[@]}"
            #echo -n "govs=(${govs[@]})" 1>&2
            for gov in "${govs[@]}"; do
                #echo -n " x: $gov " 1>&2
                found=$( array_contains_any_substring_unique "${gov}" "${governors[@]}" )
                if [[ -z "$found" ]]; then
                    #echo -n " y: $gov" 1>&2
                    governors+=("$gov")
                #else
                    #echo -n " z: ($found) ($gov)" 1>&2
                fi
            done
            #echo 1>&2
        done
        #echo "${governors[@]}" 1>&2
        echo "${governors[@]}"
    elif (( $# == 1 )); then
        /bin/bash -c "cpufreq-info -c $1 -g"
    else
        usage
    fi
}

usage() {
    echo $"Usage: $0 {set [<core id>] <governor>|get [<core id>]|available [<core id>]}"
    exit 2
}

quit () {
    echo "$1" >&2
    exit 1
}

cores=$(cat /proc/cpuinfo | grep processor | awk '{print $3}')
case "$1" in
    s)  ;&
    se) ;&
    set)
        if (( $# < 2 )) || (( $# > 3 )); then
            usage
        fi
        if (( $# == 2 )); then
            #[[ $2 =~ ^[a-zA-Z]+$ ]] || quit "Parameter must be a valid scaling governor!"
            requested=$2
            array=$( find_governors )
        else #if (( $# == 3 )); then
            [[ $2 =~ ^[0-9]+$ ]] || quit "Parameter is not a valid number!"
            requested=$3
            array=$( find_governors $2 )
        fi
        IFS=', ' read -r -a govs <<< "$array"
        modus=$( array_contains_any_substring_unique "${requested}" "${govs[@]}" )
        [[ ! -z $modus ]] || quit "Governor \"${requested}\" is not available!"
        if (( $# == 2 )); then
            for core in $cores; do
                cpufreq-set -c ${core} -g $modus
                if [[ $? -ne 0 ]]; then
                    exit 1
                fi
            done
        else
            cpufreq-set -c $2 -g $modus
        fi
        #echo "Scaling Governor was set to \"$modus\""
        #$0 get
        ;;

    g)    ;&
    ge)   ;&
    get)
        if (( $# == 1 )); then
            cpufreq-info -p | awk '{print $3}'
            for core in $cores; do
                echo -n "CPU ${core}: "
                cat "/sys/devices/system/cpu/cpu${core}/cpufreq/scaling_governor"
            done
        elif (( $# == 2 )); then
            [[ $2 =~ ^[0-9]+$ ]] || quit "parameter is not a valid number!" >&2
            [[ -e "/sys/devices/system/cpu/cpu$2" ]] || quit "Invalid core number \"$2\"!"
            [[ -e "/sys/devices/system/cpu/cpu$2/cpufreq/scaling_governor" ]] || quit "No Scaling governor available for core number \"$2\"!"
            cat "/sys/devices/system/cpu/cpu$2/cpufreq/scaling_governor"
        else
            usage
        fi
        ;;

    a)         ;&
    av)        ;&
    ava)       ;&
    avai)      ;&
    avail)     ;&
    availa)    ;&
    availab)   ;&
    availabl)  ;&
    available)
        if (( $# == 1 )); then
            /bin/bash -c "cpufreq-info -g"
        elif (( $# == 2 )); then
            [[ $2 =~ ^[0-9]+$ ]] || quit "Parameter is not a valid number!"
            find_governors $2
        else
            usage
        fi
        ;;

    *)
        usage
        ;;
esac
