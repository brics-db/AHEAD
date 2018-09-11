#!/bin/bash

if [[ -z $(which rdmsr) ]]; then
    echo "msr-tools is not installed. Run 'sudo apt-get install msr-tools' to install it." >&2
    exit 1
fi

modprobe msr

cores=$(cat /proc/cpuinfo | grep processor | awk '{print $3}')
for core in $cores; do
    case "$1" in
        enable)
            wrmsr -p${core} 0x1a0 0x850089
            ;;
        disable)
            wrmsr -p${core} 0x1a0 0x4000850089
            ;;
        show)
            ;;
        *)
            echo $"Usage: $0 {enable|disable|show}"
            exit 1
    esac
    state=$(rdmsr -p${core} 0x1a0 -f 38:38)
    if [[ $state -eq 1 ]]; then
        echo "core ${core}: disabled"
    else
        echo "core ${core}: enabled"
    fi
done

