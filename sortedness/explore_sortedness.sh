#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Illegal number of parameters"
    echo "Use one of following formats: "
    echo -e " \t ./explore_sortedness.sh ./input_dir/"
    exit 0
fi

DIRECTORY=$1

LOG_DIR="./lsm_fixedl5_t4_levelled_trivialoff/logs"
LOG_FILE="log"
LOG_EXT=".txt"

mkdir -p $LOG_DIR

NUM=500000000

# run workload generator for NUM+OSMBuffer size elements first 
# ./run_workload_generator.sh m 101000000 ${L_P} bin
# ./run_workload_generator.sh m 1010000000 ${L_P} bin
# ./run_workload_generator.sh m 505000000 5 bin

make

for FILE in $DIRECTORY/*; do 
    
    NO_ELEMS="$(cut -d'_' -f2 <<<"${FILE}")"
    NOISE_PERCENTAGE="$(cut -d'_' -f3 <<<"${FILE}")"
    L_PERCENTAGE="$(cut -d'_' -f4 <<<"${FILE}")"
    ELEMS="$(cut -d'-' -f1 <<<"${NO_ELEMS}")"
    NOISE="$(cut -d'-' -f1 <<<"${NOISE_PERCENTAGE}")"
    L="$(cut -d'-' -f1 <<<"${L_PERCENTAGE}")"

    printf "\n******************************************* EXECUTING NEW FILE ******************************************\n"
    echo "Input file: ${FILE}" 
    echo "K%,l%: $NOISE, $L"

    
    printf "\tRunning workload with LSM tree*\n"
    echo "echo 3 > /proc/sys/vm/drop_caches"

    # clear db directory before every run 
    rm -r db_working_home/*

    ./explore_sortedness -i ${FILE} -p ./db_working_home > "${LOG_DIR}/${LOG_FILE}_${NOISE}_${L}${LOG_EXT}"
    # break

    # rm ${FILE}   
done

# rm -r ./workload/${L_P}-L/*
