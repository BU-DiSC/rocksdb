#!/bin/bash

if [ "$#" -lt 4 ]; then
    echo "Illegal number of parameters"
    echo "Use one of following formats: "
    echo -e " \t ./run_workload_generator s total_elements noise l_percentage type; or "
    echo -e " \t ./run_workload_generator m total_elements l_percentage type; "

    exit 0
fi

# rm ./workload_generator.exe 
rm work_gen.o 
# g++ -g workload_generator.cpp -std=c++0x -o ./workload_generator.exe 
g++ -g work_gen.cpp -std=c++0x -o work_gen.o

if [ $1 = 's' ]; then 
    NOISE=($3)
    L_PERCENTAGE=$4
    TYPE=$5

elif [ $1 = 'm' ]; then
    # NOISE=( 5 10 25 40 75 90 100 )
    NOISE=( 0 1 5 10 25 50 )
    # NOISE=( 0 5 50 )
    L_PERCENTAGE=$3
    TYPE=$4

fi

TOTAL_ELEMENTS=$2
WINDOW_SIZE=1
SEED_VALUE=1

mkdir -p workload/
# mkdir -p workload/$L_PERCENTAGE-L/arr/

for N in "${NOISE[@]}"; do 
    
    ./work_gen.o $TOTAL_ELEMENTS $TOTAL_ELEMENTS $WINDOW_SIZE $N $L_PERCENTAGE $SEED_VALUE $TYPE

done
