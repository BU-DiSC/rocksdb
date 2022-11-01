#!/bin/bash

if [ -z "$DB" ]; then
  DB="POSTGRES"
fi
if [ -z "$N" ]; then
  N=1000
fi
if [ -z "$K" ]; then
  K=50
fi
if [ -z "$L" ]; then
  L=50
fi
if [ -z "$SEED" ]; then
  SEED=1234
fi
if [ -z "$ALPHA" ]; then
  ALPHA=1
fi
if [ -z "$BETA" ]; then
  BETA=1
fi
if [ -z "$OUTPUT_DIR" ]; then
  OUTPUT_DIR=./workload
fi
if [ -z "$ENTRY_SIZE" ]; then
  ENTRY_SIZE=0
fi

RANDOM=$SEED
WORKLOAD=$OUTPUT_DIR/createdata_N${N}_K${K}_L${L}_S${SEED}_a${ALPHA}_b${BETA}_P${ENTRY_SIZE}

# Full bulk-load = 1
# Insert only = 2
# Mixed with no pre-load = 3
# Mixed with pre-load as bulk load = 4
# Mixed with pre-load as one by one insert = 5
WORKLOAD_OPT=$1

# pre-load threshold as fraction
if [ "$2" ]; then
  NUM_PRELOAD=$((N * $2 / 100))
else
  NUM_PRELOAD=0
fi
if [ "$3" ]; then
  NUM_QUERIES=$3
else
  NUM_QUERIES=0
fi

TMP_FILE=$OUTPUT_DIR/partial.csv
OPERATIONS=$OUTPUT_DIR/operations.sql
PRELOAD=$OUTPUT_DIR/preload.sql
if [ $DB == "POSTGRES" ]; then
  DB_INIT=postgres_init.sql
elif [ $DB == "MONETDB" ]; then
  DB_INIT=monet_init.sql
elif [ $DB == "MYSQL" ]; then
  DB_INIT=mysql_init.sql
fi
LOG_FILE=$WORKLOAD.log
WORKLOAD_FILE=$WORKLOAD.csv

# # first remove files if it exists so we don't mess up statements
# echo $N, $K, $L, $SEED, $ALPHA, $BETA, $ENTRY_SIZE, "$1", $NUM_PRELOAD, $NUM_QUERIES >>$LOG_FILE
if [ ! -f $WORKLOAD_FILE ]; then
  ./sortedness_data_generator -N $N -K $K -L $L -S $SEED -a $ALPHA -b $BETA -o $WORKLOAD_FILE -P $ENTRY_SIZE
fi

