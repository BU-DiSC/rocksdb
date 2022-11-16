export OUT_CSV=results.csv
export DB=POSTGRES
export N=505000000
export WORKLOAD_OPT=2
export NUM_QUERIES=3200000
export NUM_PERCENT=80

export ALPHA=1
export BETA=1

# rm workloads/*
# K=0 L=5 ./benchmark.sh $WORKLOAD_OPT $NUM_PERCENT $NUM_QUERIES
# # rm workloads/*
# K=100 L=1 ./benchmark.sh $WORKLOAD_OPT $NUM_PERCENT $NUM_QUERIES
# # rm workloads/*
# K=50 L=1 ./benchmark.sh $WORKLOAD_OPT $NUM_PERCENT $NUM_QUERIES
# # rm workloads/*
# K=25 L=1 ./benchmark.sh $WORKLOAD_OPT $NUM_PERCENT $NUM_QUERIES
# # rm workloads/*
# K=10 L=1 ./benchmark.sh $WORKLOAD_OPT $NUM_PERCENT $NUM_QUERIES
# # rm workloads/*
# K=5 L=1 ./benchmark.sh $WORKLOAD_OPT $NUM_PERCENT $NUM_QUERIES
# # rm workloads/*
# K=1 L=1 ./benchmark.sh $WORKLOAD_OPT $NUM_PERCENT $NUM_QUERIES
# # rm workloads/*
# K=1 L=5 ./benchmark.sh $WORKLOAD_OPT $NUM_PERCENT $NUM_QUERIES
# # rm workloads/*
# K=1 L=10 ./benchmark.sh $WORKLOAD_OPT $NUM_PERCENT $NUM_QUERIES
# # rm workloads/*
# K=1 L=25 ./benchmark.sh $WORKLOAD_OPT $NUM_PERCENT $NUM_QUERIES
# # rm workloads/*
# K=1 L=50 ./benchmark.sh $WORKLOAD_OPT $NUM_PERCENT $NUM_QUERIES
# # rm workloads/*
# K=1 L=100 ./benchmark.sh $WORKLOAD_OPT $NUM_PERCENT $NUM_QUERIES
# # rm workloads/*
# K=10 L=5 ./benchmark.sh $WORKLOAD_OPT $NUM_PERCENT $NUM_QUERIES
# # rm workloads/*
# K=5 L=5 ./benchmark.sh $WORKLOAD_OPT $NUM_PERCENT $NUM_QUERIES
# # rm workloads/*
# K=5 L=10 ./benchmark.sh $WORKLOAD_OPT $NUM_PERCENT $NUM_QUERIES
# # rm workloads/*
# K=10 L=10 ./benchmark.sh $WORKLOAD_OPT $NUM_PERCENT $NUM_QUERIES

# K=100 L=100 ./benchmark.sh $WORKLOAD_OPT $NUM_PERCENT $NUM_QUERIES

K=100 L=50 ./benchmark.sh $WORKLOAD_OPT $NUM_PERCENT $NUM_QUERIES