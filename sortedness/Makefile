# import stuff from makefile under examples/
include ../make_config.mk

ifndef DISABLE_JEMALLOC
	ifdef JEMALLOC
		PLATFORM_CXXFLAGS += -DROCKSDB_JEMALLOC -DJEMALLOC_NO_DEMANGLE
	endif
	EXEC_LDFLAGS := $(JEMALLOC_LIB) $(EXEC_LDFLAGS) -lpthread
	PLATFORM_CXXFLAGS += $(JEMALLOC_INCLUDE)
endif

ifneq ($(USE_RTTI), 1)
	CXXFLAGS += -fno-rtti
endif

CFLAGS += -Wstrict-prototypes

.PHONY: clean librocksdb

# new make stuff

all: explore_sortedness

sortedness_data_generator:
	g++ -g args.hxx progressbar.hpp sortedness_data_generator.cpp -o sortedness_data_generator

explore_sortedness: librocksdb explore_sortedness.cc env.cc workload_stats.cc aux_time.cc stats.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ env.cc workload_stats.cc aux_time.cc stats.cc ../librocksdb.a -I../include -O2 -g -std=c++11  $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

.c.o:
	$(CC) $(CFLAGS) -c $< -o $@ -I../include

work_gen: 
	g++ -g work_gen.cpp -std=c++0x -o ./work_gen.o

clear:
	rm -rf ./explore_sortedness
	rm -rf ./work_gen.o
	rm -rf ./db_working_home/*
librocksdb:
	cd .. && $(MAKE) static_lib