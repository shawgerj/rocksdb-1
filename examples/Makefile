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

.PHONY: clean librocksdb

all: shared_external 

shared_external: librocksdb shared_external.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../build/librocksdb.a ../build/libboulevardier.a -I../include -I../boulevardier -O2 -std=c++14 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

clean:
	rm -rf ./shared_external 

librocksdb:
	cd ../build && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j4
