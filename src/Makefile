# To compile and test a lab solution.  The lab name is one of the names
# in the `all` target.

# Use `RUN="-run Wc` to run the Wc test; for example:
# `$ make RUN="-run Wc" mr` runs Wc test of the mr tests.

# Use `RAFT=--raft-state-machine` to run with an alternative raft
# implementation than the default in raft1.

VERB=-v
RACE=-race
VER=$(shell go version | grep -q 'go1.17.[012345]'; echo $$?)

MAINS=kvsrv1 raft1 kvraft1
MAIN_BUILDS=$(addsuffix -build, $(MAINS))

all: mr kvsrv1 lock1 raft1 rsm1 kvraft1 shardkv

$(addsuffix -build, $(MAINS)): %-build : .FORCE check-version
	go build $(RACE) -o main/$*d main/$*d.go

$(MAINS): % : .FORCE check-version %-build
	cd $@ && go test $(VERB) $(RACE) $(RUN) $(RAFT)

lock1-build: .FORCE kvsrv1-build ;

lock1: .FORCE check-version lock1-build
	cd kvsrv1/lock; go test $(VERB) $(RACE) $(RUN)

rsm1-build: .FORCE check-version
	go build $(RACE) -o main/rsm1d main/rsm1d.go

rsm1: .FORCE check-version rsm1-build
	cd kvraft1/rsm; go test $(VERB) $(RACE) $(RUN) $(RAFT)

shardkv: .FORCE check-version shardkv-build
	cd shardkv1; go test -timeout 15m $(VERB) $(RACE) $(RUN) $(RAFT)

shardkv-build: .FORCE check-version
	go build $(RACE) -o main/kvsrv1d main/kvsrv1d.go
	go build $(RACE) -o main/shardgrp1d main/shardgrp1d.go

mr: .FORCE check-version mr-build
	cd mr; go test $(VERB) $(RACE) $(RUN)

mr-build: .FORCE check-version
	go build $(RACE) -o main/mrsequential main/mrsequential.go
	go build $(RACE) -o main/mrcoordinator main/mrcoordinator.go
	go build $(RACE) -o main/mrworker main/mrworker.go&
	(cd mrapps && go build $(RACE) -buildmode=plugin wc.go) || exit 1
	(cd mrapps && go build $(RACE) -buildmode=plugin indexer.go) || exit 1
	(cd mrapps && go build $(RACE) -buildmode=plugin mtiming.go) || exit 1
	(cd mrapps && go build $(RACE) -buildmode=plugin rtiming.go) || exit 1
	(cd mrapps && go build $(RACE) -buildmode=plugin jobcount.go) || exit 1
	(cd mrapps && go build $(RACE) -buildmode=plugin early_exit.go) || exit 1
	(cd mrapps && go build $(RACE) -buildmode=plugin crash.go) || exit 1
	(cd mrapps && go build $(RACE) -buildmode=plugin nocrash.go) || exit 1

.PHONY: .FORCE $(MAINS) $(MAIN_BUILDS)

# -race with plug-ins on x86 MacOS 12 with go1.17 before 1.17.6 sometimes crash.
check-version:
ifeq ($(VER), 0)
	@echo '*** Turning off -race since it may not work on a Mac'
	$(eval RACE := $(shell echo ""))
endif
