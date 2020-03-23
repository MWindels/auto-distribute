INCLUDE = src

.PHONY: all
all:


### Build Commands ###
.PHONY: test-id test-entry test-pool test-consensus test-distributed

utils.lib: $(wildcard $(INCLUDE)/distribute/utils/*.d)
	dmd -w -lib -of=utils.lib -I=$(INCLUDE) $(wildcard $(INCLUDE)/distribute/utils/*.d)

test-id: utils.lib
	dmd -I=$(INCLUDE) utils.lib -w -main -unittest -run $(INCLUDE)/distribute/id.d

test-entry: utils.lib
	dmd -I=$(INCLUDE) utils.lib -w -main -unittest -run $(INCLUDE)/distribute/entry.d

test-pool: utils.lib
	dmd -I=$(INCLUDE) utils.lib -w -main -unittest -run $(INCLUDE)/distribute/pool.d

test-consensus: utils.lib
	rdmd -I=$(INCLUDE) utils.lib -w -main -unittest $(INCLUDE)/distribute/consensus.d
	#dmd -I=$(INCLUDE) utils.lib -w -main -unittest -run $(INCLUDE)/distribute/consensus.d $(INCLUDE)/distribute/pool.d $(INCLUDE)/distribute/entry.d $(INCLUDE)/distribute/id.d

test-distributed: utils.lib
	dmd -I=$(INCLUDE) utils.lib -w -main -unittest -run $(INCLUDE)/distribute/distributed.d $(INCLUDE)/distribute/distributed_base.d


### Clean Commands ###
.PHONY: clean clean-libs

clean-libs:
	rm -f $(wildcard *.lib)

clean: clean-libs
	rm -f $(wildcard *.obj)
	rm -f $(wildcard *.exe)