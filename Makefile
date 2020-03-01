INCLUDE = src

.PHONY: all
all:


### Build Commands ###
.PHONY: $(FILE)-test

utils.lib: $(wildcard $(INCLUDE)/distribute/utils/*.d)
	dmd -lib -of=utils.lib -I=$(INCLUDE) $(wildcard $(INCLUDE)/distribute/utils/*.d)

$(FILE).exe: utils.lib
	dmd -I=$(INCLUDE) utils.lib $(INCLUDE)/distribute/$(FILE).d

$(FILE)-test: utils.lib
	dmd -I=$(INCLUDE) utils.lib -unittest -run $(INCLUDE)/distribute/$(FILE).d


### Clean Commands ###
.PHONY: clean clean-libs

clean-libs:
	rm -f $(wildcard *.lib)

clean: clean-libs
	rm -f $(wildcard *.obj)
	rm -f $(wildcard *.exe)