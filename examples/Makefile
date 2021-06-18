
SUBDIRS := $(wildcard */.)
SUBDIRSCLEAN := $(addsuffix clean,$(SUBDIRS))

all: $(SUBDIRS)

$(SUBDIRS):
	$(MAKE) -C $@

clean: $(SUBDIRSCLEAN)

$(SUBDIRSCLEAN):
	$(MAKE) -C $(subst clean,,$@) clean

.PHONY: all $(SUBDIRS) $(SUBDIRSCLEAN) clean
