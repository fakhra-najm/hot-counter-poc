CXX_DIR := cpp
LEGACY_MAKEFILE := Makefile.legacy

all: cpp

cpp:
	$(MAKE) -C $(CXX_DIR) all

test: cpp-test

cpp-test:
	$(MAKE) -C $(CXX_DIR) test

legacy-c:
	$(MAKE) -f $(LEGACY_MAKEFILE) all

legacy-test:
	$(MAKE) -f $(LEGACY_MAKEFILE) test

legacy-clean:
	$(MAKE) -f $(LEGACY_MAKEFILE) clean

clean:
	$(MAKE) -C $(CXX_DIR) clean
	$(MAKE) -f $(LEGACY_MAKEFILE) clean

.PHONY: all cpp test cpp-test legacy-c legacy-test legacy-clean clean
