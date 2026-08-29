PREFIX?=/usr/local
BINDIR=$(DESTDIR)$(PREFIX)/bin

CFLAGS ?= -O2 -flto=auto
override CFLAGS+=-Wall -Wextra -Werror -Wformat=2 -g
override LDFLAGS?=
override LDFLAGS+=-flto=auto

LDLIBS?=
LDLIBS+=-lrt -lpthread

INSTALL?=install
PKG_CONFIG?=pkg-config

VERS=$(shell ./vers.sh)
TARG=beanstalkd
MOFILE=main.o
OFILES=\
	linux.o\
	conn.o\
	crc32c.o\
	file.o\
	heap.o\
	job.o\
	ms.o\
	net.o\
	primes.o\
	prot.o\
	serv.o\
	time.o\
	tube.o\
	util.o\
	vers.o\
	walg.o\

TOFILES=\
	testheap.o\
	testheap2.o\
	testjobs.o\
	testjob2.o\
	testms.o\
	testms2.o\
	testserv.o\
	testserv2.o\
	testtube.o\
	testutil.o\
	testutil2.o\
	teststress.o\
	testcore.o\
	testprot2.o\
	testwal2.o\
	testinject.o\
	testinject2.o\

HFILES=\
	dat.h\

# systemd support can be configured via USE_SYSTEMD:
#        no: disabled
#       yes: enabled, build fails if libsystemd is not found
# otherwise: enabled if libsystemd is found
ifneq ($(USE_SYSTEMD),no)
ifeq ($(shell $(PKG_CONFIG) --exists libsystemd && echo $$?),0)
	LDLIBS+=$(shell $(PKG_CONFIG) --libs libsystemd)
	CPPFLAGS+=-DHAVE_LIBSYSTEMD
else
ifeq ($(USE_SYSTEMD),yes)
$(error USE_SYSTEMD is set to "$(USE_SYSTEMD)", but $(PKG_CONFIG) cannot find libsystemd)
endif
endif
endif

CLEANFILES=\
	vers.c\
	$(wildcard *.gc*)

.PHONY: all
all: $(TARG)

$(TARG): $(OFILES) $(MOFILE)
	$(LINK.o) -o $@ $^ $(LDLIBS)

.PHONY: install
install: $(BINDIR)/$(TARG)

$(BINDIR)/%: %
	$(INSTALL) -d $(dir $@)
	$(INSTALL) $< $@

CLEANFILES+=$(TARG)

$(OFILES) $(MOFILE): $(HFILES)

# crc32c.c selects its implementation via predefined macros (SSE4.2,
# ARM ACLE, or a portable software table). The SSE4.2 path needs
# -msse4.2; add it only when the compiler targets x86_64 so other
# arches keep their baseline. Detect via the compiler's predefined
# macros (not uname) so cross-builds work.
ifneq ($(shell $(CC) -dM -E - </dev/null 2>/dev/null | grep __x86_64__),)
crc32c.o: override CFLAGS += -msse4.2
endif

# Test objects must stay inspectable by nm for ct/gen: slim-LTO
# bytecode objects export no symbols, which would silently generate
# an empty test list and a falsely green `make check`.
$(TOFILES): override CFLAGS += -fno-lto

CLEANFILES+=$(wildcard *.o)

.PHONY: clean
clean:
	rm -f $(CLEANFILES)

.PHONY: check
check: ct/_ctcheck
	ct/_ctcheck

.PHONY: bench
bench: ct/_ctcheck
	ct/_ctcheck -b

WRAP_FLAGS=\
	-Wl,--wrap,malloc\
	-Wl,--wrap,calloc\
	-Wl,--wrap,realloc\
	-Wl,--wrap,write\
	-Wl,--wrap,writev\
	-Wl,--wrap,read\
	-Wl,--wrap,open\
	-Wl,--wrap,ftruncate\
	-Wl,--wrap,unlink\
	-Wl,--wrap,fdatasync\
	-Wl,--wrap,stat\
	-Wl,--wrap,pthread_create\
	-Wl,--wrap,epoll_pwait

ct/_ctcheck: ct/_ctcheck.o ct/ct.o $(OFILES) $(TOFILES)
	$(LINK.o) $(WRAP_FLAGS) -o $@ $^ $(LDLIBS)

ct/_ctcheck.o: ct/_ctcheck.c

ct/_ctcheck.c: $(TOFILES) ct/gen
	ct/gen $(TOFILES) >$@.part
	mv $@.part $@

ct/ct.o ct/_ctcheck.o: ct/ct.h ct/internal.h

$(TOFILES): $(HFILES) ct/ct.h
testinject.o testinject2.o: testinject.h

# Rebuild objects when the effective flags change (e.g. make, then
# make pgo, then make check): record CC/flags in a stamp file that is
# rewritten only on change, and make every object depend on it.
CFLAGS_STAMP=.cflags-stamp
$(CFLAGS_STAMP): FORCE
	@printf '%s\n' '$(CC) $(CPPFLAGS) $(CFLAGS) $(LDFLAGS)' >$@.tmp
	@if ! cmp -s $@.tmp $@; then mv $@.tmp $@; else rm $@.tmp; fi
.PHONY: FORCE
FORCE:

$(OFILES) $(MOFILE) $(TOFILES) ct/ct.o ct/_ctcheck.o: $(CFLAGS_STAMP)

CLEANFILES+=$(CFLAGS_STAMP)

CLEANFILES+=$(wildcard ct/_* ct/*.o ct/*.gc*)

ifneq ($(shell ./verc.sh),$(shell cat vers.c 2>/dev/null))
.PHONY: vers.c
endif
vers.c:
	./verc.sh >vers.c

doc/beanstalkd.1 doc/beanstalkd.1.html: doc/beanstalkd.ronn
	ronn $<

# Profile-Guided Optimization (PGO)
#
# Usage:
#   make pgo-instrument PGO_REMOTE_DIR=/var/lib/beanstalkd/pgo
#     -> deploy, run under real load, then systemctl stop (SIGTERM flushes gcda)
#   scp remote:PGO_REMOTE_DIR/*.gcda pgo/
#     -> strip path prefix: for f in pgo/*.gcda; do mv "$f" "pgo/$$(echo "$f" | sed 's/.*#//')"; done
#   make pgo
#

PGO_DIR = pgo
PGO_REMOTE_DIR ?= /var/lib/beanstalkd/pgo

.PHONY: pgo-instrument
pgo-instrument:
	$(MAKE) CFLAGS="-O2 -fprofile-generate=$(PGO_REMOTE_DIR) -fprofile-prefix-path=$(CURDIR)" \
	        LDFLAGS="-fprofile-generate=$(PGO_REMOTE_DIR)"

.PHONY: pgo
pgo:
	$(MAKE) CFLAGS="-O2 -flto=auto -fprofile-use=$(CURDIR)/$(PGO_DIR) -fprofile-prefix-path=$(CURDIR) -fprofile-correction -Wno-missing-profile"
