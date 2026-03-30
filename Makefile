PREFIX?=/usr/local
BINDIR=$(DESTDIR)$(PREFIX)/bin

CFLAGS ?= -O2 -flto=auto
override CFLAGS+=-Wall -Werror -Wformat=2 -g
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

MWTOFILES=\
	testworker.o\

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

.PHONY: check-mw
check-mw: $(TARG) ct/_ctcheck_mw
	ct/_ctcheck_mw

ct/_ctcheck_mw: ct/_ctcheck_mw.o ct/ct.o $(OFILES) $(MWTOFILES)
	$(LINK.o) -o $@ $^ $(LDLIBS)

ct/_ctcheck_mw.o: ct/_ctcheck_mw.c

ct/_ctcheck_mw.c: $(MWTOFILES) ct/gen
	ct/gen $(MWTOFILES) >$@.part
	mv $@.part $@

$(MWTOFILES): $(HFILES) ct/ct.h

ct/_ctcheck: ct/_ctcheck.o ct/ct.o $(OFILES) $(TOFILES)
	$(LINK.o) -o $@ $^ $(LDLIBS)

ct/_ctcheck.o: ct/_ctcheck.c

ct/_ctcheck.c: $(TOFILES) ct/gen
	ct/gen $(TOFILES) >$@.part
	mv $@.part $@

ct/ct.o ct/_ctcheck.o: ct/ct.h ct/internal.h

$(TOFILES): $(HFILES) ct/ct.h

CLEANFILES+=$(wildcard ct/_* ct/*.o ct/*.gc*)
CLEANFILES+=ct/_ctcheck_mw ct/_ctcheck_mw.c ct/_ctcheck_mw.o

ifneq ($(shell ./verc.sh),$(shell cat vers.c 2>/dev/null))
.PHONY: vers.c
endif
vers.c:
	./verc.sh >vers.c

doc/beanstalkd.1 doc/beanstalkd.1.html: doc/beanstalkd.ronn
	ronn $<
