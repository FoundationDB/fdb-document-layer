ROOTDIR := $(PWD)
LIBDIR  := $(ROOTDIR)/lib
TMPDIR  := $(LIBDIR)/pcre


INCDIR = -I.
LIBDIR = -L.

CPP_FILES := $(wildcard *.c)
OBJ_FILES := $(addprefix obj/,$(notdir $(CPP_FILES:.cpp=.o)))
LD_FLAGS := ...
CC_FLAGS := ...


.PHONY: clean all

all:
	@echo "Processing       PCRE library"
	@echo $(PWD)
	@mkdir -p $(TMPDIR)

clean:
	@echo "Clean up         PCRE library"
	rm -rf $(TMPDIR)
