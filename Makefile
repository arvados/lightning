GOPATH?=$(HOME)/go
.PHONY: $(GOPATH)/bin/lightning
$(GOPATH)/bin/lightning:
	cd lightning && go install -ldflags "-X git.arvados.org/arvados.git/lib/cmd.version=$(shell ./version.sh)"
