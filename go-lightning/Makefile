# Copyright (C) The Lightning Authors. All rights reserved.
#
# SPDX-License-Identifier: AGPL-3.0

GOPATH?=$(HOME)/go
.PHONY: $(GOPATH)/bin/lightning
$(GOPATH)/bin/lightning:
	cd lightning && go install -ldflags "-X git.arvados.org/arvados.git/lib/cmd.version=$(shell ./version.sh)"
