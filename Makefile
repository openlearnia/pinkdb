# Local pinkdb services for development (scripts/dev-services.sh): the
# server with its warehouse console, run in the background from the debug
# build. The server's data directory, pid file and log live under .dev/.
#
#   make console               build and start the server and its console
#   make console UI_AUTH=none  ... without sign-in (a store without users)
#   make console AGENT=1       ... with the data agent behind the ask box
#   make console MEMORY=1      ... with agent memory
#   make stop                  stop every running pinkdb-server
#   make restart               stop, then start again
#   make status                what is running, and where it listens
#   make logs                  follow the server's log
#   make console-app           rebuild the console's browser app (web-dist)
#
# Settings: DATA_DIR (.dev/data: pinkdb.pink, state.pink, lakes/, tmp/),
# PORT (5491), UI_PORT (8091), UI_AUTH (password), AGENT, MEMORY.

DATA_DIR ?= .dev/data
PORT ?= 5491
UI_PORT ?= 8091
UI_AUTH ?= password
AGENT ?=
MEMORY ?=
export DATA_DIR PORT UI_PORT UI_AUTH AGENT MEMORY

SERVICES := scripts/dev-services.sh

.PHONY: help console stop restart status logs console-app

help:
	@sed -n 's/^#   make/make/p' $(firstword $(MAKEFILE_LIST))

console:
	@$(SERVICES) start

stop:
	@$(SERVICES) stop

restart:
	@$(SERVICES) restart

status:
	@$(SERVICES) status

logs:
	@$(SERVICES) logs

# The browser app is wasm: it needs rustup's toolchain with the
# wasm32-unknown-unknown target (a Homebrew cargo has none). Its build
# directory (about 1.6 GB) is removed afterwards.
console-app:
	PATH="$$HOME/.cargo/bin:$$PATH" RUSTUP_TOOLCHAIN=stable \
		RUSTC=$$(rustup which --toolchain stable rustc) scripts/build-console.sh
	rm -rf crates/pinkdb-server-ui-web/target
