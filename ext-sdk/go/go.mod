// Module for the pinkdb Go extension SDK's worked example. Extensions are
// c-shared libraries, not imported packages, so this module exists only so
// `go build -buildmode=c-shared ./examples` works from this directory.
module github.com/openlearnia/pinkdb/ext-sdk/go

go 1.21
