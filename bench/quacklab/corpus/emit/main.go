// Compiles a fixture model plus a query-spec file into the concrete SQL a
// QuackLab dashboard sends: every spec is driven through QuackLab's own
// query path (semanticompile.ApplyDashboardFilters, CompileQuery,
// CompileMemberValues), never written by hand. Like ../compile/main.go it imports QuackLab's internal packages, so
// it is run through `go run -overlay` from inside QuackLab's Go module and that
// checkout is never written (see ../generate.sh).
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/openlearnia/quacklab/go/internal/compiler/semantics"
	"github.com/openlearnia/quacklab/go/internal/model"
	"github.com/openlearnia/quacklab/go/internal/physical"
	"github.com/openlearnia/quacklab/go/pkg/semanticompile"
)

// spec is one corpus case: a SemanticQuery, or a distinct-values lookup.
type spec struct {
	EntryPoint   string                   `json:"entryPoint"`
	Query        *semantics.SemanticQuery `json:"query,omitempty"`
	MemberValues *memberValuesSpec        `json:"memberValues,omitempty"`
	// DashboardFilters are the dashboard-level filters a batch execute merges
	// into the widget query before compiling (see semanticompile).
	DashboardFilters []semanticompile.DashboardFilter `json:"dashboardFilters,omitempty"`
}

// memberValuesSpec is one CompileMemberValues call (dashboard filter picker).
type memberValuesSpec struct {
	SemanticID string `json:"semanticId"`
	Dimension  string `json:"dimension"`
	Search     string `json:"search,omitempty"`
	Limit      int    `json:"limit,omitempty"`
}

// corpusCase is the emitted SQL for one spec, with its input kept for the report.
type corpusCase struct {
	SQL        string          `json:"sql"`
	EntryPoint string          `json:"entryPoint"`
	Spec       json.RawMessage `json:"spec"`
}

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: emit <model.json> <queries.json>")
		os.Exit(2)
	}
	raw, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	var m model.Model
	if err := json.Unmarshal(raw, &m); err != nil {
		panic(err)
	}
	b, err := physical.BuildTableSpecs(m)
	if err != nil {
		panic(err)
	}
	cat, err := semantics.EmitCatalog(m, "bench")
	if err != nil {
		panic(err)
	}
	rawSpecs, err := os.ReadFile(os.Args[2])
	if err != nil {
		panic(err)
	}
	var specs map[string]json.RawMessage
	if err := json.Unmarshal(rawSpecs, &specs); err != nil {
		panic(err)
	}
	cases := map[string]corpusCase{}
	for name, rawSpec := range specs {
		var s spec
		if err := json.Unmarshal(rawSpec, &s); err != nil {
			panic(name + ": " + err.Error())
		}
		var sql string
		switch s.EntryPoint {
		case "query":
			if s.Query == nil {
				panic(name + ": query entryPoint needs a query")
			}
			q := semanticompile.ApplyDashboardFilters("w0", *s.Query, s.DashboardFilters)
			sql, err = semantics.CompileQuery(cat, q)
		case "memberValues":
			if s.MemberValues == nil {
				panic(name + ": memberValues entryPoint needs memberValues")
			}
			mv := s.MemberValues
			sql, err = semantics.CompileMemberValues(cat, mv.SemanticID, mv.Dimension, mv.Search, mv.Limit)
		default:
			panic(name + ": unknown entryPoint " + s.EntryPoint)
		}
		if err != nil {
			panic(name + ": " + err.Error())
		}
		cases[name] = corpusCase{SQL: sql, EntryPoint: s.EntryPoint, Spec: rawSpec}
	}
	out := map[string]any{
		"model":          m.Name,
		"semanticModels": cat.Semantics,
		"tableSpecs":     b.TableSpecs,
		"cases":          cases,
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", " ")
	if err := enc.Encode(out); err != nil {
		panic(err)
	}
}
