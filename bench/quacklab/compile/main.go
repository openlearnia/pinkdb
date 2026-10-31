// Compiles a QuackLab model into the physical artifacts and execution plan
// QuackLab's runtime executes. It imports QuackLab's internal packages, so it
// is run through `go run -overlay` from inside QuackLab's Go module, which
// leaves that checkout untouched (see compile.sh).
package main

import (
	"encoding/json"
	"os"

	"github.com/openlearnia/quacklab/go/internal/compiler/semantics"
	"github.com/openlearnia/quacklab/go/internal/model"
	"github.com/openlearnia/quacklab/go/internal/physical"
)

func main() {
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
	plan, err := physical.BuildExecutionPlan("bench", b.TableSpecs, b.ConsolidatedHydrateSQL)
	if err != nil {
		panic(err)
	}
	cat, err := semantics.EmitCatalog(m, "bench")
	if err != nil {
		panic(err)
	}
	// Chart queries: QuackLab's semantic-query compiler turns each widget
	// query in the second argument into the SQL a dashboard sends.
	charts := map[string]string{}
	if len(os.Args) > 2 {
		raw, err := os.ReadFile(os.Args[2])
		if err != nil {
			panic(err)
		}
		var qs map[string]semantics.SemanticQuery
		if err := json.Unmarshal(raw, &qs); err != nil {
			panic(err)
		}
		for name, q := range qs {
			sql, err := semantics.CompileQuery(cat, q)
			if err != nil {
				panic(name + ": " + err.Error())
			}
			charts[name] = sql
		}
	}
	out := map[string]any{
		"semanticModels":         cat.Semantics,
		"chartSql":               charts,
		"tableSpecs":             b.TableSpecs,
		"consolidatedHydrateSql": b.ConsolidatedHydrateSQL,
		"runtimeSqlCatalog":      physical.BuildRuntimeSqlCatalog(b.TableSpecs),
		"uploadContract":         physical.BuildUploadContract(b.TableSpecs),
		"refreshMartSetSql":      physical.BuildRefreshMartSetSQLPack(b.TableSpecs),
		"executionPlan":          plan,
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", " ")
	if err := enc.Encode(out); err != nil {
		panic(err)
	}
}
