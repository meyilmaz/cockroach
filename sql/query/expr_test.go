// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package query

import (
	"testing"

	"github.com/cockroachdb/cockroach/sql/parser"
)

func TestEvalExpr(t *testing.T) {
	testData := []struct {
		expr     string
		expected string
	}{
		{`1+2`, `3`},
		{`1+2+(3*4)`, `15`},
		{`1.1+1`, `2.1`},
		{`~0`, `18446744073709551615`},
		{`~0 - 1`, `18446744073709551614`},
		{`~0 + 1`, `0`},
		{`3/2`, `1.5`},
		{`1&3`, `1`},
		{`1+"2"`, `3`},
		{`2.1-"1.1"`, `1`},
		{`"18446744073709551614" + 1`, `1.8446744073709552e+19`},
		{`"1.1"+"2.1"`, `3.2`},
	}
	for i, d := range testData {
		// TODO(pmattis): The "FROM t" should not be necessary. Fix the parser.
		q, err := parser.Parse("SELECT " + d.expr + " FROM t")
		if err != nil {
			t.Fatalf("%d: %v: %s", i, err, d.expr)
		}
		expr := q.(*parser.Select).Exprs[0].(*parser.NonStarExpr).Expr
		r, err := EvalExpr(expr, nil)
		if err != nil {
			t.Fatalf("%d: %v", i, err)
		}
		s := r.DisplayString()
		if d.expected != s {
			t.Fatalf("%d: expected %s, but found %s", i, d.expected, s)
		}
	}
}
