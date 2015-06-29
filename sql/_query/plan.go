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
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlwire"
	"github.com/cockroachdb/cockroach/structured"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
)

// Planner TODO(pmattis): document.
type Planner struct {
	DB       *client.DB
	Database string
}

// GetTableDesc TODO(pmattis): document.
func (p *Planner) GetTableDesc(table *parser.TableName) (*structured.TableDescriptor, error) {
	if err := p.NormalizeTableName(table); err != nil {
		return nil, err
	}
	dbID, err := p.LookupDatabase(table.Qualifier)
	if err != nil {
		return nil, err
	}
	gr, err := p.DB.Get(keys.MakeNameMetadataKey(dbID, table.Name))
	if err != nil {
		return nil, err
	}
	if !gr.Exists() {
		return nil, fmt.Errorf("table \"%s\" does not exist", table)
	}
	descKey := gr.ValueBytes()
	desc := structured.TableDescriptor{}
	if err := p.DB.GetProto(descKey, &desc); err != nil {
		return nil, err
	}
	if err := desc.Validate(); err != nil {
		return nil, err
	}
	return &desc, nil
}

// NormalizeTableName TODO(pmattis): document.
func (p *Planner) NormalizeTableName(table *parser.TableName) error {
	if table.Qualifier == "" {
		if p.Database == "" {
			return fmt.Errorf("no database specified")
		}
		table.Qualifier = p.Database
	}
	if table.Name == "" {
		return fmt.Errorf("empty table name: %s", table)
	}
	return nil
}

// LookupDatabase TODO(pmattis): document.
func (p *Planner) LookupDatabase(name string) (uint32, error) {
	nameKey := keys.MakeNameMetadataKey(structured.RootNamespaceID, name)
	gr, err := p.DB.Get(nameKey)
	if err != nil {
		return 0, err
	} else if !gr.Exists() {
		return 0, fmt.Errorf("database \"%s\" does not exist", name)
	}
	return uint32(gr.ValueInt()), nil
}

// Select TODO(pmattis): document.
func (p *Planner) Select(n *parser.Select, args []sqlwire.Datum) (Plan, error) {
	if len(n.Exprs) != 1 {
		return nil, fmt.Errorf("TODO(pmattis): unsupported select exprs: %s", n.Exprs)
	}
	if _, ok := n.Exprs[0].(*parser.StarExpr); !ok {
		return nil, fmt.Errorf("TODO(pmattis): unsupported select expr: %s", n.Exprs)
	}

	if len(n.From) != 1 {
		return nil, fmt.Errorf("TODO(pmattis): unsupported from: %s", n.From)
	}
	var desc *structured.TableDescriptor
	{
		ate, ok := n.From[0].(*parser.AliasedTableExpr)
		if !ok {
			return nil, fmt.Errorf("TODO(pmattis): unsupported from: %s", n.From)
		}
		table, ok := ate.Expr.(*parser.TableName)
		if !ok {
			return nil, fmt.Errorf("TODO(pmattis): unsupported from: %s", n.From)
		}
		var err error
		desc, err = p.GetTableDesc(table)
		if err != nil {
			return nil, err
		}
	}

	// Retrieve all of the keys that start with our index key prefix.
	startKey := proto.Key(EncodeIndexKeyPrefix(desc.ID, desc.Indexes[0].ID))
	endKey := startKey.PrefixEnd()
	sr, err := p.DB.Scan(startKey, endKey, 0)
	if err != nil {
		return nil, err
	}

	// All of the columns for a particular row will be grouped together. We loop
	// over the returned key/value pairs and decode the key to extract the
	// columns encoded within the key and the column ID. We use the column ID to
	// lookup the column and decode the value. All of these values go into a map
	// keyed by column name. When the index key changes we output a row
	// containing the current values.
	//
	// The TODOs here are too numerous to list. This is only performing a full
	// table scan using the primary key.

	r := &valuesNode{}
	var primaryKey []byte
	vals := map[string]sqlwire.Datum{}
	for _, kv := range sr {
		if primaryKey != nil && !bytes.HasPrefix(kv.Key, primaryKey) {
			r.addRow(desc.Columns, vals)
			vals = map[string]sqlwire.Datum{}
		}

		remaining, err := DecodeIndexKey(desc, desc.Indexes[0], vals, kv.Key)
		if err != nil {
			return nil, err
		}
		primaryKey = []byte(kv.Key[:len(kv.Key)-len(remaining)])

		_, colID := encoding.DecodeUvarint(remaining)
		if err != nil {
			return nil, err
		}
		col, err := desc.FindColumnByID(uint32(colID))
		if err != nil {
			return nil, err
		}
		vals[col.Name] = unmarshalValue(col, kv)

		if log.V(2) {
			log.Infof("Scan %q -> %v", kv.Key, vals[col.Name])
		}
	}

	r.addRow(r, desc.Columns, vals)

	r.columns = make([]string, len(desc.Columns))
	for i, col := range desc.Columns {
		r.columns[i] = col.Name
	}
	return r, nil
}

// Plan TODO(pmattis): document.
type Plan interface {
	Columns() []string
	Values() []sqlwire.Datum
	Next() error
	plan()
}

func (*scanNode) plan()   {}
func (*valuesNode) plan() {}

// planNode TODO(pmattis): document.
type planNode interface {
	Plan
	Seek(key []byte) error
	planNode()
}

func (*scanNode) planNode()   {}
func (*valuesNode) planNode() {}

// scanNode TODO(pmattis): document.
type scanNode struct {
	Desc   *structured.TableDescriptor
	Index  *structured.IndexDescriptor
	Filter func([]sqlwire.Datum) bool
}

type valuesNode struct {
	columns []string
	rows    [][]sqlwire.Datum
	pos     int
}

func (n *valuesNode) addRow(cols []structured.ColumnDescriptor, vals map[string]sqlwire.Datum) {
	row := make([]sqlwire.Datum, len(cols))
	for i, col := range cols {
		row[i] = vals[col.Name]
	}
	n.rows = append(n.rows, row)
}
