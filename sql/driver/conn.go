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

package driver

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/query"
	"github.com/cockroachdb/cockroach/structured"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
)

// TODO(pmattis):
//
// - This file contains the experimental Cockroach sql driver. The driver
//   currently parses SQL and executes key/value operations in order to execute
//   the SQL. The execution will fairly quickly migrate to the server with the
//   driver performing RPCs.
//
// - Flesh out basic insert, update, delete and select operations.
//
// - Figure out transaction story.

// conn implements the sql/driver.Conn interface. Note that conn is assumed to
// be stateful and is not used concurrently by multiple goroutines; See
// https://golang.org/pkg/database/sql/driver/#Conn.
type conn struct {
	query.Planner
}

func (c *conn) Close() error {
	return nil
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	s, err := parser.Parse(query)
	if err != nil {
		return nil, err
	}
	return &stmt{conn: c, stmt: s}, nil
}

func (c *conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	stmt, err := parser.Parse(query)
	if err != nil {
		return nil, err
	}
	return c.exec(stmt, args)
}

func (c *conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	stmt, err := parser.Parse(query)
	if err != nil {
		return nil, err
	}
	return c.query(stmt, args)
}

func (c *conn) Begin() (driver.Tx, error) {
	return &tx{conn: c}, nil
}

func (c *conn) exec(stmt parser.Statement, args []driver.Value) (driver.Result, error) {
	rows, err := c.query(stmt, args)
	if err != nil {
		return nil, err
	}
	return driver.RowsAffected(len(rows.rows)), nil
}

func (c *conn) query(stmt parser.Statement, args []driver.Value) (*rows, error) {
	// TODO(pmattis): Apply the args to the statement.

	switch p := stmt.(type) {
	case *parser.CreateDatabase:
		return c.CreateDatabase(p, args)
	case *parser.CreateTable:
		return c.CreateTable(p, args)
	case *parser.Delete:
		return c.Delete(p, args)
	case *parser.Insert:
		return c.Insert(p, args)
	case *parser.Select:
		return c.Select(p, args)
	case *parser.ShowColumns:
		return c.ShowColumns(p, args)
	case *parser.ShowDatabases:
		return c.ShowDatabases(p, args)
	case *parser.ShowIndex:
		return c.ShowIndex(p, args)
	case *parser.ShowTables:
		return c.ShowTables(p, args)
	case *parser.Update:
		return c.Update(p, args)
	case *parser.Use:
		return c.Use(p, args)

	case *parser.AlterTable:
	case *parser.AlterView:
	case *parser.CreateIndex:
	case *parser.CreateView:
	case *parser.DropDatabase:
	case *parser.DropIndex:
	case *parser.DropTable:
	case *parser.DropView:
	case *parser.RenameTable:
	case *parser.Set:
	case *parser.TruncateTable:
	case *parser.Union:
		// Various unimplemented statements.

	default:
		return nil, fmt.Errorf("unknown statement type: %T", stmt)
	}

	return nil, fmt.Errorf("TODO(pmattis): unimplemented: %T %s", stmt, stmt)
}

func (c *conn) CreateDatabase(p *parser.CreateDatabase, args []driver.Value) (*rows, error) {
	if p.Name == "" {
		return nil, fmt.Errorf("empty database name")
	}

	nameKey := keys.MakeNameMetadataKey(structured.RootNamespaceID, strings.ToLower(p.Name))
	if gr, err := c.DB.Get(nameKey); err != nil {
		return nil, err
	} else if gr.Exists() {
		if p.IfNotExists {
			return &rows{}, nil
		}
		return nil, fmt.Errorf("database \"%s\" already exists", p.Name)
	}
	ir, err := c.DB.Inc(keys.DescIDGenerator, 1)
	if err != nil {
		return nil, err
	}
	nsID := uint32(ir.ValueInt() - 1)
	if err := c.DB.CPut(nameKey, nsID, nil); err != nil {
		// TODO(pmattis): Need to handle if-not-exists here as well.
		return nil, err
	}
	return &rows{}, nil
}

func (c *conn) CreateTable(p *parser.CreateTable, args []driver.Value) (*rows, error) {
	if err := c.normalizeTableName(p.Table); err != nil {
		return nil, err
	}

	dbID, err := c.lookupDatabase(p.Table.Qualifier)
	if err != nil {
		return nil, err
	}

	desc, err := makeTableDesc(p)
	if err != nil {
		return nil, err
	}
	if err := desc.AllocateIDs(); err != nil {
		return nil, err
	}

	nameKey := keys.MakeNameMetadataKey(dbID, p.Table.Name)

	// This isn't strictly necessary as the conditional put below will fail if
	// the key already exists, but it seems good to avoid the table ID allocation
	// in most cases when the table already exists.
	if gr, err := c.DB.Get(nameKey); err != nil {
		return nil, err
	} else if gr.Exists() {
		if p.IfNotExists {
			return &rows{}, nil
		}
		return nil, fmt.Errorf("table \"%s\" already exists", p.Table)
	}

	ir, err := c.DB.Inc(keys.DescIDGenerator, 1)
	if err != nil {
		return nil, err
	}
	desc.ID = uint32(ir.ValueInt() - 1)

	// TODO(pmattis): Be cognizant of error messages when this is ported to the
	// server. The error currently returned below is likely going to be difficult
	// to interpret.
	err = c.DB.Txn(func(txn *client.Txn) error {
		descKey := keys.MakeDescMetadataKey(desc.ID)
		b := &client.Batch{}
		b.CPut(nameKey, descKey, nil)
		b.Put(descKey, &desc)
		return txn.Commit(b)
	})
	if err != nil {
		// TODO(pmattis): Need to handle if-not-exists here as well.
		return nil, err
	}
	return &rows{}, nil
}

func (c *conn) Delete(p *parser.Delete, args []driver.Value) (*rows, error) {
	return nil, fmt.Errorf("TODO(pmattis): unimplemented: %T %s", p, p)
}

func (c *conn) Insert(p *parser.Insert, args []driver.Value) (*rows, error) {
	desc, err := c.getTableDesc(p.Table)
	if err != nil {
		return nil, err
	}

	// Determine which columns we're inserting into.
	cols, err := c.processColumns(desc, p.Columns)
	if err != nil {
		return nil, err
	}

	// Construct a map from column ID to the index the value appears at within a
	// row.
	colMap := map[uint32]int{}
	for i, c := range cols {
		colMap[c.ID] = i
	}

	// Verify we have at least the columns that are part of the primary key.
	for i, id := range desc.Indexes[0].ColumnIDs {
		if _, ok := colMap[id]; !ok {
			return nil, fmt.Errorf("missing \"%s\" primary key column",
				desc.Indexes[0].ColumnNames[i])
		}
	}

	// Transform the values into a rows object. This expands SELECT statements or
	// generates rows from the values contained within the query.
	r, err := c.processInsertRows(p.Rows)
	if err != nil {
		return nil, err
	}

	b := &client.Batch{}
	for _, row := range r.rows {
		if len(row) != len(cols) {
			return nil, fmt.Errorf("invalid values for columns: %d != %d", len(row), len(cols))
		}
		indexKey := encodeIndexKeyPrefix(desc.ID, desc.Indexes[0].ID)
		primaryKey, err := encodeIndexKey(desc.Indexes[0], colMap, cols, row, indexKey)
		if err != nil {
			return nil, err
		}
		for i, val := range row {
			key := encodeColumnKey(desc, cols[i], primaryKey)
			if log.V(2) {
				log.Infof("Put %q -> %v", key, val)
			}
			// TODO(pmattis): Need to convert the value type to the column type.
			b.Put(key, val)
		}
	}
	if err := c.DB.Run(b); err != nil {
		return nil, err
	}

	return &rows{}, nil
}

func (c *conn) Select(p *parser.Select, args []driver.Value) (*rows, error) {
	if len(p.Exprs) != 1 {
		return nil, fmt.Errorf("TODO(pmattis): unsupported select exprs: %s", p.Exprs)
	}
	if _, ok := p.Exprs[0].(*parser.StarExpr); !ok {
		return nil, fmt.Errorf("TODO(pmattis): unsupported select expr: %s", p.Exprs)
	}

	if len(p.From) != 1 {
		return nil, fmt.Errorf("TODO(pmattis): unsupported from: %s", p.From)
	}
	var desc *structured.TableDescriptor
	{
		ate, ok := p.From[0].(*parser.AliasedTableExpr)
		if !ok {
			return nil, fmt.Errorf("TODO(pmattis): unsupported from: %s", p.From)
		}
		table, ok := ate.Expr.(*parser.TableName)
		if !ok {
			return nil, fmt.Errorf("TODO(pmattis): unsupported from: %s", p.From)
		}
		var err error
		desc, err = c.getTableDesc(table)
		if err != nil {
			return nil, err
		}
	}

	// Retrieve all of the keys that start with our index key prefix.
	startKey := proto.Key(encodeIndexKeyPrefix(desc.ID, desc.Indexes[0].ID))
	endKey := startKey.PrefixEnd()
	sr, err := c.DB.Scan(startKey, endKey, 0)
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

	r := &rows{}
	var primaryKey []byte
	vals := map[string]driver.Value{}
	for _, kv := range sr {
		if primaryKey != nil && !bytes.HasPrefix(kv.Key, primaryKey) {
			outputRow(r, desc.Columns, vals)
			vals = map[string]driver.Value{}
		}

		remaining, err := decodeIndexKey(desc, desc.Indexes[0], vals, kv.Key)
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

	outputRow(r, desc.Columns, vals)

	r.columns = make([]string, len(desc.Columns))
	for i, col := range desc.Columns {
		r.columns[i] = col.Name
	}
	return r, nil
}

func (c *conn) ShowColumns(p *parser.ShowColumns, args []driver.Value) (*rows, error) {
	desc, err := c.getTableDesc(p.Table)
	if err != nil {
		return nil, err
	}

	// TODO(pmattis): This output doesn't match up with MySQL. Should it?
	r := &rows{
		columns: []string{"Field", "Type", "Null"},
		rows:    make([]row, len(desc.Columns)),
	}

	for i, col := range desc.Columns {
		t := make(row, len(r.columns))
		t[0] = col.Name
		t[1] = col.Type.SQLString()
		t[2] = col.Nullable
		r.rows[i] = t
	}

	return r, nil
}

func (c *conn) ShowDatabases(p *parser.ShowDatabases, args []driver.Value) (*rows, error) {
	prefix := keys.MakeNameMetadataKey(structured.RootNamespaceID, "")
	sr, err := c.DB.Scan(prefix, prefix.PrefixEnd(), 0)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(sr))
	for i, row := range sr {
		names[i] = string(bytes.TrimPrefix(row.Key, prefix))
	}
	return newSingleColumnRows("database", names), nil
}

func (c *conn) ShowIndex(p *parser.ShowIndex, args []driver.Value) (*rows, error) {
	desc, err := c.getTableDesc(p.Table)
	if err != nil {
		return nil, err
	}

	// TODO(pmattis): This output doesn't match up with MySQL. Should it?
	r := &rows{
		columns: []string{"Table", "Name", "Unique", "Seq", "Column"},
	}
	for _, index := range desc.Indexes {
		for j, col := range index.ColumnNames {
			t := make(row, len(r.columns))
			t[0] = p.Table.Name
			t[1] = index.Name
			t[2] = index.Unique
			t[3] = j + 1
			t[4] = col
			r.rows = append(r.rows, t)
		}
	}

	return r, nil
}

func (c *conn) ShowTables(p *parser.ShowTables, args []driver.Value) (*rows, error) {
	if p.Name == "" {
		if c.Database == "" {
			return nil, fmt.Errorf("no database specified")
		}
		p.Name = c.Database
	}
	dbID, err := c.lookupDatabase(p.Name)
	if err != nil {
		return nil, err
	}
	prefix := keys.MakeNameMetadataKey(dbID, "")
	sr, err := c.DB.Scan(prefix, prefix.PrefixEnd(), 0)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(sr))
	for i, row := range sr {
		names[i] = string(bytes.TrimPrefix(row.Key, prefix))
	}
	return newSingleColumnRows("tables", names), nil
}

func (c *conn) Update(p *parser.Update, args []driver.Value) (*rows, error) {
	return nil, fmt.Errorf("TODO(pmattis): unimplemented: %T %s", p, p)
}

func (c *conn) Use(p *parser.Use, args []driver.Value) (*rows, error) {
	c.Database = p.Name
	return &rows{}, nil
}

func (c *conn) processColumns(desc *structured.TableDescriptor,
	node parser.Columns) ([]structured.ColumnDescriptor, error) {
	if node == nil {
		return desc.Columns, nil
	}

	cols := make([]structured.ColumnDescriptor, len(node))
	for i, n := range node {
		switch nt := n.(type) {
		case *parser.StarExpr:
			return c.processColumns(desc, nil)
		case *parser.NonStarExpr:
			switch et := nt.Expr.(type) {
			case *parser.ColName:
				// TODO(pmattis): If et.Qualifier is not empty, verify it matches the
				// table name.
				var err error
				cols[i], err = desc.FindColumnByName(et.Name)
				if err != nil {
					return nil, err
				}
			default:
				return nil, fmt.Errorf("unexpected node: %T", nt.Expr)
			}
		}
	}

	return cols, nil
}

func (c *conn) processInsertRows(node parser.InsertRows) (*rows, error) {
	switch nt := node.(type) {
	case parser.Values:
		r := &rows{}
		for _, row := range nt {
			switch rt := row.(type) {
			case parser.ValTuple:
				var vals []driver.Value
				for _, val := range rt {
					switch vt := val.(type) {
					case parser.StrVal:
						vals = append(vals, string(vt))
					case parser.NumVal:
						vals = append(vals, string(vt))
					case parser.ValArg:
						return nil, fmt.Errorf("TODO(pmattis): unsupported node: %T", val)
					case parser.BytesVal:
						vals = append(vals, string(vt))
					default:
						return nil, fmt.Errorf("TODO(pmattis): unsupported node: %T", val)
					}
				}
				r.rows = append(r.rows, vals)
			case *parser.Subquery:
				return nil, fmt.Errorf("TODO(pmattis): unsupported node: %T", row)
			}
		}
		return r, nil
	case *parser.Select:
		return c.query(nt, nil)
	case *parser.Union:
		return c.query(nt, nil)
	}
	return nil, fmt.Errorf("TODO(pmattis): unsupported node: %T", node)
}
