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
	"math"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/sql/sqlwire"
	"github.com/cockroachdb/cockroach/structured"
	"github.com/cockroachdb/cockroach/util/encoding"
)

// EncodeIndexKeyPrefix TODO(pmattis): document.
func EncodeIndexKeyPrefix(tableID, indexID uint32) []byte {
	var key []byte
	key = append(key, keys.TableDataPrefix...)
	key = encoding.EncodeUvarint(key, uint64(tableID))
	key = encoding.EncodeUvarint(key, uint64(indexID))
	return key
}

// EncodeIndexKey TODO(pmattis): document.
func EncodeIndexKey(index structured.IndexDescriptor,
	colMap map[uint32]int, cols []structured.ColumnDescriptor,
	row []sqlwire.Datum, indexKey []byte) ([]byte, error) {
	var key []byte
	key = append(key, indexKey...)

	for i, id := range index.ColumnIDs {
		j, ok := colMap[id]
		if !ok {
			return nil, fmt.Errorf("missing \"%s\" primary key column",
				index.ColumnNames[i])
		}
		// TOOD(pmattis): Need to convert the row[i] value to the type expected by
		// the column.
		var err error
		key, err = EncodeTableKey(key, cols[j], row[j])
		if err != nil {
			return nil, err
		}
	}
	return key, nil
}

// EncodeColumnKey TODO(pmattis): document.
func EncodeColumnKey(desc *structured.TableDescriptor,
	col structured.ColumnDescriptor, primaryKey []byte) []byte {
	var key []byte
	key = append(key, primaryKey...)
	return encoding.EncodeUvarint(key, uint64(col.ID))
}

// EncodeTableKey TODO(pmattis): document.
func EncodeTableKey(b []byte, col structured.ColumnDescriptor, v sqlwire.Datum) ([]byte, error) {
	switch col.Type.Kind {
	case structured.ColumnType_BIT, structured.ColumnType_INT:
		switch v.Kind {
		case sqlwire.Datum_NULL:
			return encoding.EncodeVarint(b, t), nil

		case sqlwire.Datum_INT64:
			return encoding.EncodeVarint(b, v.GetInt64()), nil

		case sqlwire.Datum_FLOAT64:
			return encoding.EncodeVarint(b, int64(v.GetFloat64())), nil

		case sqlwire.Datum_BYTES:
		}

	case structured.ColumnType_FLOAT:
		switch v.Kind {
		case sqlwire.Datum_NULL:
		case sqlwire.Datum_INT64:
		case sqlwire.Datum_FLOAT64:
		case sqlwire.Datum_BYTES:
		}

	case structured.ColumnType_CHAR, structured.ColumnType_BINARY,
		structured.ColumnType_TEXT, structured.ColumnType_BLOB:
		switch v.Kind {
		case sqlwire.Datum_NULL:
		case sqlwire.Datum_INT64:
		case sqlwire.Datum_FLOAT64:
		case sqlwire.Datum_BYTES:
		}

	default:
		return nil, fmt.Errorf("unable to encode table key: %s", col.Type.Kind)
	}

	switch t := v.(type) {
	case int64:
		return encoding.EncodeVarint(b, t), nil
	case float64:
		return encoding.EncodeNumericFloat(b, t), nil
	case bool:
		if t {
			return encoding.EncodeVarint(b, 1), nil
		}
		return encoding.EncodeVarint(b, 0), nil
	case []byte:
		return encoding.EncodeBytes(b, t), nil
	case string:
		return encoding.EncodeBytes(b, []byte(t)), nil
	case time.Time:
		return nil, fmt.Errorf("TODO(pmattis): encode index key: time.Time")
	}
	return nil, fmt.Errorf("unable to encode table key: %T", v)
}

// DecodeIndexKey TODO(pmattis): document.
func DecodeIndexKey(desc *structured.TableDescriptor,
	index structured.IndexDescriptor, vals map[string]sqlwire.Datum, key []byte) ([]byte, error) {
	if !bytes.HasPrefix(key, keys.TableDataPrefix) {
		return nil, fmt.Errorf("%s: invalid key prefix: %q", desc.Name, key)
	}
	key = bytes.TrimPrefix(key, keys.TableDataPrefix)

	var tableID uint64
	key, tableID = encoding.DecodeUvarint(key)
	if uint32(tableID) != desc.ID {
		return nil, fmt.Errorf("%s: unexpected table ID: %d != %d", desc.Name, desc.ID, tableID)
	}

	var indexID uint64
	key, indexID = encoding.DecodeUvarint(key)
	if uint32(indexID) != index.ID {
		return nil, fmt.Errorf("%s: unexpected index ID: %d != %d", desc.Name, index.ID, indexID)
	}

	for _, id := range index.ColumnIDs {
		col, err := desc.FindColumnByID(id)
		if err != nil {
			return nil, err
		}
		switch col.Type.Kind {
		case structured.ColumnType_BIT, structured.ColumnType_INT:
			var i int64
			key, i = encoding.DecodeVarint(key)
			vals[col.Name] = sqlwire.Datum{
				Kind:  sqlwire.Datum_INT64,
				Value: encoding.EncodeUint64(nil, uint64(i)),
			}
		case structured.ColumnType_FLOAT:
			var f float64
			key, f = encoding.DecodeNumericFloat(key)
			vals[col.Name] = sqlwire.Datum{
				Kind:  sqlwire.Datum_FLOAT64,
				Value: encoding.EncodeUint64(nil, math.Float64bits(f)),
			}
		case structured.ColumnType_CHAR, structured.ColumnType_BINARY,
			structured.ColumnType_TEXT, structured.ColumnType_BLOB:
			var r []byte
			key, r = encoding.DecodeBytes(key, nil)
			vals[col.Name] = sqlwire.Datum{
				Kind:  sqlwire.Datum_BYTES,
				Value: r,
			}
		default:
			return nil, fmt.Errorf("TODO(pmattis): decoded index key: %s", col.Type.Kind)
		}
	}

	return key, nil
}

func unmarshalValue(col structured.ColumnDescriptor, kv client.KeyValue) sqlwire.Datum {
	d := sqlwire.Datum{Value: kv.ValueBytes()}
	if !kv.Exists() {
		d.Kind = sqlwire.Datum_NULL
	} else {
		switch col.Type.Kind {
		case structured.ColumnType_BIT, structured.ColumnType_INT:
			d.Kind = sqlwire.Datum_INT64
		case structured.ColumnType_FLOAT:
			d.Kind = sqlwire.Datum_FLOAT64
		case structured.ColumnType_CHAR, structured.ColumnType_BINARY,
			structured.ColumnType_TEXT, structured.ColumnType_BLOB:
			d.Kind = sqlwire.Datum_BYTES
		}
	}
	return d
}
