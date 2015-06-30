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

package sqlwire

import "strconv"

// DisplayString TODO(pmattis): Rename to String().
func (d Datum) DisplayString() string {
	if d.IntVal != nil {
		return strconv.FormatInt(*d.IntVal, 10)
	}
	if d.UintVal != nil {
		return strconv.FormatUint(*d.UintVal, 10)
	}
	if d.FloatVal != nil {
		return strconv.FormatFloat(*d.FloatVal, 'g', -1, 64)
	}
	if d.BytesVal != nil {
		return string(d.BytesVal)
	}
	if d.StringVal != nil {
		return *d.StringVal
	}
	return "NULL"
}

// ToInt converts the datum type to int.
func (d Datum) ToInt() Datum {
	if d.IntVal != nil {
		return d
	}
	if d.UintVal != nil {
		v := int64(*d.UintVal)
		return Datum{IntVal: &v}
	}
	if d.FloatVal != nil {
		v := int64(*d.FloatVal)
		return Datum{IntVal: &v}
	}
	if d.BytesVal != nil {
		v, _ := strconv.ParseInt(string(d.BytesVal), 10, 64)
		return Datum{IntVal: &v}
	}
	if d.StringVal != nil {
		v, _ := strconv.ParseInt(*d.StringVal, 10, 64)
		return Datum{IntVal: &v}
	}
	return d
}

// ToUint converts the datum type to int.
func (d Datum) ToUint() Datum {
	if d.UintVal != nil {
		return d
	}
	if d.IntVal != nil {
		v := uint64(*d.IntVal)
		return Datum{UintVal: &v}
	}
	if d.FloatVal != nil {
		v := uint64(*d.FloatVal)
		return Datum{UintVal: &v}
	}
	if d.BytesVal != nil {
		v, _ := strconv.ParseUint(string(d.BytesVal), 10, 64)
		return Datum{UintVal: &v}
	}
	if d.StringVal != nil {
		v, _ := strconv.ParseUint(*d.StringVal, 10, 64)
		return Datum{UintVal: &v}
	}
	return d
}

// ToFloat converts the datum type to float.
func (d Datum) ToFloat() Datum {
	if d.FloatVal != nil {
		return d
	}
	if d.IntVal != nil {
		v := float64(*d.IntVal)
		return Datum{FloatVal: &v}
	}
	if d.UintVal != nil {
		v := float64(*d.UintVal)
		return Datum{FloatVal: &v}
	}
	if d.BytesVal != nil {
		v, _ := strconv.ParseFloat(string(d.BytesVal), 64)
		return Datum{FloatVal: &v}
	}
	if d.StringVal != nil {
		v, _ := strconv.ParseFloat(*d.StringVal, 64)
		return Datum{FloatVal: &v}
	}
	return d
}
