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
	"fmt"
	"math"
	"strconv"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlwire"
)

type opType int

const (
	intOp opType = iota
	uintOp
	floatOp
)

var null = sqlwire.Datum{}

// Env TODO(pmattis): document.
type Env interface {
	Get(name string) (sqlwire.Datum, bool)
}

type mapEnv map[string]sqlwire.Datum

func (e mapEnv) Get(name string) (sqlwire.Datum, bool) {
	d, ok := e[name]
	return d, ok
}

// EvalExpr evaluates an SQL expression in the context of an environment.
func EvalExpr(expr parser.Expr, env Env) (sqlwire.Datum, error) {
	switch t := expr.(type) {
	case *parser.AndExpr:

	case *parser.OrExpr:

	case *parser.NotExpr:

	case *parser.ParenBoolExpr:

	case *parser.ComparisonExpr:
		return evalComparisonExpr(t, env)

	case *parser.RangeCond:

	case *parser.NullCheck:

	case *parser.ExistsExpr:

	case parser.BytesVal:
		v := string(t)
		return sqlwire.Datum{StringVal: &v}, nil

	case parser.StrVal:
		v := string(t)
		return sqlwire.Datum{StringVal: &v}, nil

	case parser.IntVal:
		v, err := strconv.ParseUint(string(t), 0, 64)
		if err != nil {
			return null, err
		}
		return sqlwire.Datum{UintVal: &v}, nil

	case parser.NumVal:
		v, err := strconv.ParseFloat(string(t), 64)
		if err != nil {
			return null, err
		}
		return sqlwire.Datum{FloatVal: &v}, nil

	case parser.ValArg:

	case *parser.NullVal:
		return null, nil

	case *parser.ColName:
		if d, ok := env.Get(t.String()); ok {
			return d, nil
		}
		return null, fmt.Errorf("column \"%s\" not found", t)

	case parser.ValTuple:
		if len(t) != 1 {
			return null, fmt.Errorf("unsupported expression type: %T: %s", expr, expr)
		}
		return EvalExpr(t[0], env)

	case *parser.Subquery:

	case *parser.BinaryExpr:
		return evalBinaryExpr(t, env)

	case *parser.UnaryExpr:
		return evalUnaryExpr(t, env)

	case *parser.FuncExpr:

	case *parser.CaseExpr:
	}
	return null, fmt.Errorf("unsupported expression type: %T", expr)
}

func evalComparisonExpr(expr *parser.ComparisonExpr, env Env) (sqlwire.Datum, error) {
	// If one or both arguments are NULL, the result of the comparison is NULL,
	// except for the NULL-safe <=> equality comparison operator. For NULL <=>
	// NULL, the result is true. No conversion is needed.
	//
	// If both arguments in a comparison operation are strings, they are compared
	// as strings.
	//
	// If both arguments are integers, they are compared as integers.
	//
	// Hexadecimal values are treated as binary strings if not compared to a number.
	//
	// If one of the arguments is a TIMESTAMP or DATETIME column and the other
	// argument is a constant, the constant is converted to a timestamp before the
	// comparison is performed. This is done to be more ODBC-friendly. Note that
	// this is not done for the arguments to IN()! To be safe, always use complete
	// datetime, date, or time strings when doing comparisons. For example, to
	// achieve best results when using BETWEEN with date or time values, use CAST()
	// to explicitly convert the values to the desired data type.
	//
	// If one of the arguments is a decimal value, comparison depends on the other
	// argument. The arguments are compared as decimal values if the other argument
	// is a decimal or integer value, or as floating-point values if the other
	// argument is a floating-point value.
	//
	// In all other cases, the arguments are compared as floating-point (real)
	// numbers.

	// left, err := EvalExpr(expr.Left, env)
	// if err != nil {
	// 	return null, err
	// }
	// right, err := EvalExpr(expr.Right, env)
	// if err != nil {
	// 	return null, err
	// }

	switch expr.Operator {
	case "=":
	case "<":
	case ">":
	case "<=":
	case ">=":
	case "!=":
	case "<=>":
	case "IN":
	case "NOT":
	case "NOT IN":
	case "LIKE":
	case "NOT LIKE":
	}
	return null, fmt.Errorf("unsupported comparison operator: %s", expr.Operator)
}

func evalBinaryExpr(expr *parser.BinaryExpr, env Env) (sqlwire.Datum, error) {
	// In the case of -, +, and *, the result is calculated with BIGINT (64-bit)
	// precision if both operands are integers.
	//
	// If both operands are integers and any of them are unsigned, the result is an
	// unsigned integer.
	//
	// If any of the operands of a +, -, /, *, % is a real or string value, the
	// precision of the result is the precision of the operand with the maximum
	// precision.
	//
	// In division performed with /, the scale of the result when using two
	// exact-value operands is the scale of the first operand plus the value of the
	// div_precision_increment system variable (which is 4 by default). For
	// example, the result of the expression 5.05 / 0.014 has a scale of six
	// decimal places (360.714286).

	left, err := EvalExpr(expr.Left, env)
	if err != nil {
		return null, err
	}
	right, err := EvalExpr(expr.Right, env)
	if err != nil {
		return null, err
	}

	switch expr.Operator {
	case '&':
		switch prepareBinaryArgs(intOp, &left, &right) {
		case uintOp:
			*left.UintVal &= *right.UintVal
		case intOp:
			*left.IntVal &= *right.IntVal
		}
		return left, nil

	case '|':
		switch prepareBinaryArgs(intOp, &left, &right) {
		case uintOp:
			*left.UintVal |= *right.UintVal
		case intOp:
			*left.IntVal |= *right.IntVal
		}
		return left, nil

	case '^':
		switch prepareBinaryArgs(intOp, &left, &right) {
		case uintOp:
			*left.UintVal ^= *right.UintVal
		case intOp:
			*left.IntVal ^= *right.IntVal
		}
		return left, nil

	case '+':
		switch prepareBinaryArgs(floatOp, &left, &right) {
		case uintOp:
			*left.UintVal += *right.UintVal
		case intOp:
			*left.IntVal += *right.IntVal
		case floatOp:
			*left.FloatVal += *right.FloatVal
		}
		return left, nil

	case '-':
		switch prepareBinaryArgs(floatOp, &left, &right) {
		case uintOp:
			*left.UintVal -= *right.UintVal
		case intOp:
			*left.IntVal -= *right.IntVal
		case floatOp:
			*left.FloatVal -= *right.FloatVal
		}
		return left, nil

	case '*':
		switch prepareBinaryArgs(floatOp, &left, &right) {
		case uintOp:
			*left.UintVal *= *right.UintVal
		case intOp:
			*left.IntVal *= *right.IntVal
		case floatOp:
			*left.FloatVal *= *right.FloatVal
		}
		return left, nil

	case '/':
		left = left.ToFloat()
		right = right.ToFloat()
		*left.FloatVal /= *right.FloatVal
		return left, nil

	case '%':
		switch prepareBinaryArgs(floatOp, &left, &right) {
		case uintOp:
			*left.UintVal %= *right.UintVal
		case intOp:
			*left.IntVal %= *right.IntVal
		case floatOp:
			*left.FloatVal = math.Mod(*left.FloatVal, *right.FloatVal)
		}
		return left, nil
	}

	return null, fmt.Errorf("unsupported binary operator: %c", expr.Operator)
}

func evalUnaryExpr(expr *parser.UnaryExpr, env Env) (sqlwire.Datum, error) {
	d, err := EvalExpr(expr.Expr, env)
	if err != nil {
		return null, err
	}
	switch expr.Operator {
	case '+':
		return d, nil
	case '-':
		if d.IntVal != nil {
			*d.IntVal = -*d.IntVal
		} else if d.FloatVal != nil {
			*d.FloatVal = -*d.FloatVal
		} else {
			d = d.ToFloat()
			*d.FloatVal = -*d.FloatVal
		}
		return d, nil
	case '~':
		d = d.ToUint()
		*d.UintVal = ^*d.UintVal
		return d, nil
	}
	return null, fmt.Errorf("unsupported unary operator: %c", expr.Operator)
}

func prepareBinaryArgs(op opType, left, right *sqlwire.Datum) opType {
	switch op {
	case intOp, uintOp:
		if left.UintVal != nil || right.UintVal != nil {
			*left = left.ToUint()
			*right = right.ToUint()
			return uintOp
		}
		*left = left.ToInt()
		*right = right.ToInt()
		return intOp

	case floatOp:
		if (left.UintVal != nil && (right.IntVal != nil || right.UintVal != nil)) ||
			(right.UintVal != nil && (left.IntVal != nil || left.UintVal != nil)) {
			*left = left.ToUint()
			*right = right.ToUint()
			return uintOp
		}
		if left.IntVal != nil && right.IntVal != nil {
			return intOp
		}
		*left = left.ToFloat()
		*right = right.ToFloat()
		return floatOp
	}

	return floatOp
}
