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
// Author: Vivek Menezes (vivek@cockroachlabs.com)

package sqlwire

import (
	"github.com/cockroachdb/cockroach/proto"
	gogoproto "github.com/gogo/protobuf/proto"
)

const (
	// Endpoint is the URL path prefix which accepts incoming
	// HTTP requests for the SQL API.
	Endpoint = "/sql/"
)

// Request is an interface for RPC requests.
type Request interface {
	gogoproto.Message
	Header() *RequestHeader
	// Method returns the request method.
	Method() Method
	// CreateReply creates a new response object.
	CreateReply() Response
}

// Response is an interface for RPC responses.
type Response interface {
	gogoproto.Message
	// Header returns the response header.
	Header() *ResponseHeader
}

// A Call is a pending database API call.
type Call struct {
	Args  Request  // The argument to the command
	Reply Response // The reply from the command
}

// Header returns the request header.
func (r *RequestHeader) Header() *RequestHeader {
	return r
}

// Method returns the method.
func (*Request) Method() Method {
	return Execute
}

// CreateReply creates an empty response for the request.
func (*Request) CreateReply() Response {
	return &Response{}
}

// Header returns the response header.
func (r *ResponseHeader) Header() *ResponseHeader {
	return r
}

// SetGoError converts the specified type into either one of the proto-
// defined error types or into a Error for all other Go errors.
func (r *ResponseHeader) SetGoError(err error) {
	if err == nil {
		r.Error = nil
		return
	}
	r.Error = &proto.Error{}
	r.Error.SetResponseGoError(err)
}
