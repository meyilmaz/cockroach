// Copyright 2014 The Cockroach Authors.
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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	yaml "gopkg.in/yaml.v1"
)

const testZoneConfig = `
replicas:
  - attrs: [dc1, ssd]
  - attrs: [dc2, ssd]
  - attrs: [dc3, ssd]
range_min_bytes: 1048576
range_max_bytes: 67108864
`

var testZoneConfigBytes = []byte(testZoneConfig)

// Example_setAndGetZone sets zone configs for a variety of key
// prefixes and verifies they can be fetched directly.
func Example_setAndGetZone() {
	_, stopper := startAdminServer()
	defer stopper.Stop()

	testData := []struct {
		prefix proto.Key
		yaml   string
	}{
		{proto.KeyMin, testZoneConfig},
		{proto.Key("db1"), testZoneConfig},
		{proto.Key("db 2"), testZoneConfig},
		{proto.Key("\xfe"), testZoneConfig},
	}

	for _, test := range testData {
		prefix := url.QueryEscape(string(test.prefix))
		RunSetZone(testContext, prefix, testZoneConfigBytes)
		RunGetZone(testContext, prefix)
	}
	// Output:
	// set zone config for key prefix ""
	// zone config for key prefix "":
	// replicas:
	// - attrs: [dc1, ssd]
	// - attrs: [dc2, ssd]
	// - attrs: [dc3, ssd]
	// range_min_bytes: 1048576
	// range_max_bytes: 67108864
	//
	// set zone config for key prefix "db1"
	// zone config for key prefix "db1":
	// replicas:
	// - attrs: [dc1, ssd]
	// - attrs: [dc2, ssd]
	// - attrs: [dc3, ssd]
	// range_min_bytes: 1048576
	// range_max_bytes: 67108864
	//
	// set zone config for key prefix "db+2"
	// zone config for key prefix "db+2":
	// replicas:
	// - attrs: [dc1, ssd]
	// - attrs: [dc2, ssd]
	// - attrs: [dc3, ssd]
	// range_min_bytes: 1048576
	// range_max_bytes: 67108864
	//
	// set zone config for key prefix "%FE"
	// zone config for key prefix "%FE":
	// replicas:
	// - attrs: [dc1, ssd]
	// - attrs: [dc2, ssd]
	// - attrs: [dc3, ssd]
	// range_min_bytes: 1048576
	// range_max_bytes: 67108864
}

// Example_lsZones creates a series of zone configs and verifies
// zone-ls works. First, no regexp lists all zone configs. Second,
// regexp properly matches results.
func Example_lsZones() {
	_, stopper := startAdminServer()
	defer stopper.Stop()

	keys := []proto.Key{
		proto.KeyMin,
		proto.Key("db1"),
		proto.Key("db2"),
		proto.Key("db3"),
		proto.Key("user"),
	}

	regexps := []string{
		"",
		"db*",
		"db[12]",
	}

	for _, key := range keys {
		prefix := url.QueryEscape(string(key))
		RunSetZone(testContext, prefix, testZoneConfigBytes)
	}

	for i, regexp := range regexps {
		fmt.Fprintf(os.Stdout, "test case %d: %q\n", i, regexp)
		if regexp == "" {
			RunLsZone(testContext, "")
		} else {
			RunLsZone(testContext, regexp)
		}
	}
	// Output:
	// set zone config for key prefix ""
	// set zone config for key prefix "db1"
	// set zone config for key prefix "db2"
	// set zone config for key prefix "db3"
	// set zone config for key prefix "user"
	// test case 0: ""
	// [default]
	// db1
	// db2
	// db3
	// user
	// test case 1: "db*"
	// db1
	// db2
	// db3
	// test case 2: "db[12]"
	// db1
	// db2
}

// Example_rmZones creates a series of zone configs and verifies
// zone-rm works by deleting some and then all and verifying entries
// have been removed via zone-ls. Also verify the default zone cannot
// be removed.
func Example_rmZones() {
	_, stopper := startAdminServer()
	defer stopper.Stop()

	keys := []proto.Key{
		proto.KeyMin,
		proto.Key("db1"),
	}

	for _, key := range keys {
		prefix := url.QueryEscape(string(key))
		RunSetZone(testContext, prefix, testZoneConfigBytes)
	}

	for _, key := range keys {
		prefix := url.QueryEscape(string(key))
		RunRmZone(testContext, prefix)
		RunLsZone(testContext, "")
	}
	// Output:
	// set zone config for key prefix ""
	// set zone config for key prefix "db1"
	// [default]
	// db1
	// removed zone config for key prefix "db1"
	// [default]
}

// Example_zoneContentTypes verifies that the Accept header can be used
// to control the format of the response and the Content-Type header
// can be used to specify the format of the request.
func Example_zoneContentTypes() {
	_, stopper := startAdminServer()
	defer stopper.Stop()

	config := &proto.ZoneConfig{}
	err := yaml.Unmarshal(testZoneConfigBytes, config)
	if err != nil {
		fmt.Println(err)
	}
	testCases := []struct {
		contentType, accept string
	}{
		{util.JSONContentType, util.JSONContentType},
		{util.YAMLContentType, util.JSONContentType},
		{util.JSONContentType, util.YAMLContentType},
		{util.YAMLContentType, util.YAMLContentType},
	}
	for i, test := range testCases {
		key := fmt.Sprintf("/test%d", i)

		var body []byte
		if test.contentType == util.JSONContentType {
			if body, err = json.MarshalIndent(config, "", "  "); err != nil {
				fmt.Println(err)
			}
		} else {
			if body, err = yaml.Marshal(config); err != nil {
				fmt.Println(err)
			}
		}
		req, err := http.NewRequest("POST", fmt.Sprintf("%s://%s%s%s", testContext.RequestScheme(), testContext.Addr,
			zonePathPrefix, key), bytes.NewReader(body))
		req.Header.Add(util.ContentTypeHeader, test.contentType)
		if _, err = sendAdminRequest(testContext, req); err != nil {
			fmt.Println(err)
		}

		req, err = http.NewRequest("GET", fmt.Sprintf("%s://%s%s%s", testContext.RequestScheme(), testContext.Addr,
			zonePathPrefix, key), nil)
		req.Header.Add(util.AcceptHeader, test.accept)
		if body, err = sendAdminRequest(testContext, req); err != nil {
			fmt.Println(err)
		}
		fmt.Println(string(body))
	}
	// Output:
	// {
	//   "replica_attrs": [
	//     {
	//       "attrs": [
	//         "dc1",
	//         "ssd"
	//       ]
	//     },
	//     {
	//       "attrs": [
	//         "dc2",
	//         "ssd"
	//       ]
	//     },
	//     {
	//       "attrs": [
	//         "dc3",
	//         "ssd"
	//       ]
	//     }
	//   ],
	//   "range_min_bytes": 1048576,
	//   "range_max_bytes": 67108864
	// }
	// {
	//   "replica_attrs": [
	//     {
	//       "attrs": [
	//         "dc1",
	//         "ssd"
	//       ]
	//     },
	//     {
	//       "attrs": [
	//         "dc2",
	//         "ssd"
	//       ]
	//     },
	//     {
	//       "attrs": [
	//         "dc3",
	//         "ssd"
	//       ]
	//     }
	//   ],
	//   "range_min_bytes": 1048576,
	//   "range_max_bytes": 67108864
	// }
	// replicas:
	// - attrs: [dc1, ssd]
	// - attrs: [dc2, ssd]
	// - attrs: [dc3, ssd]
	// range_min_bytes: 1048576
	// range_max_bytes: 67108864
	//
	// replicas:
	// - attrs: [dc1, ssd]
	// - attrs: [dc2, ssd]
	// - attrs: [dc3, ssd]
	// range_min_bytes: 1048576
	// range_max_bytes: 67108864
}
