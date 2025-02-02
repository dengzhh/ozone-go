// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

module github.com/apache/ozone-go

go 1.13

replace github.com/apache/ozone-go/api => ../api

require (
	github.com/apache/ozone-go/api v0.0.0-20201212100630-cee64fa835db
	github.com/klauspost/reedsolomon v1.11.7 // indirect
	github.com/urfave/cli v1.22.5
	google.golang.org/grpc v1.36.0 // indirect
)
