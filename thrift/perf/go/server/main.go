/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"context"
	"errors"
	"flag"
	"net"
	"os"
	"os/signal"
	"syscall"

	"apache/thrift/test/load"
	"github.com/facebook/fbthrift/thrift/lib/go/thrift"

	"github.com/golang/glog"
)

func init() {
	flag.Set("logtostderr", "true") // make glog log to stderr by default
}

func main() {
	host := flag.String("host", "", "Server host")
	port := flag.String("port", "7777", "Server port")
	flag.Parse()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, os.Interrupt)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-sig
		glog.Info("Shutting down.")
		// Do any cleanup work here
		cancel()
	}()
	addr := net.JoinHostPort(*host, *port)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		glog.Fatalf("failed to listen: %v", err)
	}

	proc := load.NewLoadTestProcessor(&LoadTestHandler{})
	server := thrift.NewServer(proc, listener, thrift.TransportIDUpgradeToRocket)

	glog.Infof("starting thrift server on '%s'", addr)
	err = server.ServeContext(ctx)
	if err == nil || errors.Is(err, context.Canceled) {
		glog.Info("Thrift server terminated")
	} else {
		glog.Fatalf("unexpected server error: %v", err)
	}
}
