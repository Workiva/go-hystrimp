/*
Copyright 2015 Workiva Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// +build example

package example

import (
	"math/rand"
	"sync/atomic"
	"time"
)

const (

	// StatusOK is the code returned by the remote system when an operation succeeds.
	StatusOK = 0

	// StatusError is the code returned by the remote system when an operation fails.
	StatusError = 1
)

// RemoteSystem is a fake remote system that exposes an API for some operations. It may know nothing about Hystrimp.
// In a real-world scenario, this might be a database, a remote host running a REST/Thrift/GRPC API, etc.
// Like a real-world system, it sometimes experiences failures and degrades under load. Sometimes network requests do not reach it.
type RemoteSystem struct {
	rand *rand.Rand
	load int32
}

// NewRemoteSystem constructs an instance of the example remote system.
func NewRemoteSystem() *RemoteSystem {
	return &RemoteSystem{
		rand: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// DoFoo is an operation that the remote system exposes on its API.
func (rs *RemoteSystem) DoFoo() (statusCode int, response string) {
	// Simulate the request going over the network to reach the remote system. It may never arrive.
	rs.simulateRandomDelay(1, 1)

	// Congratulations! The remote system got the request.
	atomic.AddInt32(&rs.load, 1)

	// Processing takes time. The RemoteSystem may itself need to talk to other systems.
	// And of course, the more loaded the system is, the longer it may take
	rs.simulateRandomDelay(0, int(rs.load))

	// Remote processing done. Returning.
	atomic.AddInt32(&rs.load, -1)

	// Simulate the response going over the network back to the client. It may never arrive.
	rs.simulateRandomDelay(1, 1)

	// Sometimes requests just fail.
	if rs.rand.Intn(10) == 0 {
		return StatusError, "Something went terribly wrong"
	}

	return StatusOK, "Much success!"
}

func (rs *RemoteSystem) simulateRandomDelay(hangPercentage, delayMultiplier int) {
	// Non-deterministic stuff happens, unfortunately, particularly in distributed systems.
	maxDelay := delayMultiplier * 100
	delay := rs.rand.Intn(maxDelay)

	// Oh man, something bad happened (packet was dropped, or remote system hung).
	if delay*100/maxDelay < hangPercentage {
		// If you don't time out the request, you may be waiting forever...
		for {
			<-time.After(time.Second)
		}
	}

	// Processing and network delays can be variable.
	<-time.After(time.Duration(delay * int(time.Millisecond)))
}
