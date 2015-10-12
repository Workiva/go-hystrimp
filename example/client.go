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
	"fmt"
	"time"

	"github.com/Workiva/go-hystrimp/hystrimp"
)

const (
	// HRemoteSystem is the configured Hystrimp name for the remote system
	HRemoteSystem = "Remote System"

	// HDoFoo is the configured Hystrimp name for the command exposed by the remote system
	HDoFoo = "DoFoo"
)

// RemoteClient is a client that uses the RemoteService.
type RemoteClient interface {
	DoFoo() (response string, err error)
}

// naiveClient was conceived by a person who really didn't think about robustness.
type naiveClient struct {
	remote *RemoteSystem
}

// hystrimpClient uses Hystrimp to robustly wrap the external dependency.
type hystrimpClient struct {
	remote   *RemoteSystem
	hService hystrimp.Service
}

// NewNaiveClient instantiates the naive client.
func NewNaiveClient(remote *RemoteSystem) RemoteClient {
	return &naiveClient{
		remote: remote,
	}
}

// NewHystrimpClient instantiates the Hystrimp client.
func NewHystrimpClient(remote *RemoteSystem) RemoteClient {
	return &hystrimpClient{
		remote: remote,
		hService: hystrimp.NewService(&hystrimp.ServiceConfiguration{
			CommonConfiguration: hystrimp.CommonConfiguration{
				Name: HRemoteSystem,
				MaxConcurrentCommands:    10,
				CBSleepWindow:            time.Millisecond,
				CBRequestVolumeThreshold: 10,
				CBErrorPercentThreshold:  50,
			},

			CommandPreregistrations: []*hystrimp.CommandConfiguration{
				&hystrimp.CommandConfiguration{
					CommonConfiguration: hystrimp.CommonConfiguration{
						Name: HDoFoo,
						MaxConcurrentCommands:    10,
						CBSleepWindow:            time.Millisecond,
						CBRequestVolumeThreshold: 10,
						CBErrorPercentThreshold:  50,
					},
					Timeout:             time.Second,
					Retries:             100,
					RetryStrategy:       hystrimp.RetryStrategyExponentialBackoff,
					RetryInitialWait:    time.Millisecond,
					RetryBackoffCeiling: time.Second,
				},
			},
		}),
	}
}

func (c *naiveClient) DoFoo() (response string, err error) {
	// The author of this code foolishly treated this use of an external dependency like a local function call.
	status, response := c.remote.DoFoo()
	if status == StatusError {
		return "", fmt.Errorf("Received failure %d:%s", status, response)
	}
	return response, nil
}

func (c *hystrimpClient) DoFoo() (response string, err error) {
	err = c.hService.Run(HDoFoo,
		// We have wrapped the interaction with DoFoo inside a Hystrimp command
		// because we recognize it is definitely NOT a local function call!
		func() (localError, remoteError error) {
			var status int
			status, response = c.remote.DoFoo()
			if status == StatusError {
				// This was a failure on the remote system, not locally. Return it as such.
				return nil, fmt.Errorf("Received failure %d:%s", status, response)
			}
			return nil, nil
		},
		// We don't have any novel strategy for handling various error types in this example,
		// but let's log warnings at a bare minimum.
		hystrimp.WarnHandlers)
	if err != nil {
		return "", err
	}

	return response, nil
}
