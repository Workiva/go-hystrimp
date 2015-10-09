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

package main

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/Workiva/go-hystrimp/example"
	"github.com/Workiva/go-hystrimp/hystrimp"
)

func main() {
	// Set the number of procs for running goroutines.
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Mute hystrimp logging (just for this example).
	hystrimp.SetLogLevel(hystrimp.LevelPanic)

	remote := example.NewRemoteSystem()

	fmt.Println("Using the Remote Service with a Hystrimp client (✓=success, x=failure)...")
	useClient(example.NewHystrimpClient(remote))

	fmt.Println("\n")

	fmt.Println("Using the Remote Service with a naive client (✓=success, x=failure)...")
	useClient(example.NewNaiveClient(remote))
}

func useClient(client example.RemoteClient) {
	wg := &sync.WaitGroup{}

	wg.Add(10)

	// Create parallel users of the remote system.
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 10; j++ {
				_, err := client.DoFoo()
				if err != nil {
					fmt.Printf("x")
				} else {
					fmt.Printf("✓")
				}
			}
			wg.Done()
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		done <- struct{}{}
	}()

	select {
	case <-done:
		fmt.Println("\nDone using the remote service!")
	case <-time.After(10 * time.Second):
		fmt.Println("\nTimeout while using the remote service!")
	}
}
