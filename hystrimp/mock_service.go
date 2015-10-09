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

package hystrimp

import "github.com/stretchr/testify/mock"

type MockService struct {
	mock.Mock
}

func (m *MockService) RegisterCommand(config *CommandConfiguration) {
	m.Called(config)
}

func (m *MockService) Run(name string, command Command, localHandler, timeoutHandler, remoteHandler, serviceCircuitBreakerOpenHandler,
	commandCircuitBreakerOpenHandler ErrorHandler) error {
	args := m.Called(name, command, localHandler, timeoutHandler, remoteHandler, serviceCircuitBreakerOpenHandler, commandCircuitBreakerOpenHandler)
	return args.Error(0)
}

func (m *MockService) RunAsync(name string, command Command, localHandler, timeoutHandler, remoteHandler, serviceCircuitBreakerOpenHandler,
	commandCircuitBreakerOpenHandler ErrorHandler) chan<- error {
	args := m.Called(name, command, localHandler, timeoutHandler, remoteHandler, serviceCircuitBreakerOpenHandler, commandCircuitBreakerOpenHandler)
	return args.Get(0).(chan<- error)
}
