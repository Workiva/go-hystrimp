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
