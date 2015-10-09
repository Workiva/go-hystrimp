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

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	testService = "testService"
	testCommand = "testCommand"
)

// Ensure that no errors are raised while happily using a service
func TestHappyParallelUsers(t *testing.T) {
	assert := assert.New(t)
	sConfig, _ := getConfigs()
	service := NewService(sConfig)

	wg := &sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 10; j++ {
				err := service.Run(testCommand, func() (error, error) {
					return nil, nil
				}, nil)
				assert.Nil(err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

// Ensure that unhandled timeout errors are passed through
func TestTimeoutErrorUnhandled(t *testing.T) {
	assert := assert.New(t)
	sConfig, cConfig := getConfigs()
	cConfig.Timeout = time.Nanosecond
	service := NewService(sConfig)

	err := service.Run(testCommand, func() (error, error) {
		<-time.After(time.Millisecond)
		return nil, nil
	}, nil)

	// Expect the unhandled error
	assert.NotNil(err)
	_, ok := err.(*TimeoutError)
	assert.True(ok)
}

// Ensure that timeout errors are handled if handler is provided
func TestTimeoutErrorHandled(t *testing.T) {
	assert := assert.New(t)
	sConfig, cConfig := getConfigs()
	cConfig.Timeout = time.Nanosecond
	service := NewService(sConfig)
	handled := make(chan error, 1)

	err := service.Run(testCommand, func() (error, error) {
		<-time.After(time.Millisecond)
		return nil, nil
	}, &ErrorHandlers{Timeout: func(err error) error {
		handled <- err
		return nil
	}})

	// Expect the error to have been handled
	assert.Nil(err)
	select {
	case err = <-handled:
		_, ok := err.(*TimeoutError)
		assert.True(ok)
	case <-time.After(time.Millisecond):
		t.Fail()
	}
}

// Ensure that error raised by timeout error handler is passed through
func TestTimeoutErrorHandlerError(t *testing.T) {
	assert := assert.New(t)
	sConfig, cConfig := getConfigs()
	cConfig.Timeout = time.Nanosecond
	service := NewService(sConfig)

	handlerError := fmt.Errorf("Handler error")
	err := service.Run(testCommand, func() (error, error) {
		<-time.After(time.Millisecond)
		return nil, nil
	}, &ErrorHandlers{Timeout: func(err error) error {
		return handlerError
	}})

	// Expect the handler to have raised an error
	assert.NotNil(err)
	hErr, ok := err.(*HandlerError)
	assert.True(ok)
	assert.Equal(handlerError, hErr.Wrapped)
}

// Ensure that unhandled local errors are passed through
func TestLocalErrorUnhandled(t *testing.T) {
	assert := assert.New(t)
	sConfig, _ := getConfigs()
	service := NewService(sConfig)

	localError := fmt.Errorf("Local error")
	err := service.Run(testCommand, func() (error, error) {
		return localError, nil
	}, nil)

	// Expect the unhandled error
	assert.NotNil(err)
	lErr, ok := err.(*LocalError)
	assert.True(ok)
	assert.Equal(localError, lErr.Wrapped)
}

// Ensure that local errors are handled if handler is provided
func TestLocalErrorHandled(t *testing.T) {
	assert := assert.New(t)
	sConfig, _ := getConfigs()
	service := NewService(sConfig)
	handled := make(chan error, 1)

	localError := fmt.Errorf("Local error")
	err := service.Run(testCommand, func() (error, error) {
		return localError, nil
	}, &ErrorHandlers{Local: func(err error) error {
		handled <- err
		return nil
	}})

	// Expect the error to have been handled
	assert.Nil(err)
	select {
	case err = <-handled:
		lErr, ok := err.(*LocalError)
		assert.True(ok)
		assert.Equal(localError, lErr.Wrapped)
	case <-time.After(time.Millisecond):
		t.Fail()
	}
}

// Ensure that error raised by local error handler is passed through
func TestLocalErrorHandlerError(t *testing.T) {
	assert := assert.New(t)
	sConfig, cConfig := getConfigs()
	cConfig.Timeout = time.Nanosecond
	service := NewService(sConfig)

	handlerError := fmt.Errorf("Handler error")
	err := service.Run(testCommand, func() (error, error) {
		return fmt.Errorf("Local error"), nil
	}, &ErrorHandlers{Local: func(err error) error {
		return handlerError
	}})

	// Expect the handler to have raised an error
	assert.NotNil(err)
	hErr, ok := err.(*HandlerError)
	assert.True(ok)
	assert.Equal(handlerError, hErr.Wrapped)
}

// Ensure that unhandled remote errors are passed through
func TestRemoteErrorUnhandled(t *testing.T) {
	assert := assert.New(t)
	sConfig, _ := getConfigs()
	service := NewService(sConfig)

	remoteError := fmt.Errorf("Remote error")
	err := service.Run(testCommand, func() (error, error) {
		return nil, remoteError
	}, nil)

	// Expect the unhandled error
	assert.NotNil(err)
	rErr, ok := err.(*RemoteError)
	assert.True(ok)
	assert.Equal(remoteError, rErr.Wrapped)
}

// Ensure that remote errors are handled if handler is provided
func TestRemoteErrorHandled(t *testing.T) {
	assert := assert.New(t)
	sConfig, _ := getConfigs()
	service := NewService(sConfig)
	handled := make(chan error, 1)

	remoteError := fmt.Errorf("Remote error")
	err := service.Run(testCommand, func() (error, error) {
		return nil, remoteError
	}, &ErrorHandlers{Remote: func(err error) error {
		handled <- err
		return nil
	}})

	// Expect the error to have been handled
	assert.Nil(err)
	select {
	case err = <-handled:
		rErr, ok := err.(*RemoteError)
		assert.True(ok)
		assert.Equal(remoteError, rErr.Wrapped)
	case <-time.After(time.Millisecond):
		t.Fail()
	}
}

// Ensure that error raised by remote error handler is passed through
func TestRemoteErrorHandlerError(t *testing.T) {
	assert := assert.New(t)
	sConfig, cConfig := getConfigs()
	cConfig.Timeout = time.Nanosecond
	service := NewService(sConfig)

	handlerError := fmt.Errorf("Handler error")
	err := service.Run(testCommand, func() (error, error) {
		return nil, fmt.Errorf("Remote error")
	}, &ErrorHandlers{Remote: func(err error) error {
		return handlerError
	}})

	// Expect the handler to have raised an error
	assert.NotNil(err)
	hErr, ok := err.(*HandlerError)
	assert.True(ok)
	assert.Equal(handlerError, hErr.Wrapped)
}

// Ensure that unhandled service circuit breaker errors are passed through
func TestServiceCircuitBreakerErrorUnhandled(t *testing.T) {
	assert := assert.New(t)
	sConfig, _ := getConfigs()
	sConfig.CBRequestVolumeThreshold = 0
	sConfig.CBSleepWindow = time.Second
	service := NewService(sConfig)

	// Trip the breaker with a remote error
	remoteError := fmt.Errorf("Remote error")
	err := service.Run(testCommand, func() (error, error) {
		return nil, remoteError
	}, nil)
	assert.NotNil(err)
	rErr, ok := err.(*RemoteError)
	assert.True(ok)
	assert.Equal(remoteError, rErr.Wrapped)

	// Try another command while the breaker is tripped
	err = service.Run(testCommand, func() (error, error) {
		return nil, nil
	}, nil)

	// Expect the unhandled error
	assert.NotNil(err)
	_, ok = err.(*ServiceCircuitBreakerOpenError)
	assert.True(ok)
}

// Ensure that service circuit breaker errors are handled if handler is provided
func TestServiceCircuitBreakerErrorHandled(t *testing.T) {
	assert := assert.New(t)
	sConfig, _ := getConfigs()
	sConfig.CBRequestVolumeThreshold = 0
	sConfig.CBSleepWindow = time.Second
	service := NewService(sConfig)

	// Trip the breaker with a remote error
	remoteError := fmt.Errorf("Remote error")
	err := service.Run(testCommand, func() (error, error) {
		return nil, remoteError
	}, nil)
	assert.NotNil(err)
	rErr, ok := err.(*RemoteError)
	assert.True(ok)
	assert.Equal(remoteError, rErr.Wrapped)

	// Try another command while the breaker is tripped
	handled := make(chan error, 1)
	err = service.Run(testCommand, func() (error, error) {
		return nil, nil
	}, &ErrorHandlers{ServiceCB: func(err error) error {
		handled <- err
		return nil
	}})

	// Expect the error to have been handled
	assert.Nil(err)
	select {
	case err = <-handled:
		_, ok = err.(*ServiceCircuitBreakerOpenError)
		assert.True(ok)
	case <-time.After(time.Millisecond):
		t.Fail()
	}
}

// Ensure that error raised by service breaker error handler is passed through
func TestServiceCircuitBreakerErrorHandlerError(t *testing.T) {
	assert := assert.New(t)
	sConfig, _ := getConfigs()
	sConfig.CBRequestVolumeThreshold = 0
	sConfig.CBSleepWindow = time.Second
	service := NewService(sConfig)

	// Trip the breaker with a remote error
	remoteError := fmt.Errorf("Remote error")
	err := service.Run(testCommand, func() (error, error) {
		return nil, remoteError
	}, nil)
	assert.NotNil(err)
	rErr, ok := err.(*RemoteError)
	assert.True(ok)
	assert.Equal(remoteError, rErr.Wrapped)

	// Try another command while the breaker is tripped
	handlerError := fmt.Errorf("Handler error")
	err = service.Run(testCommand, func() (error, error) {
		return nil, nil
	}, &ErrorHandlers{ServiceCB: func(err error) error {
		return handlerError
	}})

	// Expect the handler to have raised an error
	assert.NotNil(err)
	hErr, ok := err.(*HandlerError)
	assert.True(ok)
	assert.Equal(handlerError, hErr.Wrapped)
}

// Ensure that unhandled command circuit breaker errors are passed through
func TestCommandCircuitBreakerErrorUnhandled(t *testing.T) {
	assert := assert.New(t)
	sConfig, cConfig := getConfigs()
	cConfig.CBRequestVolumeThreshold = 0
	cConfig.CBSleepWindow = time.Second
	service := NewService(sConfig)

	// Trip the breaker with a remote error
	remoteError := fmt.Errorf("Remote error")
	err := service.Run(testCommand, func() (error, error) {
		return nil, remoteError
	}, nil)
	assert.NotNil(err)
	rErr, ok := err.(*RemoteError)
	assert.True(ok)
	assert.Equal(remoteError, rErr.Wrapped)

	// Try another command while the breaker is tripped
	err = service.Run(testCommand, func() (error, error) {
		return nil, nil
	}, nil)

	// Expect the unhandled error
	assert.NotNil(err)
	_, ok = err.(*CommandCircuitBreakerOpenError)
	assert.True(ok)
}

// Ensure that command circuit breaker errors are handled if handler is provided
func TestCommandCircuitBreakerErrorHandled(t *testing.T) {
	assert := assert.New(t)
	sConfig, cConfig := getConfigs()
	cConfig.CBRequestVolumeThreshold = 0
	cConfig.CBSleepWindow = time.Second
	service := NewService(sConfig)

	// Trip the breaker with a remote error
	remoteError := fmt.Errorf("Remote error")
	err := service.Run(testCommand, func() (error, error) {
		return nil, remoteError
	}, nil)
	assert.NotNil(err)
	rErr, ok := err.(*RemoteError)
	assert.True(ok)
	assert.Equal(remoteError, rErr.Wrapped)

	// Try another command while the breaker is tripped
	handled := make(chan error, 1)
	err = service.Run(testCommand, func() (error, error) {
		return nil, nil
	}, &ErrorHandlers{CommandCB: func(err error) error {
		handled <- err
		return nil
	}})

	// Expect the error to have been handled
	assert.Nil(err)
	select {
	case err = <-handled:
		_, ok = err.(*CommandCircuitBreakerOpenError)
		assert.True(ok)
	case <-time.After(time.Millisecond):
		t.Fail()
	}
}

// Ensure that command circuit breaker errors are handled if handler is provided
func TestCommandCircuitBreakerErrorHandlerError(t *testing.T) {
	assert := assert.New(t)
	sConfig, cConfig := getConfigs()
	cConfig.CBRequestVolumeThreshold = 0
	cConfig.CBSleepWindow = time.Second
	service := NewService(sConfig)

	// Trip the breaker with a remote error
	remoteError := fmt.Errorf("Remote error")
	err := service.Run(testCommand, func() (error, error) {
		return nil, remoteError
	}, nil)
	assert.NotNil(err)
	rErr, ok := err.(*RemoteError)
	assert.True(ok)
	assert.Equal(remoteError, rErr.Wrapped)

	// Try another command while the breaker is tripped
	handlerError := fmt.Errorf("Handler error")
	err = service.Run(testCommand, func() (error, error) {
		return nil, nil
	}, &ErrorHandlers{CommandCB: func(err error) error {
		return handlerError
	}})

	// Expect the handler to have raised an error
	assert.NotNil(err)
	hErr, ok := err.(*HandlerError)
	assert.True(ok)
	assert.Equal(handlerError, hErr.Wrapped)
}

// Ensure that service parallelism constraints are respected
func TestServiceParallelismConstraint(t *testing.T) {
	assert := assert.New(t)
	sConfig, _ := getConfigs()
	sConfig.MaxConcurrentCommands = 1
	service := NewService(sConfig)

	counter := uint64(0)

	wg := &sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 10; j++ {
				err := service.Run(testCommand, func() (error, error) {
					if !atomic.CompareAndSwapUint64(&counter, 0, 1) {
						t.FailNow()
					}

					time.Sleep(time.Millisecond)

					if !atomic.CompareAndSwapUint64(&counter, 1, 0) {
						t.FailNow()
					}

					return nil, nil
				}, nil)
				assert.Nil(err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

// Ensure that command parallelism constraints are respected
func TestCommandParallelismConstraint(t *testing.T) {
	assert := assert.New(t)
	sConfig, cConfig := getConfigs()
	cConfig.MaxConcurrentCommands = 1
	service := NewService(sConfig)

	counter := uint64(0)

	wg := &sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 10; j++ {
				err := service.Run(testCommand, func() (error, error) {
					if !atomic.CompareAndSwapUint64(&counter, 0, 1) {
						t.FailNow()
					}

					time.Sleep(time.Millisecond)

					if !atomic.CompareAndSwapUint64(&counter, 1, 0) {
						t.FailNow()
					}

					return nil, nil
				}, nil)
				assert.Nil(err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

// Ensure that circuit breakers trip only when request volume and error percentage are above thresholds
func TestServiceCircuitBreakerTripCriteria(t *testing.T) {
	assert := assert.New(t)
	sConfig, _ := getConfigs()
	sConfig.CBRequestVolumeThreshold = 10
	sConfig.CBErrorPercentThreshold = 10
	sConfig.CBSleepWindow = time.Millisecond
	service := NewService(sConfig)

	// Error that will eventually trip the breaker
	remoteError := fmt.Errorf("Remote error")
	err := service.Run(testCommand, func() (error, error) {
		return nil, remoteError
	}, nil)
	assert.NotNil(err)
	rErr, ok := err.(*RemoteError)
	assert.True(ok)
	assert.Equal(remoteError, rErr.Wrapped)

	// Next 9 requests should run without issue because volume threshold not reached
	for i := 0; i < 9; i++ {
		err := service.Run(testCommand, func() (error, error) {
			return nil, nil
		}, nil)
		assert.Nil(err)
	}

	// After 10 requests and 1 error (10% failure rate) we expect the breaker to trip
	err = service.Run(testCommand, func() (error, error) {
		return nil, nil
	}, nil)
	assert.NotNil(err)
	_, ok = err.(*ServiceCircuitBreakerOpenError)
	assert.True(ok)
}

// Ensure that circuit breakers trip only when request volume and error percentage are above thresholds
func TestCommandCircuitBreakerTripCriteria(t *testing.T) {
	assert := assert.New(t)
	sConfig, cConfig := getConfigs()
	cConfig.CBRequestVolumeThreshold = 10
	cConfig.CBErrorPercentThreshold = 10
	cConfig.CBSleepWindow = time.Millisecond
	service := NewService(sConfig)

	// Error that will eventually trip the breaker
	remoteError := fmt.Errorf("Remote error")
	err := service.Run(testCommand, func() (error, error) {
		return nil, remoteError
	}, nil)
	assert.NotNil(err)
	rErr, ok := err.(*RemoteError)
	assert.True(ok)
	assert.Equal(remoteError, rErr.Wrapped)

	// Next 9 requests should run without issue because volume threshold not reached
	for i := 0; i < 9; i++ {
		err := service.Run(testCommand, func() (error, error) {
			return nil, nil
		}, nil)
		assert.Nil(err)
	}

	// After 10 requests and 1 error (10% failure rate) we expect the breaker to trip
	err = service.Run(testCommand, func() (error, error) {
		return nil, nil
	}, nil)
	assert.NotNil(err)
	_, ok = err.(*CommandCircuitBreakerOpenError)
	assert.True(ok)
}

// Ensure that the service circuit breaker resets after being tripped
func TestServiceCircuitBreakerReset(t *testing.T) {
	assert := assert.New(t)
	sConfig, _ := getConfigs()
	sConfig.CBRequestVolumeThreshold = 0
	sConfig.CBSleepWindow = 10 * time.Millisecond
	service := NewService(sConfig)

	// Trip the breaker with a remote error
	remoteError := fmt.Errorf("Remote error")
	err := service.Run(testCommand, func() (error, error) {
		return nil, remoteError
	}, nil)
	assert.NotNil(err)
	rErr, ok := err.(*RemoteError)
	assert.True(ok)
	assert.Equal(remoteError, rErr.Wrapped)

	// Try another command while the breaker is tripped
	err = service.Run(testCommand, func() (error, error) {
		return nil, nil
	}, nil)

	// Expect the unhandled error
	assert.NotNil(err)
	_, ok = err.(*ServiceCircuitBreakerOpenError)
	assert.True(ok)

	// Wait awhile
	<-time.After(20 * time.Millisecond)

	// Expect the breaker to be reset
	err = service.Run(testCommand, func() (error, error) {
		return nil, nil
	}, nil)
	assert.Nil(err)
}

// Ensure that the command circuit breaker resets after being tripped
func TestCommandCircuitBreakerReset(t *testing.T) {
	assert := assert.New(t)
	sConfig, cConfig := getConfigs()
	cConfig.CBRequestVolumeThreshold = 0
	cConfig.CBSleepWindow = 10 * time.Millisecond
	service := NewService(sConfig)

	// Trip the breaker with a remote error
	remoteError := fmt.Errorf("Remote error")
	err := service.Run(testCommand, func() (error, error) {
		return nil, remoteError
	}, nil)
	assert.NotNil(err)
	rErr, ok := err.(*RemoteError)
	assert.True(ok)
	assert.Equal(remoteError, rErr.Wrapped)

	// Try another command while the breaker is tripped
	err = service.Run(testCommand, func() (error, error) {
		return nil, nil
	}, nil)

	// Expect the unhandled error
	assert.NotNil(err)
	_, ok = err.(*CommandCircuitBreakerOpenError)
	assert.True(ok)

	// Wait awhile
	<-time.After(20 * time.Millisecond)

	// Expect the breaker to be reset
	err = service.Run(testCommand, func() (error, error) {
		return nil, nil
	}, nil)
	assert.Nil(err)
}

// Ensure that automatic retry for timeout errors works
func TestTimeoutErrorAutoRetry(t *testing.T) {
	assert := assert.New(t)
	sConfig, cConfig := getConfigs()
	sConfig.CBRequestVolumeThreshold = 10
	cConfig.CBRequestVolumeThreshold = 10
	cConfig.Timeout = time.Nanosecond
	cConfig.Retries = 5
	service := NewService(sConfig)

	timesRun := 0
	err := service.Run(testCommand, func() (error, error) {
		timesRun++
		<-time.After(time.Millisecond)
		return nil, nil
	}, nil)
	assert.NotNil(err)
	_, ok := err.(*TimeoutError)
	assert.True(ok)
	assert.Equal(cConfig.Retries+1, timesRun)
}

// Ensure that automatic retry for remote errors works
func TestRemoteErrorAutoRetry(t *testing.T) {
	assert := assert.New(t)
	sConfig, cConfig := getConfigs()
	sConfig.CBRequestVolumeThreshold = 10
	cConfig.CBRequestVolumeThreshold = 10
	cConfig.Retries = 5
	service := NewService(sConfig)

	timesRun := 0
	remoteError := fmt.Errorf("Remote error")
	err := service.Run(testCommand, func() (error, error) {
		timesRun++
		return nil, remoteError
	}, nil)
	assert.NotNil(err)
	rErr, ok := err.(*RemoteError)
	assert.True(ok)
	assert.Equal(remoteError, rErr.Wrapped)
	assert.Equal(cConfig.Retries+1, timesRun)
}

// Return service and command configurations at default values, with the command preregistered to the service
func getConfigs() (*ServiceConfiguration, *CommandConfiguration) {
	commandConfig := NewCommandConfiguration(testCommand)
	return &ServiceConfiguration{
		CommonConfiguration:     *NewCommonConfiguration(testService),
		CommandPreregistrations: []*CommandConfiguration{commandConfig},
	}, commandConfig
}
