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

/* USAGE EXAMPLE
// Create a configured Hystrimp service
fooService := hystrimp.NewService(&ServiceConfiguration{
	CommonConfiguration: {
		Name: "FooService", // Name of the remote service wrapped with Hystrimp
		MaxConcurrentCommands: 10, // Allow at most 10 threads to use the service at once
		CBSleepWindow: time.Minute, // Circuit breaker stays tripped for 1 minute
		CBRequestVolumeThreshold: 10, // Don't trip the breaker until at least this many requests are made
		CBErrorPercentThreshold: 10, // Trip the breaker if more than 10% of the commands have failed since the last time the breaker tripped
	}
})

// Register a command to the Hystrimp service. Can also be done as part of configuring the service.
fooService.RegisterCommand(&CommandConfiguration{
	Name: "DoStuff", // Name of the command
	MaxConcurrentCommands: 1, // Allow at most 1 thread to use the command at once
	CBSleepWindow: time.Minute, // Circuit breaker stays tripped for 1 minute
	CBRequestVolumeThreshold: 10, // Don't trip the breaker until at least this many requests are made
	CBErrorPercentThreshold: 10, // Trip the breaker if more than 10% of the commands have failed since the last time the breaker tripped
	Timeout: time.Second, // Raise a timeout error if the command takes more than 1s to execute
	Retries: 100, // Retry the command up to 100 times when unhandled timeout or remote errors occur
	RetryInitialWait: time.Millisecond, // First wait will be 1ms
	RetryStrategy: RetryStrategyExponentialBackoff, // Back off expontentially with subsequent failures between retries
	RetryBackoffCeiling: 10 * time.Second, // Cap exponential backoff at 1s
})

// Make a fully-specified Hystrimp call. Any error that is ultimately unhandled will be returned.
err := fooService.Run("DoStuff", func() (localErr, remoteErr error){
	// This is the wrapped call to FooService.DoStuff
	// If errors are encountered, assign blame either to the local or remote
}, &ErrorHandlers{
	Local: func(err error) error { ... },
	Remote:  func(err error) error { ... },
	Timeout:  func(err error) error { ... },
	CommandCB:  func(err error) error { ... },
	ServiceCB:   func(err error) error { ... },
})

// Errors are nicely-typed so you can deal with them by kind
switch typedErr := err.(type) {
	case *LocalError:
	case *RemoteError:
	case *TimeoutError:
	case *HandlerError:
	case *ServiceCircuitBreakerOpenError:
	case *CommandCircuitBreakerOpenError:
	default:
		// Should never be reachable
}

// Make a lazy man's Hystrimp call with no error handlers given.
err = fooService.Run("DoStuff", func() (localErr, remoteErr error){
	// This is the wrapped call to FooService.DoStuff
	// If errors are encountered, assign blame either to the local or remote
}, nil)
*/

// Package hystrimp contains a Go implementation of Netflix's Hystrix project.
package hystrimp

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
)

const (
	// RetryStrategyImmediate retries immediately.
	RetryStrategyImmediate RetryWaitStrategy = iota
	// RetryStrategyFixedDelay retries after a fixed delay.
	RetryStrategyFixedDelay
	// RetryStrategyExponentialBackoff retries after an exponentially-growing backoff.
	RetryStrategyExponentialBackoff

	// LevelDebug logs debug events and above.
	LevelDebug LogLevel = iota
	// LevelInfo logs only info events and above.
	LevelInfo
	// LevelWarn logs only warning events and above.
	LevelWarn
	// LevelError logs only error events and above.
	LevelError
	// LevelFatal logs only fatal events and above.
	LevelFatal
	// LevelPanic logs only panic events.
	LevelPanic

	//  exponentialBackoffFactor is the factor by which an exponential backoff grows each time.
	exponentialBackoffFactor = 2
)

var (
	// WarnHandler "handles" errors by logging them as warnings and returning them (doesn't really handle them).
	WarnHandler = func(err error) (handled bool, handlerError error) {
		log.Warnln(err)
		return false, nil
	}

	// WarnHandlers is a set of error handlers which log warnings for each kind of error.
	WarnHandlers = &ErrorHandlers{
		Local:     WarnHandler,
		Timeout:   WarnHandler,
		Remote:    WarnHandler,
		ServiceCB: WarnHandler,
		CommandCB: WarnHandler,
	}

	// This non-default Logger is used for logging.
	log = logrus.New()
)

// This function runs when file is first loaded by Go.
func init() {
	// The default log level is warn.
	SetLogLevel(LevelWarn)
}

// LogLevel defines the verbosity of logging.
type LogLevel uint8

// RetryWaitStrategy is a method of waiting between retrying commands after remote failures.
type RetryWaitStrategy uint8

// Service sends commands to a wrapped remote service.
type Service interface {
	// RegisterCommand registers a command so that it can be run.
	RegisterCommand(config *CommandConfiguration)

	// Run executes the given registered command synchronously.
	// If error handlers are provided, invokes the appropriate error handler.
	// Returns any unhandled error (one of the error types below), else nil
	Run(name string, command Command, handlers *ErrorHandlers) error

	// RunAsync executes the given registered command asynchronously.
	// If error handlers are provided, invoke the appropriate error handler when errors are encountered.
	// Returns any unhandled error (one of the error types below), else nil
	RunAsync(name string, command Command, handlers *ErrorHandlers) chan<- error
}

// CommandConfiguration defines options with which commands should be run.
type CommandConfiguration struct {
	// CommonConfiguration defines options common to both commands and services.
	CommonConfiguration

	// Timeout defines how long to wait for the command to finish before raising a TimeoutError.
	Timeout time.Duration

	// Retries defines the maximum number of retries when an unhandled TimeoutError or RemoteError is encountered.
	Retries int

	// RetryStrategy defines how to wait between retries.
	RetryStrategy RetryWaitStrategy

	// RetryInitialWait defines the initial wait between retries.
	RetryInitialWait time.Duration

	// RetryBackoffCeiling defines the maximum amount of time between retries, even with exponential backoff.
	RetryBackoffCeiling time.Duration
}

// ServiceConfiguration defines options to configure a service.
type ServiceConfiguration struct {
	// CommonConfiguration defines options common to both commands and services.
	CommonConfiguration

	// CommandPreregistrations is a convenience for pre-registering commands at the time of service creation.
	CommandPreregistrations []*CommandConfiguration
}

// CommonConfiguration defines options that apply to both commands and services.
type CommonConfiguration struct {
	// Name is the name of the command or service.
	Name string

	// MaxConcurrentCommands bounds the number of parallel instances of the command that may run at once.
	MaxConcurrentCommands int

	// CBSleepWindow defines how long to wait after a circuit breaker opens before testing for recovery.
	CBSleepWindow time.Duration

	// CBRequestVolumeThreshold defines the minimum number of requests needed before a circuit can be tripped.
	CBRequestVolumeThreshold int

	// CBErrorPercentThreshold defines a failure percentage for requests that will cause the circuit breaker to be tripped.
	CBErrorPercentThreshold int
}

// NewCommonConfiguration constructs a new CommonConfiguration with some reasonable default values.
func NewCommonConfiguration(name string) *CommonConfiguration {
	return &CommonConfiguration{
		Name: name,
		MaxConcurrentCommands:    math.MaxInt32,
		CBSleepWindow:            5 * time.Second,
		CBRequestVolumeThreshold: 5,
		CBErrorPercentThreshold:  50,
	}
}

// NewCommandConfiguration constructs a new CommandConfiguration with some reasonable default values.
func NewCommandConfiguration(name string) *CommandConfiguration {
	return &CommandConfiguration{
		CommonConfiguration: *NewCommonConfiguration(name),
		Timeout:             10 * time.Minute,
		Retries:             0,
		RetryStrategy:       RetryStrategyExponentialBackoff,
		RetryInitialWait:    10 * time.Millisecond,
		RetryBackoffCeiling: time.Hour,
	}
}

// Command is a wrapped operation of a remote service.
// Errors that are encountered locally are returned as localError.
// Errors that are caused remotely are returned through remoteError.
type Command func() (localError, remoteError error)

// ErrorHandler handles an error that was encountered while attempting to run a Command.
// Returns nil if the handler was able to handle the error. May return new errors that occur within the handler.
type ErrorHandler func(err error) (handled bool, handlerError error)

// ErrorHandlers defines a set of error handlers for use while attempting to run a command.
type ErrorHandlers struct {
	// Local handles errors that occur locally within the command.
	Local ErrorHandler
	// Timeout handles the command timing out.
	Timeout ErrorHandler
	// Remote handles errors for which the remote service is responsible.
	Remote ErrorHandler
	// CommandCB handles the command circuit breaker being open.
	CommandCB ErrorHandler
	// ServiceCB handles the service circuit breaker being open.
	ServiceCB ErrorHandler
}

// TimeoutError arises when command execution times out.
type TimeoutError struct {
	// Elapsed is the amount of time after which this timeout error was raised.
	Elapsed time.Duration
}

func (err *TimeoutError) Error() string {
	return fmt.Sprintf("Timeout after %v", err.Elapsed)
}

// ServiceCircuitBreakerOpenError arises when the service's circuit breaker is open at the time of running a command.
type ServiceCircuitBreakerOpenError struct {
	// Name is the name of the service whose circuit breaker is open.
	Name string
}

func (err *ServiceCircuitBreakerOpenError) Error() string {
	return fmt.Sprintf("Service %s circuit breaker open", err.Name)
}

// CommandCircuitBreakerOpenError arises when the command's circuit breaker is open at the time of running a command.
type CommandCircuitBreakerOpenError struct {
	// Name is the name of the command whose circuit breaker is open.
	Name string
}

func (err *CommandCircuitBreakerOpenError) Error() string {
	return fmt.Sprintf("Command %s circuit breaker open", err.Name)
}

// LocalError arises when the command encounters a local error that was not returned by the remote system.
type LocalError struct {
	// Wrapped is the underlying local error.
	Wrapped error
}

func (err *LocalError) Error() string {
	return err.Wrapped.Error()
}

// RemoteError arises when the command encounters an error that was returned by the remote system.
type RemoteError struct {
	// Wrapped is the underlying remote error.
	Wrapped error
}

func (err *RemoteError) Error() string {
	return err.Wrapped.Error()
}

// HandlerError arises when a handler attempts unsuccessfully to handle an error.
type HandlerError struct {
	// Input is the error that the handler attempted to handle.
	Input error
	// Wrapped is the underlying error that the handler returned.
	Wrapped error
}

func (err *HandlerError) Error() string {
	return err.Wrapped.Error()
}

// NewService constructs a new Service with the provided configuration.
func NewService(config *ServiceConfiguration) Service {
	h := &service{
		config:  newCommonConfiguration(config.CommonConfiguration),
		configs: map[string]*commandConfiguration{},
	}

	if config.CommandPreregistrations != nil {
		for _, commandConfig := range config.CommandPreregistrations {
			h.RegisterCommand(commandConfig)
		}
	}

	return h
}

func (h *service) RegisterCommand(config *CommandConfiguration) {
	if _, alreadyConfigured := h.configs[config.Name]; alreadyConfigured {
		log.WithField("command name", config.Name).Warningln("Already had configuration for command. Reconfiguring as requested.")
	}

	h.configs[config.Name] = newCommandConfiguration(config)
}

func (h *service) Run(name string, command Command, handlers *ErrorHandlers) error {
	log.WithFields(logrus.Fields{
		"service name": h.config.name,
		"command name": name,
		"command":      command,
		"handlers":     handlers,
	}).Debugln("Running command")

	if handlers == nil {
		handlers = &ErrorHandlers{}
	}

	// Respect max parallelism constraint on service
	h.config.semaphore.down()
	defer h.config.semaphore.up()

	// Respect max parallelism constraint on command
	commandConfig := h.configs[name]
	commandConfig.semaphore.down()
	defer commandConfig.semaphore.up()

	retryWait := commandConfig.retryInitialWait

	var unhandled error
	for i := 0; i <= commandConfig.retries; i++ {
		// Check the circuit breakers
		if breakerOpen, err := checkBreakers(h.config, &commandConfig.commonConfiguration, handlers); breakerOpen {
			return err
		}

		// Run the command asynchronously so that timeouts can be detected
		done := make(chan struct{})
		var localError, remoteError error
		go func() {
			localError, remoteError = command()
			done <- struct{}{}
		}()
		select {
		case <-done:
			if localError != nil {
				// Local errors are not considered errors of the remote command/service.
				// Thus we do not retry if we encounter one.
				return runHandler(handlers.Local, &LocalError{Wrapped: localError})
			} else if remoteError != nil {
				// Record the failure
				recordCommandResult(h.config, &commandConfig.commonConfiguration, 0, 1)
				if unhandled = runHandler(handlers.Remote, &RemoteError{Wrapped: remoteError}); unhandled != nil {
					break // Error not handled, retry if allowed
				}

				// Error handled
				return nil
			} else {
				// Record the success
				recordCommandResult(h.config, &commandConfig.commonConfiguration, 1, 0)

				// No error to handle
				return nil
			}
		case <-time.After(commandConfig.timeout):
			// Record the failure
			recordCommandResult(h.config, &commandConfig.commonConfiguration, 0, 1)
			if unhandled = runHandler(handlers.Timeout, &TimeoutError{Elapsed: commandConfig.timeout}); unhandled != nil {
				break // Error not handled, retry if allowed
			}

			// Error handled
			return nil
		}

		// Execute backoff before retry
		retryWait = backoff(commandConfig, retryWait)
	}

	return unhandled
}

func (h *service) RunAsync(name string, command Command, handlers *ErrorHandlers) chan<- error {
	log.WithFields(logrus.Fields{
		"service name": h.config.name,
		"command name": name,
		"command":      command,
		"handlers":     handlers,
	}).Debugln("Running command asynchronously")

	errChan := make(chan error, 1)
	go func() {
		errChan <- h.Run(name, command, handlers)
	}()
	return errChan
}

// SetLogLevel configures the verbosity of logging to the given level.
func SetLogLevel(level LogLevel) error {
	switch level {
	case LevelDebug:
		log.Level = logrus.DebugLevel
	case LevelInfo:
		log.Level = logrus.InfoLevel
	case LevelWarn:
		log.Level = logrus.WarnLevel
	case LevelError:
		log.Level = logrus.ErrorLevel
	case LevelFatal:
		log.Level = logrus.FatalLevel
	case LevelPanic:
		log.Level = logrus.PanicLevel
	default:
		fmt.Errorf("Unknown log level %v", level)
	}
	return nil
}

// Implementation of Service
type service struct {
	config  *commonConfiguration             // Configuration for the service
	configs map[string]*commandConfiguration // Configurations for each command
}

// Options with which to register/execute remote commands
type commandConfiguration struct {
	commonConfiguration

	// How long to wait for the command to run before raising a TimeoutError
	timeout time.Duration

	// Maximum number of retries when unhandled errors are encountered
	retries int

	// How to wait between retries
	retryStrategy RetryWaitStrategy

	// Amount of time for the first retry wait (applies to fixed and backoff strategies)
	retryInitialWait time.Duration

	// Maximum amount of time between retries
	retryBackoffCeiling time.Duration
}

// Common configuration options that apply to both commands and services
type commonConfiguration struct {
	// Name of the command or service
	name string

	// How long to wait after a circuit opens before testing for recovery
	cBSleepWindow time.Duration

	// The minimum number of requests needed before a circuit can be tripped due to health
	cBRequestVolumeThreshold int

	// Causes circuit to open once the rolling measure of remote errors exceeds this percent of requests
	cBErrorPercentThreshold int

	// Used to manage maximum parallelism
	semaphore semaphore

	// Lock for synchronizing around circuit breaker decisions
	breakerLock *sync.Mutex

	// Circuit breaker
	breakerOpen bool

	// Number of times this has run without a remote error/timeout
	successes int

	// Number of times this has run with a remote error/timeout
	failures int
}

// Converts public configuration to an equivalent private, immutable configuration
func newCommonConfiguration(config CommonConfiguration) *commonConfiguration {
	return &commonConfiguration{
		name:                     config.Name,
		cBSleepWindow:            config.CBSleepWindow,
		cBRequestVolumeThreshold: config.CBRequestVolumeThreshold,
		cBErrorPercentThreshold:  config.CBErrorPercentThreshold,
		semaphore:                newSemaphore(config.MaxConcurrentCommands),
		breakerLock:              &sync.Mutex{},
		successes:                0,
		failures:                 0,
	}
}

// Converts public configuration to an equivalent private, immutable configuration
func newCommandConfiguration(config *CommandConfiguration) *commandConfiguration {
	return &commandConfiguration{
		commonConfiguration: *newCommonConfiguration(config.CommonConfiguration),
		timeout:             config.Timeout,
		retries:             config.Retries,
		retryStrategy:       config.RetryStrategy,
		retryInitialWait:    config.RetryInitialWait,
		retryBackoffCeiling: config.RetryBackoffCeiling,
	}
}

// Check the circuit breakers, returning whether one was open an unhandled error if a handler doesn't handle it
func checkBreakers(serviceConfig, commandConfig *commonConfiguration, handlers *ErrorHandlers) (open bool, unhandled error) {
	if serviceConfig.breakerOpen {
		return true, runHandler(handlers.ServiceCB, &ServiceCircuitBreakerOpenError{Name: serviceConfig.name})
	}

	if commandConfig.breakerOpen {
		return true, runHandler(handlers.CommandCB, &CommandCircuitBreakerOpenError{Name: commandConfig.name})
	}

	return false, nil
}

// Back off before retrying a command with unhandled errors
func backoff(config *commandConfiguration, currentWait time.Duration) time.Duration {
	// No retries, or else retry with no wait
	if config.retries == 0 || config.retryStrategy == RetryStrategyImmediate {
		return currentWait
	}

	// Wait for the current backoff
	<-time.After(currentWait)

	// Exponential backoff with ceiling
	if config.retryStrategy == RetryStrategyExponentialBackoff {
		nextWait := time.Duration(int64(currentWait) * exponentialBackoffFactor)
		if nextWait > config.retryBackoffCeiling {
			nextWait = config.retryBackoffCeiling
		}
		return nextWait
	}

	// Fixed backoff, so next wait = current wait
	return currentWait
}

// Apply the handler to the error, returning the result
func runHandler(handler ErrorHandler, toHandle error) error {
	log.WithField("error", toHandle).Debugln("Handling error")

	if handler == nil {
		log.Debugln("No handler available")
		return toHandle
	}

	handled, handlerError := handler(toHandle)
	if handlerError != nil {
		log.WithFields(logrus.Fields{
			"toHandle":     toHandle,
			"handlerError": handlerError,
		}).Debugln("Handler encountered its own error.")
		return &HandlerError{Input: toHandle, Wrapped: handlerError}
	}
	if !handled {
		log.WithField("error", toHandle).Debugln("Handler unable to handle error.")
		return toHandle
	}

	log.WithField("error", toHandle).Debugln("Error successfully handled")
	return nil
}

// Record the given number of successes and failures
func recordCommandResult(serviceConfig, commandConfig *commonConfiguration, success, failure int) {
	possiblyTripBreaker(serviceConfig, success, failure)
	possiblyTripBreaker(commandConfig, success, failure)
}

// Trip the given circuit breaker, asynchronously resetting it after a delay
// Assert: the lock is already held by the caller
func possiblyTripBreaker(config *commonConfiguration, success, failure int) {
	config.breakerLock.Lock()
	defer config.breakerLock.Unlock()
	if config.breakerOpen {
		// Another thread beat us here and tripped the breaker already. Nothing to do
		return
	}
	config.successes = config.successes + success
	config.failures = config.failures + failure
	// Trip the service breaker if enough results recorded and failure percentage too high
	if config.successes+config.failures >= config.cBRequestVolumeThreshold &&
		100*config.failures/(config.successes+config.failures) >= config.cBErrorPercentThreshold {
		log.WithFields(logrus.Fields{
			"name":  config.name,
			"sleep": config.cBSleepWindow,
		}).Infoln("Circuit breaker tripped!")
		config.breakerOpen = true
		go func() {
			<-time.After(config.cBSleepWindow)

			// Reset the breaker
			config.breakerLock.Lock()
			config.failures = 0
			config.successes = 0
			config.breakerOpen = false
			log.WithField("name", config.name).Infoln("Circuit breaker reset.")
			config.breakerLock.Unlock()
		}()
	}
}
