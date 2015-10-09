// HYSTRIx iMProved

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
}, func(err error) error{
	// Handle local errors returned by the command
	// Return nil if handled, else return any new problem
}, func(err error) error{
	// Handle timeout of the command
	// Return nil if handled, else return any new problem
}, func(err error) error{
	// Handle remote errors from the command
	// Return nil if handled, else return any new problem
}, func(err error) error{
	// Handle the service circuit breaker being tripped when the command was attempted
	// Return nil if handled, else return any new problem
}, func(err error) error{
	// Handle the command circuit breaker being tripped when the command was attempted
	// Return nil if handled, else return any new problem
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
}, nil, nil, nil, nil, nil)
*/

package hystrimp

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
)

const (
	RetryStrategyImmediate          RetryWaitStrategy = iota // Retry the command immediately
	RetryStrategyFixedDelay                                  // Retry the command after a fixed delay
	RetryStrategyExponentialBackoff                          // Retry the command after a backoff that gets exponentially-larger

	LevelDebug LogLevel = iota
	LevelInfo
	LevelWarn
	LevelError
	LevelFatal
	LevelPanic

	exponentialBackoffFactor = 2
)

var (
	// An error "handler" that warns about errors but does not handle them
	WarnHandler = func(err error) error {
		log.Warnln(err)
		return err
	}

	// A set of warn handlers
	WarnHandlers = &ErrorHandlers{
		Local:     WarnHandler,
		Timeout:   WarnHandler,
		Remote:    WarnHandler,
		ServiceCB: WarnHandler,
		CommandCB: WarnHandler,
	}

	log = logrus.New()
)

// Runs when file is first loaded
func init() {
	// Default log level to warn
	SetLogLevel(LevelWarn)
}

// Verbosity of logging
type LogLevel uint8

// Method of waiting between retrying commands after remote failures
type RetryWaitStrategy uint8

// A remote service with commands that can be run
type Service interface {
	// Register a command so that it can be run
	RegisterCommand(config *CommandConfiguration)

	// Runs the given command.
	// If error handlers are provided, invokes the appropriate error handler.
	// Returns any unhandled error (one of the error types below), else nil
	Run(name string, command Command, handlers *ErrorHandlers) error

	// Asynchronously run the given command on the given remote service.
	// If error handlers are provided, invoke the appropriate error handler when errors are encountered.
	// Returns any unhandled error (one of the error types below), else nil
	RunAsync(name string, command Command, handlers *ErrorHandlers) chan<- error
}

// Options with which to register/execute remote commands
type CommandConfiguration struct {
	CommonConfiguration

	// How long to wait for the command to run before raising a TimeoutError
	Timeout time.Duration

	// Maximum number of retries when unhandled errors are encountered
	Retries int

	// How to wait between retries
	RetryStrategy RetryWaitStrategy

	// Amount of time for the first retry wait (applies to fixed and backoff strategies)
	RetryInitialWait time.Duration

	// Maximum amount of time between retries
	RetryBackoffCeiling time.Duration
}

// Options to configure a service
type ServiceConfiguration struct {
	CommonConfiguration

	// Commands to register at the time of service construction
	CommandPreregistrations []*CommandConfiguration
}

// Common configuration options that apply to both commands and services
type CommonConfiguration struct {
	// Name of the service
	Name string

	// Maximum number of parallel instances of any commandsthat may run at once
	MaxConcurrentCommands int

	// How long to wait after a circuit opens before testing for recovery
	CBSleepWindow time.Duration

	// The minimum number of requests needed before a circuit can be tripped due to health
	CBRequestVolumeThreshold int

	// Causes circuit to open once the rolling measure of remote errors exceeds this percent of requests
	CBErrorPercentThreshold int
}

// Creates a new CommonConfiguration with unlimited concurrency, 50% error percent threshold for circuit breaker
func NewCommonConfiguration(name string) *CommonConfiguration {
	return &CommonConfiguration{
		Name: name,
		MaxConcurrentCommands:    math.MaxInt32,
		CBSleepWindow:            5 * time.Second,
		CBRequestVolumeThreshold: 5,
		CBErrorPercentThreshold:  50,
	}
}

// Creates a new CommandConfiguration with unlimited concurrency, 50% error percent threshold for circuit breaker, 10min timeout and no retries.
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

// A command to a remote service.
// Errors that are caused locally are returned as localError.
// Errors that are caused remotely are returned through remoteError
type Command func() (localError, remoteError error)

// A handler function to attempt to resolve an error
// Returns nil if the handler was able to handle the error
type ErrorHandler func(err error) error

// A set of error handlers to be used in running a command
type ErrorHandlers struct {
	Local     ErrorHandler // Handles errors that occur locally within the command
	Timeout   ErrorHandler // Handles the command timing out
	Remote    ErrorHandler // Handles errors for which the remote service is responsible
	CommandCB ErrorHandler // Handles the command circuit breaker being open
	ServiceCB ErrorHandler // Handles the service circuit breaker being open
}

// An error arising due to a remote request timeout
type TimeoutError struct {
	Elapsed time.Duration // Amount of time after which a timeout was declared
}

func (err *TimeoutError) Error() string {
	return fmt.Sprintf("Timeout after %v", err.Elapsed)
}

// An error arising because the service's circuit breaker is currently open
type ServiceCircuitBreakerOpenError struct {
	Name string
}

func (err *ServiceCircuitBreakerOpenError) Error() string {
	return fmt.Sprintf("Service %s circuit breaker open", err.Name)
}

// An error arising because the command's circuit breaker is currently open
type CommandCircuitBreakerOpenError struct {
	Name string
}

func (err *CommandCircuitBreakerOpenError) Error() string {
	return fmt.Sprintf("Command %s circuit breaker open", err.Name)
}

// An error that occurred locally (was *not* the remote's fault)
type LocalError struct {
	Wrapped error // Wrapped error
}

func (err *LocalError) Error() string {
	return err.Wrapped.Error()
}

// An error that occurred remotely (*was* the remote's fault)
type RemoteError struct {
	Wrapped error // Wrapped error
}

func (err *RemoteError) Error() string {
	return err.Wrapped.Error()
}

// An error that occurred inside an error handler while it was trying to handle the input error
type HandlerError struct {
	Input   error // Error that the handler tried to handle
	Wrapped error // Wrapped error
}

func (err *HandlerError) Error() string {
	return err.Wrapped.Error()
}

// Constructs a new Service with the provided configuration
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

// Configure options for the given command.
// Required before the command is used.
// Commands can also be registered inside the ServiceConfiguration
func (h *service) RegisterCommand(config *CommandConfiguration) {
	if _, alreadyConfigured := h.configs[config.Name]; alreadyConfigured {
		log.WithField("command name", config.Name).Warningln("Already had configuration for command. Reconfiguring as requested.")
	}

	h.configs[config.Name] = newCommandConfiguration(config)
}

// Run the given command on the given remote service.
// If error handlers are provided, invokes the appropriate error handler.
// Returns any unhandled error as one of the defined error types
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

// Asynchronously run the given command on the given remote service.
// If error handlers are provided, invoke the appropriate error handler when errors are encountered.
// Returns a channel on which any unhandled error (as one of the defined error types) is sent, else nil
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

// Set verbosity of logging to the given level
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

	if err := handler(toHandle); err != nil {
		log.WithField("error", err).Debugln("Handler returned error.")
		return &HandlerError{Input: toHandle, Wrapped: err}
	}

	log.Debugln("Error successfully handled")
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
