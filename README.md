HYSTRIx iMProved
===================
[Hystrix](https://github.com/Netflix/Hystrix) is an innovative project from Netflix that aims to make interactions with external dependencies robust. It forces one to answer important questions such as:

*What do I do if I time out before hearing from the remote system?*

*What do I do if the remote system returns an error?*

*How much load should I reasonably place on the remote system?*

*If I cannot use the remote system, should I retry my request? How long should I wait before retrying?* 

Hystrimp provides a Go implementation of these ideas:

![Hystrix Flow](flow.png)

It improves upon [hystrix-go](https://github.com/afex/hystrix-go) (similar project) in the following ways:

* Support for automatic retry of command with backoff upon remote errors or timeouts.
* Remote commands are grouped into logical services
   * Support for concurrency limits on the service as a whole
   * Support for circuit breakers on a service as a whole
* Better ergonomics/interface
* Simpler implementation

Ideas for improvement and new features are welcome! Please use GitHub's issue tracker.
