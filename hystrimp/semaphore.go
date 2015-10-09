package hystrimp

type semaphore chan struct{}

func newSemaphore(size int) semaphore {
	return semaphore(make(chan struct{}, size))
}

func (s semaphore) up() {
	select {
	case <-s:
	default:
		log.Warningln("Attempt to up semaphore beyond its capacity was unexpected and probably indicates a bug!")
	}
}

func (s semaphore) down() {
	s <- struct{}{}
}
