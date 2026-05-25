package main

import (
	"sync"
)

func ping(ch1 chan int, ch2 chan int, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < 20; i++ {
		v, ok := <-ch1
		if !ok {
			return
		}
		v++
		ch2 <- v
	}
	close(ch2)
}

func pong(ch2 chan int, ch1 chan int, wg *sync.WaitGroup) {
	defer wg.Done()
	v, ok := <-ch2
	if !ok {
		close(ch1)
		return
	}
	v++
	ch1 <- v
}

func main() {
	ch1 := make(chan int)
	ch2 := make(chan int)
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		ch1 <- 0
	}()

	go ping(ch1, ch2, &wg)

	go pong(ch2, ch1, &wg)

	wg.Wait()
}
