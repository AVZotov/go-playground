package main

import (
	"fmt"
	"sync"
)

func ping(ch1 chan int, ch2 chan int, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < 5; i++ {
		v, ok := <-ch1
		fmt.Printf("ping received: %d ok:%t\n", v, ok)
		if !ok {
			return
		}
		v++
		ch2 <- v
		fmt.Printf("ping sent: %d ok:%t\n", v, ok)
	}
	close(ch2)
}

func pong(ch2 chan int, ch1 chan int, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		v, ok := <-ch2
		fmt.Printf("pong received: %d ok:%t\n", v, ok)
		if !ok {
			close(ch1)
			return
		}
		v++
		ch1 <- v
		fmt.Printf("pong sent: %d ok:%t\n", v, ok)
	}
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
