package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

func gen(ctx context.Context, timeOut time.Duration, in chan<- int) {
	for {
		v := rand.Intn(100)
		select {
		case in <- v:
			time.Sleep(timeOut * time.Millisecond)
		case <-ctx.Done():
			return
		}
	}
}

func funIn(ctx context.Context, in ...<-chan int) (out chan int) {
	out = make(chan int)
	wg := sync.WaitGroup{}

	for _, ch := range in {
		wg.Add(1)
		go func(<-chan int) {
			defer wg.Done()
			for v := range ch {
				select {
				case out <- v:
				case <-ctx.Done():
					return
				}
			}
		}(ch)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func main() {

	ch1 := make(chan int)
	ch2 := make(chan int)
	ch3 := make(chan int)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	go gen(ctx, time.Duration(100), ch1)
	go gen(ctx, time.Duration(200), ch2)
	go gen(ctx, time.Duration(300), ch3)

	fmt.Println(<-funIn(ctx, ch1, ch2, ch3))
}
