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
			time.Sleep(timeOut)
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
		go func() {
			wg.Wait()
			close(out)
		}()
	}
	return out
}

func main() {

	in := make(chan int)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	d := []int{100, 200, 300}
	for _, dd := range d {
		go gen(ctx, time.Duration(dd), in)
	}
	out := funIn(ctx, in)
	fmt.Println(<-out)
}
