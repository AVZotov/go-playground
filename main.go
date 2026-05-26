package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

func sGen(ctx context.Context, out chan<- string) {
	strs := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	for {
		str := strs[rand.Intn(len(strs))]
		select {
		case out <- str:
			time.Sleep(500 * time.Millisecond)
		case <-ctx.Done():
			return
		}
	}
}

func iGen(ctx context.Context, out chan<- int) {
	for {
		i := rand.Intn(10)
		select {
		case out <- i:
			time.Sleep(300 * time.Millisecond)
		case <-ctx.Done():
			return
		}
	}
}

func collector(ctx context.Context, sOut <-chan string, iOut <-chan int, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case v := <-sOut:
			fmt.Println(v)
		case v := <-iOut:
			fmt.Println(v)
		case <-ctx.Done():
			fmt.Println(ctx.Err())
			return
		}
	}
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	sChan := make(chan string)
	iChan := make(chan int)
	defer cancel()
	wg := sync.WaitGroup{}
	go sGen(ctx, sChan)
	go iGen(ctx, iChan)
	wg.Add(1)
	go collector(ctx, sChan, iChan, &wg)
	wg.Wait()
}
