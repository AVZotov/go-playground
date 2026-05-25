package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

func server(req chan int, resp chan string, wg *sync.WaitGroup) {
	go func() {
		defer wg.Done()
		for r := range req {
			sec := rand.Intn(3) + 1
			time.Sleep(time.Duration(sec) * time.Second)
			resp <- fmt.Sprintf("%d Response with delay:%d", r, sec)
		}
		close(resp)
	}()
}

func ping(req chan int, count int, wg *sync.WaitGroup) {
	go func() {
		defer wg.Done()
		for i := 0; i < count; i++ {
			req <- i
		}
		close(req)
	}()
}

func worker(resp chan string, wg *sync.WaitGroup) {
	go func() {
		defer wg.Done()
		for {
			select {
			case v, ok := <-resp:
				if !ok {
					return
				}
				fmt.Println(v)
			case <-time.After(time.Second * 2):
				fmt.Println("timeout")
			}
		}
	}()
}

func main() {
	req := make(chan int)
	resp := make(chan string)
	var wg sync.WaitGroup
	wg.Add(3)
	server(req, resp, &wg)
	ping(req, 1, &wg)
	worker(resp, &wg)

	wg.Wait()
	fmt.Println("done")
}
