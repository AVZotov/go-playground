package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

func server(req chan int, resp chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	go func() {
		for r := range req {
			sec := rand.Intn(3) + 1
			time.Sleep(time.Duration(sec) * time.Second)
			resp <- fmt.Sprintf("%d Response with delay:%d", r, sec)
		}
		close(resp)
	}()
}

func ping(req chan int, count int, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < count; i++ {
		req <- i
	}
	close(req)
}

func worker(resp chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	go func() {
		var str string
		for {
			select {
			case str = <-resp:
				fmt.Println(str)
			case <-time.After(time.Second * 2):
				fmt.Println("timeout")
			case _, ok := <-resp:
				if !ok {
					return
				}
			default:
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
	ping(req, 100, &wg)
	worker(resp, &wg)

	wg.Wait()
	fmt.Println("done")
}
