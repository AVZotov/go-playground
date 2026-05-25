package main

import (
	"fmt"
	"sync"
)

func producer(jobs chan int) {
	go func() {
		for i := 1; i < 11; i++ {
			jobs <- i
		}
		close(jobs)
	}()
}

func worker(jobs chan int, results chan int, wg *sync.WaitGroup) {
	go func() {
		defer wg.Done()
		for v := range jobs {
			results <- v * v
		}
	}()
}

func main() {
	jobs := make(chan int, 10)
	results := make(chan int, 10)
	wg := new(sync.WaitGroup)

	producer(jobs)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		worker(jobs, results, wg)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	for v := range results {
		fmt.Println(v)
	}
}
