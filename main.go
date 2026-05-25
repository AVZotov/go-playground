package main

import (
	"fmt"
	"sync"
)

func generator(gen chan<- int) {
	for i := 1; i <= 5; i++ {
		gen <- i
	}
	close(gen)
}

func filter(gen <-chan int, filtered chan<- int, filter func(int) bool) {
	for v := range gen {
		if filter(v) {
			filtered <- v
		}
	}
	close(filtered)
}

func worker(filtered <-chan int, processed chan<- int, wg *sync.WaitGroup) {
	defer wg.Done()
	for v := range filtered {
		v *= v
		processed <- v
	}
}

func collector(processed <-chan int, wg *sync.WaitGroup) {
	defer wg.Done()
	var data []int
	for v := range processed {
		data = append(data, v)
	}
	fmt.Println(data)
}

func main() {
	gen := make(chan int)
	filtered := make(chan int)
	processed := make(chan int)
	wg := new(sync.WaitGroup)
	go generator(gen)
	go filter(gen, filtered, isPrime)
	go func() {
		wg1 := new(sync.WaitGroup)
		const workers = 3
		wg1.Add(workers)
		for i := 0; i < workers; i++ {
			go worker(filtered, processed, wg)
		}
		wg1.Wait()
		close(processed)
	}()
	wg.Add(1)
	go collector(processed, wg)

	wg.Wait()
}

func isPrime(n int) bool {
	if n < 2 {
		return false
	}
	for i := 2; i*i <= n; i++ {
		if n%i == 0 {
			return false
		}
	}
	return true
}
