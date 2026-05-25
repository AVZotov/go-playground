package main

import "fmt"

func producer(jobs chan int) {
	go func() {
		for i := 1; i < 11; i++ {
			jobs <- i
		}
		close(jobs)
	}()
}

func worker(jobs chan int, results chan int) {
	go func() {
		for v := range jobs {
			results <- v * v
		}
	}()
}

func main() {
	jobs := make(chan int, 10)
	results := make(chan int, 10)

	producer(jobs)
	for i := 0; i < 3; i++ {
		worker(jobs, results)
	}

	for i := 0; i < 11; i++ {
		fmt.Println(<-results)
	}
}
