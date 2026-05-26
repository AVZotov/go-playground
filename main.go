package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

func fetchData(ctx context.Context, url string) (string, error) {
	res := make(chan string, 1)

	go func() {
		opt := []int{1, 4}
		t := opt[rand.Intn(2)]
		time.Sleep(time.Duration(t) * time.Second)
		res <- fmt.Sprintf("time elapsed to send: %d", t)
	}()

	select {
	case v := <-res:
		return v, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func main() {
	for i := 0; i < 10; i++ {
		ctx, close := context.WithTimeout(context.Background(), 2*time.Second)
		defer close()
		v, err := fetchData(ctx, "http://www.google.com")
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(v)
	}
}
