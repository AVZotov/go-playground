package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type workerManager struct {
	mu         sync.Mutex
	wg         *sync.WaitGroup
	minWorkers int
	maxWorkers int
	wCount     *int64
	cancels    []context.CancelFunc
}

func newWorkerManager(minWorkers, maxWorkers int) *workerManager {
	return &workerManager{
		minWorkers: minWorkers,
		maxWorkers: maxWorkers,
		wCount:     new(int64),
		cancels:    make([]context.CancelFunc, 0),
		wg:         &sync.WaitGroup{},
	}
}

func (wm *workerManager) addWorkers(ctx context.Context, ch chan string, amount int) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	if len(wm.cancels) == 0 {
		for i := 0; i < wm.minWorkers; i++ {
			wm.wg.Add(1)
			go worker(ctx, ch, wm.wCount, wm.wg)
		}
	}
	for i := 0; i < amount; i++ {
		chCtx, cancel := context.WithCancel(ctx)
		wm.wg.Add(1)
		go worker(chCtx, ch, wm.wCount, wm.wg)
		wm.cancels = append(wm.cancels, cancel)
	}
}

func (wm *workerManager) cancelWorkers(amount int) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	if len(wm.cancels) < amount {
		return
	}
	for i := 0; i < amount; i++ {
		wm.cancels[i]()
	}
	wm.cancels = wm.cancels[amount:]
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req := reqGenerator(ctx)
	filtered := rateLimiter(ctx, req)
	res := masterWorker(ctx, filtered)
	out := semaphore(ctx, res)
	for v := range out {
		fmt.Println(v)
	}
}

func semaphore(ctx context.Context, ch <-chan string) chan string {
	wg := &sync.WaitGroup{}
	out := make(chan string)
	sem := make(chan struct{}, 5)

	go func() {
		defer func() {
			wg.Wait()
			close(out)
		}()
		for v := range ch { // читаем пока ch открыт
			wg.Add(1)
			sem <- struct{}{} // занимаем место (ждём если 5 заняты)
			go func(val string) {

				defer func() {
					<-sem
					wg.Done()
				}() // освобождаем место
				select {
				case out <- val:
					time.Sleep(1 * time.Second)
				case <-ctx.Done():
				}
			}(v)
		}
	}()

	return out
}

func masterWorker(ctx context.Context, filtered <-chan int) chan string {
	const (
		MinWorkers = 5
		MaxWorkers = 50
	)
	ch := make(chan string, MaxWorkers)

	wm := newWorkerManager(MinWorkers, MaxWorkers)
	wm.addWorkers(ctx, ch, 0)

	go func() {
		wm.wg.Wait()
		close(ch)
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				fmt.Println("master worker done")
				return
			case req := <-filtered:
				target := req
				if target < wm.minWorkers {
					continue
				}
				if target <= wm.maxWorkers && target > wm.minWorkers {
					target = target - wm.minWorkers
				}

				if target > wm.maxWorkers {
					target = wm.maxWorkers - wm.minWorkers
				}

				// шаг 2 — приводим к target
				current := len(wm.cancels)
				if target > current {
					go wm.addWorkers(ctx, ch, target-current)
				}
				if target < current {
					wm.cancelWorkers(current - target)
				}
			}
		}
	}()
	return ch
}

func worker(done context.Context, ch chan<- string, index *int64, wg *sync.WaitGroup) {
	defer wg.Done()
	i := atomic.AddInt64(index, 1)

	for {
		select {
		case <-done.Done():
			atomic.AddInt64(index, -1)
			return
		case ch <- fmt.Sprintf("hi from worker %d", i):
			time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
		}
	}
}

func rateLimiter(done context.Context, reqCh <-chan int) chan int {
	ch := make(chan int)
	timer := time.NewTicker(500 * time.Millisecond)
	go func() {
		for {
			select {
			case <-done.Done():
				timer.Stop()
				close(ch)
				return
			case v, ok := <-reqCh:
				if !ok {
					timer.Stop()
					close(ch)
					return
				}
				select {
				case <-timer.C:
					ch <- v
				}
			}
		}
	}()
	return ch
}

// Generate random values
func reqGenerator(done context.Context) chan int {
	reqCh := make(chan int)

	go func() {
		for {
			select {
			case <-done.Done():
				fmt.Println("req generator down")
				close(reqCh)
				return
			default:
				reqCh <- rand.Intn(100)
			}
		}
	}()
	return reqCh
}
