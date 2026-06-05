package main

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type EventBus struct {
	clients map[string][]*HandlerWrapper
}

func NewEventBus() *EventBus {
	return &EventBus{
		clients: make(map[string][]*HandlerWrapper),
	}
}

func (eb *EventBus) Publish(topic string, data any) {
	wg := sync.WaitGroup{}
	for k := range eb.clients {
		if matchTopic(k, topic) {
			handlers := eb.clients[k]
			remaining := make([]*HandlerWrapper, 0, len(handlers))
			for _, hw := range eb.clients[k] {
				wg.Add(1)
				go func() {
					defer wg.Done()
					hw.Handler(data)
				}()
				if !hw.isOnce {
					remaining = append(remaining, hw)
				}
			}
			eb.clients[k] = remaining
		}
	}
	wg.Wait()
}

func (eb *EventBus) Subscribe(topic string, hw *HandlerWrapper) {
	eb.clients[topic] = append(eb.clients[topic], hw)
}

func (eb *EventBus) SubscribeOnce(topic string, hw *HandlerWrapper) {
	hw.isOnce = true
	eb.Subscribe(topic, hw)
}

func (eb *EventBus) Unsubscribe(topic string, hw *HandlerWrapper) {
	handlers := eb.clients[topic]
	updated := make([]*HandlerWrapper, 0, len(handlers))
	for _, h := range handlers {
		if h.id != hw.id {
			updated = append(updated, h)
		}
	}
	eb.clients[topic] = updated
}

type HandlerWrapper struct {
	id      string
	Handler func(data any)
	isOnce  bool
}

func NewHandlerWrapper(f func(data any)) *HandlerWrapper {
	return &HandlerWrapper{
		id:      uuid.New().String(),
		Handler: f,
	}
}

func matchTopic(pattern, topic string) bool {
	if pattern == "*" {
		return true
	}

	pSlice := strings.Split(pattern, ".")
	tSlice := strings.Split(topic, ".")
	if len(pSlice) != len(tSlice) {
		return false
	}
	for i, p := range pSlice {
		if p != tSlice[i] && p != "*" {
			return false
		}
	}
	return true
}

func main() {
	bus := NewEventBus()

	h1 := NewHandlerWrapper(
		func(data any) {
			time.Sleep(100 * time.Millisecond)
			fmt.Println("h1 got:", data)
		},
	)
	h2 := NewHandlerWrapper(
		func(data any) {
			time.Sleep(50 * time.Millisecond)
			fmt.Println("h2 got:", data)
		},
	)

	bus.Subscribe("user.created", h1)
	bus.Subscribe("user.created", h2)

	start := time.Now()
	bus.Publish("user.created", "alice")
	fmt.Printf("заняло %v (должно ~100ms, не 150ms)\n", time.Since(start).Round(time.Millisecond))
}
