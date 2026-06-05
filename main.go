package main

import (
	"fmt"

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
	for _, hw := range eb.clients[topic] {
		hw.Handler(data)
		if hw.isOnce {
			eb.Unsubscribe(topic, hw)
		}
	}
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

func main() {
	bus := NewEventBus()

	f1 := func(data any) { fmt.Println("h1 got:", data) }
	f2 := func(data any) { fmt.Println("h2 got:", data) }
	h1 := NewHandlerWrapper(f1)
	h2 := NewHandlerWrapper(f2)

	bus.SubscribeOnce("user.created", h1)
	bus.Subscribe("user.created", h2)
	bus.Publish("user.created", "Alice")
	bus.Publish("user.created", "Bob")
	bus.Publish("user.created", "Carlos")
}
