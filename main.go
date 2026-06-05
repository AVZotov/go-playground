package main

import (
	"fmt"
	
	"github.com/google/uuid"
)

type EventBus struct {
	clients map[string]*HandlerWrapper
}

func NewEventBus() *EventBus {
	return &EventBus{
		clients: make(map[string]*HandlerWrapper),
	}
}

func (eb *EventBus) Publish(topic string, data any) {
	for _, v := range eb.clients {
		if topic == v.topic {
			v.Handler(data)
		}
	}
}

func (eb *EventBus) Subscribe(topic string, hw *HandlerWrapper) {
	hw.topic = topic
	eb.clients[topic] = hw
}

func (eb *EventBus) Unsubscribe(topic string, hw *HandlerWrapper) {
	for k, v := range eb.clients {
		if topic == v.topic && k == hw.id {
			delete(eb.clients, k)
		}
	}
}

type HandlerWrapper struct {
	id      string
	topic   string
	Handler func(data any)
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
	
	bus.Subscribe("user.created", h1)
	bus.Subscribe("user.created", h2)
	bus.Subscribe("order.placed", h1)
	bus.Publish("user.created", "Alice")
	bus.Unsubscribe("user.created", h1)
	bus.Publish("user.created", "Bob")
	bus.Publish("order.placed", "order-123")
}
