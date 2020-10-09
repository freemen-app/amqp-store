package amqpStore_test

import (
	"time"

	"github.com/streadway/amqp"

	amqpStore "github.com/freemen-app/amqp-store"
)

func ExampleNew() {
	conf := &amqpStore.Config{
		Host:     "localhost",
		Port:     "5672",
		Username: "test",
		Password: "test",
	}
	amqpStore.New(conf.DSN(), time.Second)
}

func ExampleStore_Publish() {
	conf := &amqpStore.Config{
		Host:     "localhost",
		Port:     "5672",
		Username: "test",
		Password: "test",
	}
	store := amqpStore.New(conf.DSN(), time.Second)

	publishConfig := &amqpStore.PublishConfig{
		Exchange: amqpStore.ExchangeConfig{
			Name: "test",
			Type: amqp.ExchangeFanout,
		},
	}
	msg := &amqp.Publishing{
		ContentType: "application/json",
		Body:        []byte("test msg"),
	}
	err := store.Publish(publishConfig, msg)
	if err != nil {
		// Handle error
	}
}

func ExampleStore_Subscribe() {
	conf := &amqpStore.Config{
		Host:     "localhost",
		Port:     "5672",
		Username: "test",
		Password: "test",
	}
	store := amqpStore.New(conf.DSN(), time.Second)

	consumeConfig := &amqpStore.ConsumeConfig{
		Exchange: amqpStore.ExchangeConfig{
			Name:       "test",
			Type:       amqp.ExchangeDirect,
			RoutingKey: "test_queue",
			Queue: amqpStore.QueueConfig{
				Name: "test_queue",
			},
		},
	}
	err := store.Subscribe(consumeConfig, func(delivery amqp.Delivery) {
		// Handle message
	})
	if err != nil {
		// Handle error
	}
}
