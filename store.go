package amqpStore

import (
	"net"
	"time"

	"github.com/streadway/amqp"
)

type (
	PubSub interface {
		Publish(publishConfig *PublishConfig, message *amqp.Publishing) error
		Subscribe(consumeConfig *ConsumeConfig, handler func(amqp.Delivery)) error
	}

	Store interface {
		PubSub
		DSN() string
		IsRunning() bool
		Start() error
		Shutdown()
	}

	store struct {
		isRunning      bool
		dsn            string
		connConfig     amqp.Config
		publishConn    *amqp.Connection
		consumeConn    *amqp.Connection
		publishChannel *amqp.Channel
		consumeChannel *amqp.Channel
	}
)

func New(dsn string, timeout time.Duration) Store {
	return &store{
		isRunning: false,
		dsn:       dsn,
		connConfig: amqp.Config{
			Heartbeat: 10 * time.Second,
			Locale:    "en_US",
			Dial: func(network, addr string) (net.Conn, error) {
				conn, err := net.DialTimeout(network, addr, timeout)
				if err != nil {
					return nil, err
				} else if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
					return nil, err
				}
				return conn, nil
			},
		},
	}
}

func (s *store) DSN() string {
	return s.dsn
}

func (s *store) IsRunning() bool {
	return s.isRunning
}

func (s *store) Publish(publishConfig *PublishConfig, message *amqp.Publishing) error {
	if !s.IsRunning() {
		return ErrStoreIsNotRunning
	}
	if err := s.publishChannel.ExchangeDeclare(
		publishConfig.Exchange.Name,
		publishConfig.Exchange.Type,
		publishConfig.Exchange.Durable,
		publishConfig.Exchange.AutoDelete,
		publishConfig.Exchange.Internal,
		publishConfig.Exchange.NoWait,
		publishConfig.Exchange.Args,
	); err != nil {
		return err
	}

	err := s.publishChannel.Publish( // Publishes a message onto the queue.
		publishConfig.Exchange.Name,
		publishConfig.Exchange.RoutingKey,
		publishConfig.Mandatory,
		publishConfig.Immediate,
		*message,
	)
	return err
}

func (s *store) Subscribe(consumeConfig *ConsumeConfig, handler func(amqp.Delivery)) error {
	if !s.IsRunning() {
		return ErrStoreIsNotRunning
	}
	if err := s.consumeChannel.ExchangeDeclare(
		consumeConfig.Exchange.Name,
		consumeConfig.Exchange.Type,
		consumeConfig.Exchange.Durable,
		consumeConfig.Exchange.AutoDelete,
		consumeConfig.Exchange.Internal,
		consumeConfig.Exchange.NoWait,
		consumeConfig.Exchange.Args,
	); err != nil {
		return err
	}

	queue, err := s.consumeChannel.QueueDeclare(
		consumeConfig.Exchange.Queue.Name,
		consumeConfig.Exchange.Queue.Durable,
		consumeConfig.Exchange.Queue.AutoDelete,
		consumeConfig.Exchange.Queue.Exclusive,
		consumeConfig.Exchange.Queue.NoWait,
		consumeConfig.Exchange.Queue.Args,
	)
	if err != nil {
		return err
	}

	if err = s.consumeChannel.QueueBind(
		consumeConfig.Exchange.Queue.Name,
		consumeConfig.Exchange.RoutingKey,
		consumeConfig.Exchange.Name,
		consumeConfig.Exchange.Queue.NoWait,
		consumeConfig.Exchange.Queue.Args,
	); err != nil {
		return err
	}

	messages, err := s.consumeChannel.Consume(
		queue.Name,
		consumeConfig.Name,
		consumeConfig.AutoAck,
		consumeConfig.Exclusive,
		consumeConfig.NoLocal,
		consumeConfig.NoWait,
		consumeConfig.Args,
	)
	if err != nil {
		return err
	}

	go consumeLoop(messages, handler)
	return nil
}

func (s *store) Start() error {
	var err error
	if s.publishConn, err = amqp.DialConfig(s.dsn, s.connConfig); err != nil {
		return err
	}
	if s.publishChannel, err = s.publishConn.Channel(); err != nil {
		return err
	}

	if s.consumeConn, err = amqp.DialConfig(s.dsn, s.connConfig); err != nil {
		return err
	}
	if s.consumeChannel, err = s.consumeConn.Channel(); err != nil {
		return err
	}
	s.isRunning = true
	return nil
}

func (s *store) Shutdown() {
	if s.publishConn != nil && !s.publishConn.IsClosed() {
		_ = s.publishConn.Close()
	}
	if s.consumeConn != nil && !s.publishConn.IsClosed() {
		_ = s.publishConn.Close()
	}
	s.isRunning = false
}

func consumeLoop(deliveries <-chan amqp.Delivery, handlerFunc func(d amqp.Delivery)) {
	for d := range deliveries {
		handlerFunc(d)
	}
}
