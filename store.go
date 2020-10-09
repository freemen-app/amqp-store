package amqpStore

import (
	"io"
	"log"
	"net"
	"time"

	"github.com/streadway/amqp"
)

type (
	Store interface {
		Publish(publishConfig *PublishConfig, message *amqp.Publishing) error
		Subscribe(consumeConfig *ConsumeConfig, handler func(amqp.Delivery)) error
		Shutdown()
	}

	store struct {
		publishConn    *amqp.Connection
		consumeConn    *amqp.Connection
		publishChannel *amqp.Channel
		consumeChannel *amqp.Channel
		cleanupTasks   []io.Closer
	}
)

func New(dsn string, timeout time.Duration) *store {
	var err error
	s := new(store)
	defer s.shutdownOnPanic()

	connConfig := amqp.Config{
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
	}

	s.publishConn, err = amqp.DialConfig(dsn, connConfig)
	s.panicErr(err)
	s.addCleanupTask(s.publishConn)

	s.consumeConn, err = amqp.DialConfig(dsn, connConfig)
	s.panicErr(err)
	s.addCleanupTask(s.consumeConn)

	s.publishChannel, err = s.publishConn.Channel()
	s.panicErr(err)
	s.addCleanupTask(s.publishChannel)

	s.consumeChannel, err = s.consumeConn.Channel()
	s.panicErr(err)
	s.addCleanupTask(s.consumeChannel)

	return s
}

func (s *store) Publish(publishConfig *PublishConfig,
	message *amqp.Publishing) error {
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

func (s *store) Shutdown() {
	lastIndex := len(s.cleanupTasks) - 1

	for i := range s.cleanupTasks {
		if err := s.cleanupTasks[lastIndex-i].Close(); err != nil {
			log.Printf("error while closing rabbitmq object: %v", err)
		}
	}
}

func (s *store) addCleanupTask(task io.Closer) {
	s.cleanupTasks = append(s.cleanupTasks, task)
}

func (s *store) panicErr(err error) {
	if err != nil {
		panic(err)
	}
}

func consumeLoop(deliveries <-chan amqp.Delivery, handlerFunc func(d amqp.Delivery)) {
	for d := range deliveries {
		handlerFunc(d)
	}
}

func (s *store) shutdownOnPanic() {
	if r := recover(); r != nil {
		s.Shutdown()
		panic(r)
	}
}
