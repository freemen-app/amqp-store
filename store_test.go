package amqpStore_test

import (
	"errors"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"

	amqpStore "github.com/freemen-app/amqp-store"
)

var (
	conf *amqpStore.Config
)

func TestMain(m *testing.M) {
	conf = &amqpStore.Config{
		Host:     os.Getenv("AMQP_HOST"),
		Port:     os.Getenv("AMQP_PORT"),
		Username: os.Getenv("AMQP_USERNAME"),
		Password: os.Getenv("AMQP_PASSWORD"),
	}
	os.Exit(m.Run())
}

func TestNew(t *testing.T) {
	config := &amqpStore.Config{
		Host:     "localhost",
		Port:     "5672",
		Username: "test",
		Password: "test",
	}
	store := amqpStore.New(config.DSN(), time.Second)
	assert.False(t, store.IsRunning())
	assert.EqualValues(t, config.DSN(), store.DSN())
}

func TestStore_Start(t *testing.T) {
	type args struct {
		conf    *amqpStore.Config
		timeout time.Duration
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{
			name: "succeed",
			args: args{
				conf:    conf,
				timeout: time.Second,
			},
		},
		{
			name: "invalid credentials",
			args: args{
				conf: &amqpStore.Config{
					Host:     "localhost",
					Port:     "5672",
					Username: "test",
					Password: "test",
				},
				timeout: time.Second,
			},
			wantErr: amqp.ErrCredentials,
		},
		{
			name: "invalid host/port",
			args: args{
				conf: &amqpStore.Config{
					Host:     "localhost",
					Port:     "5673",
					Username: "test",
					Password: "test",
				},
				timeout: time.Second,
			},
			wantErr: syscall.ECONNREFUSED,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := amqpStore.New(tt.args.conf.DSN(), tt.args.timeout)
			gotErr := store.Start()
			assert.True(t, errors.Is(gotErr, tt.wantErr), gotErr)
			t.Cleanup(store.Shutdown)
		})
	}

	t.Run("timeout error", func(t *testing.T) {
		store := amqpStore.New(conf.DSN(), 0)
		err := store.Start()
		opErr := &net.OpError{}
		assert.True(t, errors.As(err, &opErr))
		assert.True(t, opErr.Timeout())
		t.Cleanup(store.Shutdown)
	})
}

func TestStore_Shutdown(t *testing.T) {
	store := amqpStore.New(conf.DSN(), time.Second)

	err := store.Start()
	assert.NoError(t, err)
	assert.True(t, store.IsRunning())

	store.Shutdown()
	assert.False(t, store.IsRunning())
}

func TestStore_Publish_Subscribe(t *testing.T) {
	store := amqpStore.New(conf.DSN(), time.Second)

	err := store.Start()
	assert.NoError(t, err)
	t.Cleanup(store.Shutdown)

	testMsg := []byte("test message")
	exchange := amqpStore.ExchangeConfig{
		Name:       "test_exchange",
		Type:       amqp.ExchangeDirect,
		RoutingKey: "test_queue",
		AutoDelete: true,
		Queue:      amqpStore.QueueConfig{Name: "test_queue", AutoDelete: true},
	}
	publishConf := &amqpStore.PublishConfig{Exchange: exchange}
	consumeConf := &amqpStore.ConsumeConfig{Exchange: exchange}

	t.Run("succeed", func(t *testing.T) {
		var gotMsg bool
		err = store.Subscribe(consumeConf, func(delivery amqp.Delivery) {
			assert.EqualValues(t, testMsg, delivery.Body)
			gotMsg = true
		})

		err = store.Publish(publishConf, &amqp.Publishing{Body: testMsg})
		assert.NoError(t, err, err)

		// Wait until msg is delivered
		time.Sleep(time.Second / 4)
		assert.True(t, gotMsg)
		assert.NoError(t, err, err)
	})

	t.Run("connection closed", func(t *testing.T) {
		store.Shutdown()
		err = store.Publish(publishConf, &amqp.Publishing{Body: testMsg})
		assert.True(t, errors.Is(err, amqpStore.ErrStoreIsNotRunning), err)

		err = store.Subscribe(consumeConf, func(delivery amqp.Delivery) {
			assert.EqualValues(t, testMsg, delivery.Body)
		})
		assert.True(t, errors.Is(err, amqpStore.ErrStoreIsNotRunning), err)
	})
}
