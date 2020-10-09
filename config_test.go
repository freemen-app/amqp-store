package amqpStore_test

import (
	"testing"

	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"

	amqpStore "github.com/freemen-app/amqp-store"
)

func TestAMQPConfig_Validate(t *testing.T) {
	type fields struct {
		host      string
		port      string
		username  string
		password  string
		publishes map[string]*amqpStore.PublishConfig
		consumes  map[string]*amqpStore.ConsumeConfig
	}
	tests := []struct {
		name       string
		fields     fields
		wantErrKey string
	}{
		{
			name: "valid",
			fields: fields{
				host:     "localhost",
				port:     "8000",
				username: "test",
				password: "test",
			},
		},
		{
			name: "required username",
			fields: fields{
				host:     "localhost",
				port:     "8000",
				password: "test",
			},
			wantErrKey: "Username",
		},
		{
			name: "required password",
			fields: fields{
				host:     "localhost",
				port:     "8000",
				username: "test",
			},
			wantErrKey: "Password",
		},
		{
			name: "invalid host",
			fields: fields{
				host:     "test@gmail.com",
				port:     "8000",
				username: "test",
				password: "test",
			},
			wantErrKey: "Host",
		},
		{
			name: "invalid host",
			fields: fields{
				host:     "@@1234",
				port:     "8000",
				username: "test",
				password: "test",
			},
			wantErrKey: "Host",
		},
		{
			name: "invalid port",
			fields: fields{
				host:     "localhost",
				port:     "999999999",
				username: "test",
				password: "test",
			},
			wantErrKey: "Port",
		},
		{
			name: "invalid port",
			fields: fields{
				host:     "localhost",
				port:     "test",
				username: "test",
				password: "test",
			},
			wantErrKey: "Port",
		},
		{
			name: "invalid exchange type",
			fields: fields{
				host:     "localhost",
				port:     "8000",
				username: "test",
				password: "test",
				publishes: map[string]*amqpStore.PublishConfig{
					"test_publish": {
						Exchange: amqpStore.ExchangeConfig{Type: "invalid"},
					},
				},
			},
			wantErrKey: "test_publish",
		},
		{
			name: "invalid exchange type",
			fields: fields{
				host:     "localhost",
				port:     "8000",
				username: "test",
				password: "test",
				consumes: map[string]*amqpStore.ConsumeConfig{
					"test_consume": {
						Exchange: amqpStore.ExchangeConfig{Type: "invalid"},
					},
				},
			},
			wantErrKey: "test_consume",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := amqpStore.Config{
				Host:      tt.fields.host,
				Port:      tt.fields.port,
				Username:  tt.fields.username,
				Password:  tt.fields.password,
				Consumes:  tt.fields.consumes,
				Publishes: tt.fields.publishes,
			}
			err := c.Validate()
			if tt.wantErrKey == "" {
				assert.Nil(t, err, err)
			} else {
				assert.NotNil(t, err, tt.wantErrKey)
				assert.IsType(t, validation.Errors{}, err)
				validationErr := err.(validation.Errors)
				_, ok := validationErr[tt.wantErrKey]
				assert.True(t, ok, validationErr.Error(), tt.wantErrKey)
			}
		})
	}
}

func TestExchangeConfig_Validate(t *testing.T) {
	type fields struct {
		Type string
		Args amqp.Table
	}
	tests := []struct {
		name       string
		fields     fields
		wantErrKey string
	}{
		{
			name:   "valid",
			fields: fields{Type: amqp.ExchangeFanout},
		},
		{
			name:   "valid",
			fields: fields{Type: amqp.ExchangeTopic},
		},
		{
			name:   "valid",
			fields: fields{Type: amqp.ExchangeDirect},
		},
		{
			name:       "invalid type",
			fields:     fields{Type: "test"},
			wantErrKey: "type",
		},
		{
			name: "valid",
			fields: fields{
				Type: amqpStore.ExchangeDelayed,
				Args: amqp.Table{"x-delayed-type": amqp.ExchangeFanout},
			},
		},
		{
			name:       "missing header x-delayed-type",
			fields:     fields{Type: amqpStore.ExchangeDelayed},
			wantErrKey: "args",
		},
		{
			name: "invalid x-delayed-type",
			fields: fields{
				Type: amqpStore.ExchangeDelayed,
				Args: amqp.Table{"x-delayed-type": "test"},
			},
			wantErrKey: "args",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := amqpStore.ExchangeConfig{Type: tt.fields.Type, Args: tt.fields.Args}
			gotErr := e.Validate()
			if tt.wantErrKey == "" {
				assert.Nil(t, gotErr, gotErr)
			} else {
				assert.NotNil(t, gotErr, tt.wantErrKey)
				assert.IsType(t, validation.Errors{}, gotErr)
				validationErr := gotErr.(validation.Errors)
				_, ok := validationErr[tt.wantErrKey]
				assert.True(t, ok, validationErr.Error(), tt.wantErrKey)
			}
		})
	}
}

func TestConfig_DSN(t *testing.T) {
	conf := amqpStore.Config{
		Host:     "localhost",
		Port:     "8000",
		Username: "test",
		Password: "test1234",
	}
	got := conf.DSN()
	want := "amqp://test:test1234@localhost:8000/"
	assert.EqualValues(t, got, want)
}
