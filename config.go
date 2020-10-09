package amqpStore

import (
	"errors"
	"fmt"

	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/go-ozzo/ozzo-validation/v4/is"
	"github.com/streadway/amqp"
)

type (
	Config struct {
		Host      string
		Port      string
		Username  string
		Password  string
		Consumes  map[string]*ConsumeConfig
		Publishes map[string]*PublishConfig
	}

	ConsumeConfig struct {
		Name      string
		AutoAck   bool `config:"auto_ack"`
		Exclusive bool
		NoLocal   bool `config:"no_local"`
		NoWait    bool `config:"no_wait"`
		Args      amqp.Table
		Exchange  exchangeConfig
	}

	PublishConfig struct {
		Mandatory bool
		Immediate bool
		Exchange  exchangeConfig
	}

	exchangeConfig struct {
		Name       string      `json:"name"`
		Type       string      `json:"type"`
		Durable    bool        `json:"durable"`
		AutoDelete bool        `config:"auto_delete" json:"auto_delete"`
		Internal   bool        `json:"internal"`
		NoWait     bool        `config:"no_wait" json:"no_wait"`
		RoutingKey string      `config:"routing_key" json:"routing_key"`
		Args       amqp.Table  `json:"args"`
		Queue      queueConfig `json:"queue"`
	}

	queueConfig struct {
		Name       string
		Durable    bool
		AutoDelete bool `config:"auto_delete"`
		Exclusive  bool
		Internal   bool
		NoWait     bool `config:"no_wait"`
		Args       map[string]interface{}
	}
)

var exchangeTypes = []interface{}{
	amqp.ExchangeFanout,
	amqp.ExchangeDirect,
	amqp.ExchangeTopic,
	ExchangeDelayed,
}

func (c *Config) DSN() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%s/", c.Username, c.Password, c.Host, c.Port)
}

func (c *Config) Validate() error {
	if err := validation.ValidateStruct(c,
		validation.Field(&c.Host, validation.Required, is.Host),
		validation.Field(&c.Port, validation.Required, is.Port),
		validation.Field(&c.Username, validation.Required),
		validation.Field(&c.Password, validation.Required),
	); err != nil {
		return err
	}
	if err := validation.Validate(c.Publishes); err != nil {
		return err
	} else if err := validation.Validate(c.Consumes); err != nil {
		return err
	}
	return nil
}

func (p *PublishConfig) Validate() error {
	return validation.Validate(&p.Exchange)
}

func (c *ConsumeConfig) Validate() error {
	return validation.Validate(&c.Exchange)
}

func (e exchangeConfig) Validate() error {
	if err := validation.ValidateStruct(
		&e,
		validation.Field(&e.Type, validation.In(exchangeTypes...)),
	); err != nil {
		return err
	}

	if e.Type == "x-delayed-message" {
		var errMsg string

		if val, ok := e.Args["x-delayed-type"]; !ok {
			errMsg = fmt.Sprintf(
				"exchange '%s' with type [x-delayed-message] must contain 'x-delayed-type' argument",
				e.Name,
			)
		} else if err := validation.Validate(&val, validation.In(exchangeTypes...)); err != nil {
			errMsg = fmt.Sprintf("invalid type [%s] for x-delayed-type argument", val)
		}

		if errMsg != "" {
			return validation.Errors{"args": errors.New(errMsg)}
		}
	}
	return nil
}
