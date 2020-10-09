package amqpStore

import "errors"

var (
	ExchangeDelayed = "x-delayed-message"
)

var (
	ErrConnectionRefused = errors.New("connection refused")
	ErrStoreIsNotRunning = errors.New("store: is not running")
)
