package middleware

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type rabbitMiddleware struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

func (r *rabbitMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	return nil
}

func (r *rabbitMiddleware) StopConsuming() {}

func (r *rabbitMiddleware) Send(msg Message) error {
	return nil
}

func (r *rabbitMiddleware) Close() error {
	return nil
}

func NewRabbitMiddleware(exchange string, keys []string, connectionSettings ConnSettings) Middleware {
	conn, err := amqp.Dial(fmt.Sprintf(
		"amqp://guest:guest@%s:%d/",
		connectionSettings.Hostname,
		connectionSettings.Port,
	))
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	return &rabbitMiddleware{
		conn: conn,
		ch:   ch,
	}
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}
