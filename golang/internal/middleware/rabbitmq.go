package middleware

import (
	"context"
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type rabbitMiddleware struct {
	conn *amqp.Connection
	ch   *amqp.Channel

	exchange string
	keys     []string

	queueName string
}

func (r *rabbitMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	q, err := r.ch.QueueDeclare(
		"",    // name
		false, // durability
		true,  // delete when unused -> cuadno se me desconectan los consumidores, se borra la queue
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return mapError(err)
	}

	r.queueName = q.Name

	for _, key := range r.keys {
		err = r.ch.QueueBind(
			q.Name,     // queue name
			key,        // routing key
			r.exchange, // exchange
			false,
			nil,
		)
		if err != nil {
			return mapError(err)
		}
	}

	msgs, err := r.ch.Consume(
		q.Name, //queue
		"",     // consumer
		false,  // auto ack -> lo hago manualmente
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)
	if err != nil {
		return mapError(err)
	}

	go func() {
		for d := range msgs {
			callbackFunc(
				Message{Body: string(d.Body)},
				func() { _ = d.Ack(false) },
				func() { _ = d.Nack(false, true) },
			)
		}
	}()

	return nil
}

func (r *rabbitMiddleware) StopConsuming() {
}

func (r *rabbitMiddleware) Send(msg Message) error {
	if r.ch == nil {
		return ErrMessageMiddlewareDisconnected
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := r.ch.PublishWithContext(
		ctx,
		r.exchange, // exchange
		r.keys[0],  // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			Body: []byte(msg.Body),
		},
	)
	if err != nil {
		return mapError(err)
	}

	return nil
}

func (r *rabbitMiddleware) Close() error {
	if r.ch != nil {
		if err := r.ch.Close(); err != nil {
			return ErrMessageMiddlewareClose
		}
	}
	if r.conn != nil {
		if err := r.conn.Close(); err != nil {
			return ErrMessageMiddlewareClose
		}
	}
	return nil
}

func NewRabbitMiddleware(exchange string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	conn, err := amqp.Dial(fmt.Sprintf(
		"amqp://guest:guest@%s:%d/",
		connectionSettings.Hostname,
		connectionSettings.Port,
	))
	if err != nil {
		return nil, ErrMessageMiddlewareDisconnected
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, ErrMessageMiddlewareDisconnected
	}

	err = ch.ExchangeDeclare(
		exchange, // name
		"direct", // type
		true,     // durability -> si se reinicia rabbit, el exchange sigue existiendo
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		return nil, ErrMessageMiddlewareMessage
	}

	return &rabbitMiddleware{
		conn:     conn,
		ch:       ch,
		exchange: exchange,
		keys:     keys,
	}, nil
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, amqp.ErrClosed) {
		return ErrMessageMiddlewareDisconnected
	}
	return ErrMessageMiddlewareMessage
}
