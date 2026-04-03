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
}

type exchangeMiddleware struct {
	*rabbitMiddleware

	exchange string
	keys     []string

	queueName string
}

type queueMiddleware struct {
	*rabbitMiddleware

	queueName string
}

func (r *rabbitMiddleware) consumeLoop(
	msgs <-chan amqp.Delivery,
	callbackFunc func(msg Message, ack func(), nack func()),
) {
	go func() {
		for d := range msgs {
			callbackFunc(
				Message{Body: string(d.Body)},
				func() { _ = d.Ack(false) },
				func() { _ = d.Nack(false, true) },
			)
		}
	}()
}

func (e *exchangeMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	q, err := e.ch.QueueDeclare(
		"",    // name
		false, // durability
		true,  // delete when unused -> cuando se desconectan los consumidores, se borra la queue
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return mapError(err)
	}

	e.queueName = q.Name

	for _, key := range e.keys {
		err = e.ch.QueueBind(
			q.Name,
			key,
			e.exchange,
			false,
			nil,
		)
		if err != nil {
			return mapError(err)
		}
	}

	msgs, err := e.ch.Consume(
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

	e.consumeLoop(msgs, callbackFunc)

	return nil
}

func (q *queueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	msgs, err := q.ch.Consume(
		q.queueName, //queue
		"",          // consumer
		false,       // auto ack -> lo hago manualmente
		false,       // exclusive
		false,       // no local
		false,       // no wait
		nil,         // args
	)
	if err != nil {
		return mapError(err)
	}

	q.consumeLoop(msgs, callbackFunc)

	return nil
}

func (r *rabbitMiddleware) StopConsuming() {
}

func (e *exchangeMiddleware) Send(msg Message) error {
	if e.ch == nil {
		return ErrMessageMiddlewareDisconnected
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return e.ch.PublishWithContext(
		ctx,
		e.exchange, // exchange
		e.keys[0],  // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			Body: []byte(msg.Body),
		},
	)
}

func (q *queueMiddleware) Send(msg Message) error {
	if q.ch == nil {
		return ErrMessageMiddlewareDisconnected
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return q.ch.PublishWithContext(
		ctx,
		"",          // default exchange
		q.queueName, // routing key = queue name
		false,
		false,
		amqp.Publishing{
			Body: []byte(msg.Body),
		},
	)
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

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
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

	return &exchangeMiddleware{
		rabbitMiddleware: &rabbitMiddleware{
			conn: conn,
			ch:   ch,
		},
		exchange: exchange,
		keys:     keys,
	}, nil
}

func CreateQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
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

	_, err = ch.QueueDeclare(
		queueName,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, mapError(err)
	}

	return &queueMiddleware{
		rabbitMiddleware: &rabbitMiddleware{
			conn: conn,
			ch:   ch,
		},
		queueName: queueName,
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
