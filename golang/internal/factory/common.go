package factory

import (
	"context"
	"errors"
	"fmt"
	"time"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

func stopConsuming(ch *amqp.Channel, consumerTag *string, cancelFunc *context.CancelFunc) error {
	// Si no hay channel, probablemente ya esté desconectado
	if ch == nil {
		return m.ErrMessageMiddlewareDisconnected
	}

	if consumerTag != nil && *consumerTag != "" {
		err := ch.Cancel(*consumerTag, false)
		*consumerTag = ""
		if err != nil {
			return m.ErrMessageMiddlewareDisconnected
		}
	}

	if cancelFunc != nil && *cancelFunc != nil {
		(*cancelFunc)()
		*cancelFunc = nil
	}

	return nil
}

func sendWithContext(ch *amqp.Channel, exchange, routingKey string, msg m.Message) error {
	if ch == nil {
		return m.ErrMessageMiddlewareDisconnected
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := ch.PublishWithContext(
		ctx,
		exchange,   // exchange
		routingKey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		},
	)

	if err != nil {
		return mapError(err)
	}
	return nil
}

func consumeWithTag(ch *amqp.Channel, queueName string, consumerTag *string) (<-chan amqp.Delivery, error) {
	if ch == nil {
		return nil, m.ErrMessageMiddlewareDisconnected
	}

	tag := fmt.Sprintf("consumer-%d", time.Now().UnixNano())
	*consumerTag = tag

	// con esto le digo a rabbitMQ que no le de más de un mensaje a los trabajadores al mismo tiempo. (cuando lo procese luego le envío otro)
	err := ch.Qos(1, 0, false)
	if err != nil {
		return nil, mapError(err)
	}

	msgs, err := ch.Consume(
		queueName,
		tag,
		false,
		false,
		false,
		false,
		nil,
	)

	return msgs, err
}

func consumeLoop(
	msgs <-chan amqp.Delivery,
	cancelFunc *context.CancelFunc,
	callbackFunc func(msg m.Message, ack func(), nack func()),
) error {
	ctx, cancel := context.WithCancel(context.Background())
	*cancelFunc = cancel

	for {
		select {
		case <-ctx.Done():
			return nil

		case d, ok := <-msgs:
			if !ok {
				return m.ErrMessageMiddlewareDisconnected
			}

			callbackFunc(
				m.Message{Body: string(d.Body)},
				func() { _ = d.Ack(false) },
				func() { _ = d.Nack(false, true) },
			)
		}
	}
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, amqp.ErrClosed) {
		return m.ErrMessageMiddlewareDisconnected
	}
	return m.ErrMessageMiddlewareMessage
}

func dialWithRetry(url string, retries int) (*amqp.Connection, error) {
	var conn *amqp.Connection
	var err error

	for i := 0; i < retries; i++ {
		conn, err = amqp.Dial(url)
		if err == nil {
			return conn, nil
		}
		time.Sleep(2 * time.Second)
	}

	return nil, err
}
