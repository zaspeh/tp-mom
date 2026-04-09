package factory

import (
	"context"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type queueMiddleware struct {
	conn *amqp.Connection
	ch   *amqp.Channel

	consumerTag string
	cancelFunc  context.CancelFunc
	queueName   string
}

func (q *queueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	if q.ch == nil {
		return m.ErrMessageMiddlewareDisconnected
	}

	msgs, err := consumeWithTag(q.ch, q.queueName, &q.consumerTag)
	if err != nil {
		return mapError(err)
	}

	return consumeLoop(msgs, &q.cancelFunc, callbackFunc)
}

func (q *queueMiddleware) StopConsuming() error {
	return stopConsuming(q.ch, &q.consumerTag, &q.cancelFunc)
}

func (q *queueMiddleware) Send(msg m.Message) error {
	if q.ch == nil {
		return m.ErrMessageMiddlewareDisconnected
	}
	return sendWithContext(q.ch, "", q.queueName, msg)
}

func (q *queueMiddleware) Close() error {
	_ = q.StopConsuming()
	var closeErr error = nil

	if q.ch != nil {
		if err := q.ch.Close(); err != nil {
			closeErr = m.ErrMessageMiddlewareClose
		}
		q.ch = nil
	}

	if q.conn != nil {
		if err := q.conn.Close(); err != nil {
			closeErr = m.ErrMessageMiddlewareClose
		}
		q.conn = nil
	}

	return closeErr
}
