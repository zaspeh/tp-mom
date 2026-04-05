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

	consumerTag string
	cancelFunc  context.CancelFunc
	queueName   string
}

type exchangeMiddleware struct {
	*rabbitMiddleware

	exchange string
	keys     []string
}

type queueMiddleware struct {
	*rabbitMiddleware
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

	msgs, err := e.consumeWithTag(e.queueName)

	if err != nil {
		return mapError(err)
	}

	e.consumeLoop(msgs, callbackFunc)

	return nil
}

func (q *queueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	msgs, err := q.consumeWithTag(q.queueName)

	if err != nil {
		return mapError(err)
	}

	q.consumeLoop(msgs, callbackFunc)

	return nil
}

func (r *rabbitMiddleware) StopConsuming() error {
	// Si no hay channel, probablemente ya esté desconectado
	if r.ch == nil {
		return ErrMessageMiddlewareDisconnected
	}

	if r.ch != nil && r.consumerTag != "" {
		err := r.ch.Cancel(r.consumerTag, false)
		r.consumerTag = ""
		if err != nil {
			return mapError(err)
		}
	}

	if r.cancelFunc != nil {
		r.cancelFunc()
		r.cancelFunc = nil
	}

	return nil
}

func (e *exchangeMiddleware) Send(msg Message) error {
	// debería haber una routing key
	if len(e.keys) == 0 {
		return ErrMessageMiddlewareMessage
	}

	return sendWithContext(e.ch, e.exchange, e.keys[0], msg)
}

func (q *queueMiddleware) Send(msg Message) error {
	return sendWithContext(q.ch, "", q.queueName, msg)
}

func (r *rabbitMiddleware) Close() error {
	if err := r.StopConsuming(); err != nil {
		// TODO: preguntar al profe que error es resoluble y que no.
		if err != ErrMessageMiddlewareDisconnected {
			return ErrMessageMiddlewareClose
		}
	}

	if r.ch != nil {
		if err := r.ch.Close(); err != nil { // seguro sea error interno no resoluble
			return ErrMessageMiddlewareClose
		}
		r.ch = nil
	}

	if r.conn != nil {
		if err := r.conn.Close(); err != nil { // seguro sea error interno no resoluble
			return ErrMessageMiddlewareClose
		}
		r.conn = nil
	}

	return nil
}

// CONSTRUCTORES

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
			conn:      conn,
			ch:        ch,
			queueName: queueName,
		},
	}, nil
}

// FUNCIONES AUXILIARES

func sendWithContext(ch *amqp.Channel, exchange, routingKey string, msg Message) error {
	if ch == nil {
		return ErrMessageMiddlewareDisconnected
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
			Body: []byte(msg.Body),
		},
	)

	if err != nil {
		return mapError(err)
	}
	return nil
}

func (r *rabbitMiddleware) consumeWithTag(queueName string) (<-chan amqp.Delivery, error) {
	tag := fmt.Sprintf("consumer-%d", time.Now().UnixNano())
	r.consumerTag = tag

	// con esto le digo a rabbitMQ que no le de más de un mensaje a los trabajadores al mismo tiempo. (cuando lo procese luego le envío otro)
	err := r.ch.Qos(1, 0, false)
	if err != nil {
		return nil, mapError(err)
	}

	msgs, err := r.ch.Consume(
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

// Se usa select en lugar de "range msgs" para poder cancelar el consumo
// mediante el contexto -> "range" podía bloquearse esperando mensajes
func (r *rabbitMiddleware) consumeLoop(
	msgs <-chan amqp.Delivery,
	callbackFunc func(msg Message, ack func(), nack func()),
) {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancelFunc = cancel

	go func() {
		for {
			select {
			case <-ctx.Done(): // si se cerró la comunicación...
				return

			case d, ok := <-msgs: // esto me permite iterar los mensajes
				if !ok {
					return
				}

				callbackFunc(
					Message{Body: string(d.Body)},
					func() { _ = d.Ack(false) },
					func() { _ = d.Nack(false, true) },
				)
			}
		}
	}()
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
