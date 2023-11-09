package main

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"direct_logs",
		"direct",
		false,
		true,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to declare a queue")

	ticker := time.NewTicker(3 * time.Second)
	counts := 1
	for range ticker.C {
		pushMessage(ch, "error", counts)
		counts++
		pushMessage(ch, "info", counts)
		counts++
		pushMessage(ch, "warning", counts)
		counts++
	}
}

func pushMessage(ch *amqp.Channel, key string, num int) {
	err := ch.PublishWithContext(
		context.TODO(),
		"direct_logs",
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(fmt.Sprintf("%d", num)),
		})

	log.Printf("Send message [%s:%d]\n", key, num)

	failOnError(err, "Failed to publish a message")
}
