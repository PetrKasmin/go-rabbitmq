package main

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
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
	failOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		"", // Название очереди
		false,
		true,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to declare a queue")

	keys := []string{"error", "info", "warning"}

	for _, key := range keys {
		err = ch.QueueBind(
			q.Name,        // Название очереди
			key,           // Роутинг ключ
			"direct_logs", // Название обменника
			false,
			nil,
		)
		failOnError(err, "Failed to bind a queue")
	}

	msgs, err := ch.Consume(
		q.Name,         // Название очереди
		"consumer_all", // Название потребителя
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to register a consumer_ecological")

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")

	forever := make(chan bool)
	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
		}
	}()

	<-forever
}
