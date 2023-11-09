package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var cars = []string{"BMW", "Audi", "Tesla", "Mercedes"}
var colors = []string{"red", "white", "black"}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func generateRoutingKey() string {
	rand.NewSource(time.Now().UnixNano())
	randCarIndex := rand.Intn(len(cars))
	randColorIndex := rand.Intn(len(colors))
	return fmt.Sprintf("%s.%s", cars[randCarIndex], colors[randColorIndex])
}

func main() {
	counter := 0

	for {
		timeToSleep := rand.Intn(1000) + 1000
		time.Sleep(time.Duration(timeToSleep) * time.Millisecond)

		conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
		failOnError(err, "Failed to connect to RabbitMQ")
		defer conn.Close()

		ch, err := conn.Channel()
		failOnError(err, "Failed to open a channel")
		defer ch.Close()

		err = ch.ExchangeDeclare(
			"topic_logs",
			"topic",
			false,
			true,
			false,
			false,
			nil,
		)
		failOnError(err, "Failed to declare an exchange")

		var routingKey string
		if counter%4 == 0 {
			routingKey = "Tesla.red.fast.ecological"
		} else if counter%5 == 0 {
			routingKey = "Mercedes.exclusive.expensive.ecological"
		} else {
			routingKey = generateRoutingKey()
		}

		message := fmt.Sprintf("Message type [%s] from publisher N %d", routingKey, counter)

		err = ch.PublishWithContext(
			context.TODO(),
			"topic_logs",
			routingKey,
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(message),
			})
		failOnError(err, "Failed to publish a message")

		log.Printf("Message type [%s] is sent into Topic Exchange [N:%d]", routingKey, counter)
		counter++
	}
}
