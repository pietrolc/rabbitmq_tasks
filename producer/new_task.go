package main

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func bodyFrom(args []string) string {
	var s string
	if (len(args) < 2) || os.Args[1] == "" {
		s = "hello from Pietro's RabbitMQ producer"
	} else {
		s = strings.Join(args[1:], " ")
	}
	return s
}

func main() {
	// 1. Read from environment variables
	rabbitMQUser := os.Getenv("RABBITMQ_USER")
	rabbitMQPass := os.Getenv("RABBITMQ_PASS")
	rabbitMQHost := os.Getenv("RABBITMQ_HOST")
	rabbitMQPort := os.Getenv("RABBITMQ_PORT")

	// Provide default values or error if not set
	if rabbitMQUser == "" {
		rabbitMQUser = "guest"
	}
	if rabbitMQPass == "" {
		rabbitMQPass = "guest"
	}
	if rabbitMQHost == "" {
		rabbitMQHost = "localhost"
	}
	if rabbitMQPort == "" {
		rabbitMQPort = "5672"
	}

	rabbitMQURL := "amqp://" + rabbitMQUser + ":" + rabbitMQPass + "@" + rabbitMQHost + ":" + rabbitMQPort + "/"
	conn, err := amqp.Dial(rabbitMQURL)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// Declare a queue to send messages to
	q, err := ch.QueueDeclare(
		"tasks_queue", // name
		true,          // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	failOnError(err, "Failed to declare a queue")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := bodyFrom(os.Args)
	err = ch.PublishWithContext(ctx,
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(body),
		})
	failOnError(err, "Failed to publish a message")
	log.Printf(" [x] Sent %s", body)
}
