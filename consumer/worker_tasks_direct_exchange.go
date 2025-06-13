package main

import (
	"bytes"
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

func routingKeyFrom(args []string) string {
	var s string
	if (len(args) < 2) || os.Args[1] == "" {
		s = "default_routing_key"
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

	err = ch.ExchangeDeclare(
		"shared_tasks_direct", // name
		"direct",              // type
		true,                  // durable
		false,                 // auto-deleted
		false,                 // internal
		false,                 // no-wait
		nil,                   // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name,                  // queue name
		routingKeyFrom(os.Args), // routing key
		"shared_tasks_direct",   // exchange
		false,
		nil,
	)
	failOnError(err, "Failed to bind a queue")

	/*
		// This section is commented out because it is not needed for the fanout exchange
		// Set Quality of Service (QoS) to limit the number of unacknowledged messages
		   	err = ch.Qos(
		   		1,     // prefetch count
		   		0,     // prefetch size
		   		false, // global
		   	)
		   	failOnError(err, "Failed to set QoS") */

	// Start consuming messages from the queue
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	// Create a channel to keep the program running
	var forever chan struct{}

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			dotCount := bytes.Count(d.Body, []byte("."))
			t := time.Duration(dotCount)
			time.Sleep(t * time.Second)
			log.Printf("Done")
			d.Ack(false) // Acknowledge the message
			log.Printf("Acknowledged message: %s", d.Body)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever // Block forever to keep the program running
}
