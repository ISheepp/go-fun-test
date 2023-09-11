package main

import (
	"crypto/tls"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"os"
	"time"
)

func getDefaultKafkaWriter() (*kafka.Writer, error) {
	mechanism, err := scram.Mechanism(scram.SHA256, os.Getenv("KAFKA_USERNAME"), os.Getenv("KAFKA_PASSWORD"))
	if err != nil {
		return nil, err
	}

	dialer := &kafka.Dialer{
		SASLMechanism: mechanism,
		TLS:           &tls.Config{},
	}

	w := &kafka.Writer{
		Addr:         kafka.TCP(os.Getenv("KAFKA_URL")),
		Topic:        "game",
		Balancer:     &kafka.Hash{},
		WriteTimeout: 10 * time.Second,
		Transport: &kafka.Transport{
			Dial: dialer.DialFunc,
			TLS:  dialer.TLS,
		}}
	return w, nil
}
