package kafka

import (
	"context"

	kgo "github.com/segmentio/kafka-go"
)

func FetchRecord(brokers []string, topic string, partition int, offset int64) (*kgo.Message, error) {
	reader := kgo.NewReader(kgo.ReaderConfig{
		Brokers:   brokers,
		Topic:     topic,
		Partition: partition,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer reader.Close()

	if err := reader.SetOffset(offset); err != nil {
		return nil, err
	}
	msg, err := reader.ReadMessage(context.Background())
	if err != nil {
		return nil, err
	}
	return &msg, nil
}
