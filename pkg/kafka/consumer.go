package kafka

import (
	"context"
	"time"

	kgo "github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kgo.Reader
}

func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	return &Consumer{reader: kgo.NewReader(kgo.ReaderConfig{
		Brokers:     brokers,
		GroupID:     groupID,
		Topic:       topic,
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     200 * time.Millisecond,
		StartOffset: kgo.FirstOffset,
	})}
}

func (c *Consumer) Read(ctx context.Context) (kgo.Message, error) {
	return c.reader.FetchMessage(ctx)
}

func (c *Consumer) Commit(ctx context.Context, msg kgo.Message) error {
	return c.reader.CommitMessages(ctx, msg)
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
