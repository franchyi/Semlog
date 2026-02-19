package kafka

import (
	"context"
	"errors"
	"time"

	kgo "github.com/segmentio/kafka-go"
)

type Producer struct {
	writers map[string]*kgo.Writer
}

func NewProducer(brokers []string, topics []string) *Producer {
	writers := make(map[string]*kgo.Writer, len(topics))
	for _, topic := range topics {
		writers[topic] = &kgo.Writer{
			Addr:         kgo.TCP(brokers...),
			Topic:        topic,
			RequiredAcks: kgo.RequireOne,
			Balancer:     &kgo.Hash{},
			Async:        false,
			BatchTimeout: 5 * time.Millisecond,
		}
	}
	return &Producer{writers: writers}
}

func (p *Producer) Write(ctx context.Context, topic, key string, value []byte) (partition int, offset int64, err error) {
	w, ok := p.writers[topic]
	if !ok {
		return -1, -1, errors.New("unknown topic")
	}
	msg := kgo.Message{Key: []byte(key), Value: value, Time: time.Now()}
	err = w.WriteMessages(ctx, msg)
	if err != nil {
		return -1, -1, err
	}
	return msg.Partition, msg.Offset, nil
}

func (p *Producer) Close() error {
	for _, w := range p.writers {
		if err := w.Close(); err != nil {
			return err
		}
	}
	return nil
}
