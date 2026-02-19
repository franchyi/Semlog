package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/yourorg/redpanda-mm/pkg/finalizer"
	"github.com/yourorg/redpanda-mm/pkg/kafka"
)

func main() {
	var (
		broker     = flag.String("broker", "127.0.0.1:9092", "Kafka broker")
		rebaseMode = flag.String("rebase-mode", "rebase", "rebase mode: rebase|lww")
		logLevel   = flag.String("log-level", "info", "log level")
	)
	flag.Parse()
	_ = logLevel

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	brokers := strings.Split(*broker, ",")
	producer := kafka.NewProducer(brokers, []string{"arb.final"})
	defer producer.Close()

	f := finalizer.New(brokers, producer, *rebaseMode)
	if err := f.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
