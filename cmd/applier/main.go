package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/yourorg/redpanda-mm/pkg/applier"
	"github.com/yourorg/redpanda-mm/pkg/kafka"
)

func main() {
	var (
		broker   = flag.String("broker", "127.0.0.1:9092", "Kafka broker")
		region   = flag.String("region", "", "region: A or B")
		port     = flag.String("port", "8090", "HTTP port")
		logLevel = flag.String("log-level", "info", "log level")
	)
	flag.Parse()
	_ = logLevel

	if *region == "" {
		log.Fatal("--region is required")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	brokers := strings.Split(*broker, ",")
	producer := kafka.NewProducer(brokers, []string{"arb.cert", "arb.final"})
	defer producer.Close()

	a, err := applier.New(*region, brokers, producer)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		if err := a.Run(ctx); err != nil {
			log.Printf("applier loop exited with error: %v", err)
			cancel()
		}
	}()

	srv := &http.Server{Addr: ":" + *port, Handler: a.Handler()}
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()

	log.Printf("applier region=%s listening on :%s", strings.ToUpper(*region), *port)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
