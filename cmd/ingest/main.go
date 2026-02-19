package main

import (
	"flag"
	"log"
	"net/http"
	"strings"

	"github.com/yourorg/redpanda-mm/pkg/ingest"
	"github.com/yourorg/redpanda-mm/pkg/kafka"
)

func main() {
	var (
		broker   = flag.String("broker", "127.0.0.1:9092", "Kafka broker")
		region   = flag.String("region", "", "region: A or B")
		port     = flag.String("port", "8080", "HTTP port")
		logLevel = flag.String("log-level", "info", "log level")
	)
	flag.Parse()
	_ = logLevel

	if *region == "" {
		log.Fatal("--region is required")
	}

	brokers := strings.Split(*broker, ",")
	producer := kafka.NewProducer(brokers, []string{"ingest.A", "ingest.B"})
	defer producer.Close()

	svc, err := ingest.NewService(*region, producer)
	if err != nil {
		log.Fatal(err)
	}

	addr := ":" + *port
	log.Printf("ingest service region=%s listening on %s", strings.ToUpper(*region), addr)
	log.Fatal(http.ListenAndServe(addr, svc.Handler()))
}
