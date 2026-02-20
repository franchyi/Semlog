package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/yourorg/redpanda-mm/pkg/finalizer"
	"github.com/yourorg/redpanda-mm/pkg/kafka"
)

func main() {
	var (
		broker       = flag.String("broker", "127.0.0.1:9092", "Kafka broker")
		rebaseMode   = flag.String("rebase-mode", "rebase", "rebase mode: rebase|rebase+llm|lww")
		llmProvider  = flag.String("llm-provider", "openai", "llm provider: openai")
		llmAPIURL    = flag.String("llm-api-url", "https://api.openai.com/v1/chat/completions", "LLM API URL")
		llmModel     = flag.String("llm-model", "gpt-5-mini", "LLM model")
		llmTimeoutMS = flag.Int("llm-timeout-ms", 15000, "LLM timeout (ms)")
		llmMaxTokens = flag.Int("llm-max-tokens", 1024, "LLM max response tokens")
		logLevel     = flag.String("log-level", "info", "log level")
	)
	flag.Parse()
	_ = logLevel

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	brokers := strings.Split(*broker, ",")
	producer := kafka.NewProducer(brokers, []string{"arb.final"})
	defer producer.Close()

	var llmClient finalizer.LLMClient
	if finalizer.NormalizeRebaseMode(*rebaseMode) == finalizer.RebaseModeRebaseLLM {
		switch strings.ToLower(strings.TrimSpace(*llmProvider)) {
		case "openai":
			llmClient = finalizer.NewOpenAIClientFromEnv(*llmAPIURL, *llmModel, time.Duration(*llmTimeoutMS)*time.Millisecond)
			if llmClient == nil {
				log.Printf("rebase+llm requested but OPENAI_API_KEY is not set; falling back to deterministic rebase")
			}
		default:
			log.Printf("unknown llm provider=%s; falling back to deterministic rebase", *llmProvider)
		}
	}

	f := finalizer.New(brokers, producer, finalizer.Config{
		RebaseMode: *rebaseMode,
		LLM:        llmClient,
		LLMTimeout: time.Duration(*llmTimeoutMS) * time.Millisecond,
		LLMTokens:  *llmMaxTokens,
	})
	if err := f.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
