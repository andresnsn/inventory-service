package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	brokers := kafkaBrokersFromEnv()
	topic := envOrDefault("ORDERS_TOPIC", "orders.created.v1")
	groupID := envOrDefault("KAFKA_GROUP_ID", "inventory-service")
	catalogURL := envOrDefault("CATALOG_BASE_URL", "http://localhost:8081")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	catalog := httpCatalogClient{
		baseURL: catalogURL,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
	processor := orderProcessor{catalog: catalog}

	log.Printf("inventory-service consuming topic=%s group=%s brokers=%s catalog=%s", topic, groupID, strings.Join(brokers, ","), catalogURL)
	if err := consumeOrders(ctx, brokers, topic, groupID, processor); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("consume orders: %v", err)
	}
}

func consumeOrders(ctx context.Context, brokers []string, topic, groupID string, processor orderProcessor) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0,
	})
	defer reader.Close()

	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}

		var event orderCreatedEvent
		if err := decodeOrderEvent(message.Value, &event); err != nil {
			return fmt.Errorf("decode Kafka message at offset %d: %w", message.Offset, err)
		}
		results, err := processor.Process(ctx, event)
		if err != nil {
			return fmt.Errorf("process order %s at offset %d: %w", event.OrderID, message.Offset, err)
		}
		for _, result := range results {
			log.Printf("orderId=%s productId=%s name=%q requested=%d available=%d decision=%s",
				result.OrderID, result.ProductID, result.ProductName, result.Requested, result.Available, result.Decision)
		}
		if err := reader.CommitMessages(ctx, message); err != nil {
			return fmt.Errorf("commit Kafka message at offset %d: %w", message.Offset, err)
		}
	}
}

func kafkaBrokersFromEnv() []string {
	value := envOrDefault("KAFKA_BROKERS", "localhost:9092")
	parts := strings.Split(value, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		if broker := strings.TrimSpace(part); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	return brokers
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
