package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

type orderItem struct {
	ProductID string `json:"productId"`
	Quantity  int    `json:"quantity"`
}

type orderCreatedEvent struct {
	OrderID   string      `json:"orderId"`
	Items     []orderItem `json:"items"`
	CreatedAt time.Time   `json:"createdAt"`
}

type catalogProduct struct {
	ProductID      string `json:"productId"`
	Name           string `json:"name"`
	AvailableStock int    `json:"availableStock"`
}

type stockResult struct {
	OrderID     string
	ProductID   string
	ProductName string
	Requested   int
	Available   int
	Decision    string
}

type catalogClient interface {
	GetProduct(ctx context.Context, productID string) (catalogProduct, error)
}

type orderProcessor struct {
	catalog catalogClient
}

func (p orderProcessor) Process(ctx context.Context, event orderCreatedEvent) ([]stockResult, error) {
	if err := validateOrderEvent(event); err != nil {
		return nil, err
	}

	results := make([]stockResult, 0, len(event.Items))
	for _, item := range event.Items {
		product, err := p.catalog.GetProduct(ctx, item.ProductID)
		if err != nil {
			return nil, fmt.Errorf("look up product %s: %w", item.ProductID, err)
		}

		decision := "available"
		if product.AvailableStock < item.Quantity {
			decision = "insufficient_stock"
		}
		results = append(results, stockResult{
			OrderID:     event.OrderID,
			ProductID:   product.ProductID,
			ProductName: product.Name,
			Requested:   item.Quantity,
			Available:   product.AvailableStock,
			Decision:    decision,
		})
	}
	return results, nil
}

func decodeOrderEvent(data []byte, event *orderCreatedEvent) error {
	decoder := json.NewDecoder(io.LimitReader(bytes.NewReader(data), 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(event); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("message must contain one JSON object")
	}
	return validateOrderEvent(*event)
}

func validateOrderEvent(event orderCreatedEvent) error {
	if event.OrderID == "" {
		return errors.New("orderId is required")
	}
	if len(event.Items) == 0 {
		return errors.New("items must contain at least one product")
	}
	for _, item := range event.Items {
		if item.ProductID == "" {
			return errors.New("each item must have a productId")
		}
		if item.Quantity <= 0 {
			return errors.New("each item quantity must be greater than zero")
		}
	}
	return nil
}
