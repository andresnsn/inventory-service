package main

import (
	"context"
	"testing"
)

type fakeCatalog struct {
	products map[string]catalogProduct
	calls    []string
}

func (c *fakeCatalog) GetProduct(_ context.Context, productID string) (catalogProduct, error) {
	c.calls = append(c.calls, productID)
	return c.products[productID], nil
}

func TestProcessOrderChecksEachItemAgainstCatalog(t *testing.T) {
	catalog := &fakeCatalog{products: map[string]catalogProduct{
		"book-123": {ProductID: "book-123", Name: "Go Basics", AvailableStock: 5},
		"pen-456":  {ProductID: "pen-456", Name: "Pen", AvailableStock: 1},
	}}
	processor := orderProcessor{catalog: catalog}
	event := orderCreatedEvent{
		OrderID: "order-1",
		Items: []orderItem{
			{ProductID: "book-123", Quantity: 2},
			{ProductID: "pen-456", Quantity: 3},
		},
	}

	results, err := processor.Process(context.Background(), event)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("results = %d, want 2", len(results))
	}
	if results[0].Decision != "available" || results[1].Decision != "insufficient_stock" {
		t.Fatalf("unexpected decisions: %q, %q", results[0].Decision, results[1].Decision)
	}
	if len(catalog.calls) != 2 || catalog.calls[0] != "book-123" || catalog.calls[1] != "pen-456" {
		t.Fatalf("catalog calls = %v", catalog.calls)
	}
}

func TestDecodeOrderEventRejectsUnknownFields(t *testing.T) {
	var event orderCreatedEvent
	err := decodeOrderEvent([]byte(`{"orderId":"order-1","items":[{"productId":"book-123","quantity":1}],"unexpected":true}`), &event)
	if err == nil {
		t.Fatal("decodeOrderEvent() accepted an unknown field")
	}
}
