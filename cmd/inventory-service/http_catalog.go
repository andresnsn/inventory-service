package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

type httpCatalogClient struct {
	baseURL    string
	httpClient *http.Client
}

func (c httpCatalogClient) GetProduct(ctx context.Context, productID string) (catalogProduct, error) {
	endpoint := strings.TrimRight(c.baseURL, "/") + "/products/" + url.PathEscape(productID)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return catalogProduct{}, fmt.Errorf("build catalog request: %w", err)
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return catalogProduct{}, fmt.Errorf("call catalog: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return catalogProduct{}, fmt.Errorf("catalog returned HTTP %d", response.StatusCode)
	}

	var product catalogProduct
	if err := json.NewDecoder(response.Body).Decode(&product); err != nil {
		return catalogProduct{}, fmt.Errorf("decode catalog response: %w", err)
	}
	if product.ProductID == "" {
		return catalogProduct{}, fmt.Errorf("catalog response is missing productId")
	}
	return product, nil
}
