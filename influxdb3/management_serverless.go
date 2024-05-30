package influxdb3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type (
	// ServerlessClient represents a client for InfluxDB Serverless administration operations.
	ServerlessClient struct {
		client *Client
	}

	Bucket struct {
		Name           string                `json:"name"`
		OrgID          string                `json:"orgID,omitempty"`
		Description    string                `json:"description,omitempty"`
		RetentionRules []BucketRetentionRule `json:"retentionRules"`
	}

	BucketRetentionRule struct {
		Type               string `json:"type,omitempty"`
		EverySeconds       int    `json:"everySeconds,omitempty"`
		ShardGroupDuration int    `json:"shardGroupDuration,omitempty"`
	}
)

// NewServerlessClient creates new ServerlessClient with given InfluxDB client.
func NewServerlessClient(client *Client) *ServerlessClient {
	return &ServerlessClient{client: client}
}

// CreateBucket creates a new bucket
func (c *ServerlessClient) CreateBucket(ctx context.Context, bucket *Bucket) error {
	if bucket == nil {
		return fmt.Errorf("bucket must not be nil")
	}

	if bucket.OrgID == "" {
		bucket.OrgID = c.client.config.Organization
	}

	if bucket.Name == "" {
		bucket.Name = c.client.config.Database
	}

	return c.createBucket(ctx, "/api/v2/buckets", bucket)
}

// createBucket is a helper function for CreateBucket to enhance test coverage.
func (c *ServerlessClient) createBucket(ctx context.Context, path string, bucket any) error {
	u, err := c.client.apiURL.Parse(path)
	if err != nil {
		return fmt.Errorf("failed to parth bucket creation path: %w", err)
	}

	body, err := json.Marshal(bucket)
	if err != nil {
		return fmt.Errorf("failed to marshal bucket creation request body: %w", err)
	}

	headers := http.Header{}
	headers.Set("Content-Type", "application/json")

	param := httpParams{
		endpointURL: u,
		queryParams: nil,
		httpMethod:  "POST",
		headers:     headers,
		body:        bytes.NewReader(body),
	}

	_, err = c.client.makeAPICall(ctx, param)
	return err
}
