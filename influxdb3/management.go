package influxdb3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type (
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

func (c *Client) CreateBucket(ctx context.Context, bucket *Bucket) error {
	u, _ := c.apiURL.Parse("/api/v2/buckets")

	if bucket.OrgID == "" {
		bucket.OrgID = c.config.Organization
	}

	if bucket.Name == "" {
		bucket.Name = c.config.Database
	}

	headers := http.Header{}
	headers.Set("Content-Type", "application/json")

	body, err := json.Marshal(bucket)
	if err != nil {
		return fmt.Errorf("failed to marshal bucket creation request body: %w", err)
	}

	param := httpParams{
		endpointURL: u,
		queryParams: nil,
		httpMethod:  "POST",
		headers:     headers,
		body:        bytes.NewReader(body),
	}

	_, err = c.makeAPICall(ctx, param)
	return err
}
