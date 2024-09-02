/*
 The MIT License

 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:

 The above copyright notice and this permission notice shall be included in
 all copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 THE SOFTWARE.
*/

package influxdb3

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDedicatedClientCreateDatabase(t *testing.T) {
	correctPath := fmt.Sprintf("/api/v0/accounts/%s/clusters/%s/databases", "test-account", "test-cluster")

	tests := []struct {
		name         string
		db           *Database
		clientConfig *ClientConfig
		wantBody     map[string]any
		wantErr      bool
	}{
		{
			name: "create database with defaults",
			db: &Database{
				ClusterDatabasePartitionTemplate: []PartitionTemplate{},
			},
			clientConfig: &ClientConfig{
				Host:         "",
				Token:        "my-token",
				Organization: "default-organization",
				Database:     "default-database",
			},
			wantBody: map[string]any{
				"name":               "default-database",
				"maxTables":          float64(500),
				"maxColumnsPerTable": float64(250),
				"retentionPeriod":    float64(0),
				"partitionTemplate":  []any{},
			},
			wantErr: false,
		},
		{
			name: "create database with name and defaults",
			db: &Database{
				ClusterDatabaseName:              "test-database",
				ClusterDatabasePartitionTemplate: []PartitionTemplate{},
			},
			clientConfig: &ClientConfig{
				Host:         "",
				Token:        "my-token",
				Organization: "default-organization",
				Database:     "default-database",
			},
			wantBody: map[string]any{
				"name":               "test-database",
				"maxTables":          float64(500),
				"maxColumnsPerTable": float64(250),
				"retentionPeriod":    float64(0),
				"partitionTemplate":  []any{},
			},
			wantErr: false,
		},
		{
			name: "create database with name and custom values",
			db: &Database{
				ClusterDatabaseName:               "test-database",
				ClusterDatabaseMaxTables:          1000,
				ClusterDatabaseMaxColumnsPerTable: 500,
				ClusterDatabaseRetentionPeriod:    1000,
				ClusterDatabasePartitionTemplate: []PartitionTemplate{
					Tag{
						Type:  "tag",
						Value: "tag-value",
					},
					TagBucket{
						Type: "tag",
						Value: TagBucketValue{
							TagName:         "tagName",
							NumberOfBuckets: 3,
						},
					},
				},
			},
			clientConfig: &ClientConfig{
				Host:         "",
				Token:        "my-token",
				Organization: "default-organization",
				Database:     "default-database",
			},
			wantBody: map[string]any{
				"name":               "test-database",
				"maxTables":          float64(1000),
				"maxColumnsPerTable": float64(500),
				"retentionPeriod":    float64(1000),
				"partitionTemplate": []any{
					map[string]any{
						"type":  "tag",
						"value": "tag-value",
					},
					map[string]any{
						"type": "tag",
						"value": map[string]any{
							"tagName":         "tagName",
							"numberOfBuckets": float64(3),
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "nil database",
			db:   nil,
			clientConfig: &ClientConfig{
				Host:         "",
				Token:        "my-token",
				Organization: "default-organization",
				Database:     "default-database",
			},
			wantBody: map[string]any{},
			wantErr:  true,
		},
		{
			name: "db partition template has more than 7 tags",
			db: &Database{
				ClusterDatabaseName: "test-database",
				ClusterDatabasePartitionTemplate: []PartitionTemplate{
					Tag{},
					Tag{},
					Tag{},
					Tag{},
					Tag{},
					Tag{},
					Tag{},
					Tag{},
				},
			},
			clientConfig: &ClientConfig{
				Host:         "",
				Token:        "my-token",
				Organization: "default-organization",
				Database:     "default-database",
			},
			wantBody: map[string]any{},
			wantErr:  true,
		},
		{
			name: "empty database name",
			db: &Database{
				ClusterDatabaseName: "",
			},
			clientConfig: &ClientConfig{
				Host:         "",
				Token:        "my-token",
				Organization: "default-organization",
				Database:     "",
			},
			wantBody: map[string]any{},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// initialization of query client
				if r.Method == "PRI" {
					return
				}

				assert.EqualValues(t, correctPath, r.URL.String())
				bodyBytes, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				var body map[string]any
				err = json.Unmarshal(bodyBytes, &body)
				require.NoError(t, err)
				assert.Equal(t, tt.wantBody, body)
				w.WriteHeader(201)
			}))

			tt.clientConfig.Host = ts.URL
			c, err := New(*tt.clientConfig)
			require.NoError(t, err)

			managementAPIURL, _ := url.Parse(ts.URL)
			config := CloudDedicatedClientConfig{
				AccountID:        "test-account",
				ClusterID:        "test-cluster",
				ManagementToken:  "dummy",
				ManagementAPIURL: managementAPIURL,
			}

			dc := NewCloudDedicatedClient(c)
			err = dc.CreateDatabase(context.Background(), &config, tt.db)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}

	t.Run("Internal error cases", func(t *testing.T) {
		c, err := New(ClientConfig{
			Host:  "dummy",
			Token: "dummy",
		})
		require.NoError(t, err)

		managementAPIURL, _ := url.Parse(c.config.Host)

		config := CloudDedicatedClientConfig{
			AccountID:        "test-account",
			ClusterID:        "test-cluster",
			ManagementToken:  "dummy",
			ManagementAPIURL: managementAPIURL,
		}

		dc := NewCloudDedicatedClient(c)
		err = dc.createDatabase(context.Background(), "wrong path:", nil, &config)
		assert.Error(t, err)

		wrongBody := map[string]any{
			"funcField": func() {},
		}

		err = dc.createDatabase(context.Background(), correctPath, wrongBody, &config)
		assert.Error(t, err)

		config.ManagementAPIURL = nil
		err = dc.createDatabase(context.Background(), correctPath, nil, &config)
		assert.Error(t, err)
	})

}
