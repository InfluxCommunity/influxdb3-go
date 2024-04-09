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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryDatabaseNotSet(t *testing.T) {
	c, err := New(ClientConfig{
		Host:  "http://localhost:8086",
		Token: "my-token",
	})
	require.NoError(t, err)
	iterator, err := c.Query(context.Background(), "SHOW NAMESPACES")
	assert.Nil(t, iterator)
	assert.Error(t, err)
	assert.EqualError(t, err, "database not specified")
}

func TestQueryWithOptionsNotSet(t *testing.T) {
	c, err := New(ClientConfig{
		Host:     "http://localhost:8086",
		Token:    "my-token",
		Database: "my-database",
	})
	require.NoError(t, err)
	iterator, err := c.QueryWithOptions(context.Background(), nil, "SHOW NAMESPACES")
	assert.Nil(t, iterator)
	assert.Error(t, err)
	assert.EqualError(t, err, "options not set")
}
