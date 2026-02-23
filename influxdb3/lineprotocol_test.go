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
	"testing"
	"time"

	"github.com/influxdata/line-protocol/v2/lineprotocol"
	"github.com/stretchr/testify/require"
)

func TestLineProtocolPrecisionCompatibility(t *testing.T) {
	t.Run("WithPrecision accepts lineprotocol.Precision", func(t *testing.T) {
		var p lineprotocol.Precision = lineprotocol.Millisecond
		options := newWriteOptions(&DefaultWriteOptions, []WriteOption{WithPrecision(p)})
		require.Equal(t, lineprotocol.Millisecond, options.Precision)
	})

	t.Run("MarshalBinary accepts lineprotocol.Precision", func(t *testing.T) {
		point := NewPoint("m", map[string]string{"host": "a"}, map[string]any{"value": 1}, time.Unix(60, 0))
		var p lineprotocol.Precision = lineprotocol.Second

		line, err := point.MarshalBinary(p)
		require.NoError(t, err)
		require.Equal(t, "m,host=a value=1i 60\n", string(line))
	})

	t.Run("MarshalBinaryWithDefaultTags accepts lineprotocol.Precision", func(t *testing.T) {
		point := NewPoint("m", map[string]string{"host": "a"}, map[string]any{"value": 1}, time.Unix(60, 0))
		defaultTags := map[string]string{"region": "west"}
		var p lineprotocol.Precision = lineprotocol.Second

		line, err := point.MarshalBinaryWithDefaultTags(p, defaultTags)
		require.NoError(t, err)
		require.Equal(t, "m,host=a,region=west value=1i 60\n", string(line))
	})
}
