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

package gzip_test

import (
	"compress/gzip"
	"io"
	"testing"

	igzip "github.com/InfluxCommunity/influxdb3-go/v2/influxdb3/gzip"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompressWithGzip(t *testing.T) {
	t.Run("compresses and decompresses correctly", func(t *testing.T) {
		input := []byte("measurement,tag=value field=1 1000000000\n")
		r, err := igzip.CompressWithGzip(input)
		require.NoError(t, err)
		require.NotNil(t, r)

		gr, err := gzip.NewReader(r)
		require.NoError(t, err)
		defer gr.Close()

		got, err := io.ReadAll(gr)
		require.NoError(t, err)
		assert.Equal(t, input, got)
	})

	t.Run("returns replayable reader", func(t *testing.T) {
		input := []byte("measurement,tag=value field=1 1000000000\n")
		r, err := igzip.CompressWithGzip(input)
		require.NoError(t, err)

		readAndDecompress := func() []byte {
			_, err := r.Seek(0, io.SeekStart)
			require.NoError(t, err)
			gr, err := gzip.NewReader(r)
			require.NoError(t, err)
			defer gr.Close()
			data, err := io.ReadAll(gr)
			require.NoError(t, err)
			return data
		}

		// Read twice to confirm the reader is replayable
		assert.Equal(t, input, readAndDecompress())
		assert.Equal(t, input, readAndDecompress())
	})

	t.Run("empty input", func(t *testing.T) {
		r, err := igzip.CompressWithGzip([]byte{})
		require.NoError(t, err)
		require.NotNil(t, r)

		gr, err := gzip.NewReader(r)
		require.NoError(t, err)
		defer gr.Close()

		got, err := io.ReadAll(gr)
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("compressed output is smaller for repetitive data", func(t *testing.T) {
		// Generate repetitive data that should compress well
		input := make([]byte, 1024)
		for i := range input {
			input[i] = byte('a' + (i % 26))
		}

		r, err := igzip.CompressWithGzip(input)
		require.NoError(t, err)

		assert.Less(t, r.Size(), int64(len(input)))
	})
}
