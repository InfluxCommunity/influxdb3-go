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
	"math"
	"testing"
	"time"

	"github.com/influxdata/line-protocol/v2/lineprotocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewValueFromNative(t *testing.T) {
	p := NewPoint(
		"test",
		map[string]string{
			"id":        "10ad=",
			"ven=dor":   "AWS",
			`host"name`: `ho\st "a"`,
			`x\" x`:     "a b",
		},
		map[string]interface{}{},
		time.Unix(60, 70))

	p.AddFieldFromValue("float64", NewValueFromNative(80.1234567))
	p.AddFieldFromValue("int64", NewValueFromNative(int64(-1234567890)))
	p.AddFieldFromValue("uint64", NewValueFromNative(uint64(12345677890)))
	p.AddFieldFromValue("string", NewValueFromNative(`six, "seven", eight`))
	p.AddFieldFromValue("bytes", NewValueFromNative([]byte(`six=seven\, eight`)))
	p.AddFieldFromValue("bool", NewValueFromNative(false))

	line, err := p.MarshalBinary(lineprotocol.Nanosecond)
	require.NoError(t, err)
	assert.EqualValues(t, `test,host"name=ho\st\ "a",id=10ad\=,ven\=dor=AWS,x\"\ x=a\ b bool=false,bytes="six=seven\\, eight",float64=80.1234567,int64=-1234567890i,string="six, \"seven\", eight",uint64=12345677890u 60000000070`+"\n", string(line))

	assert.PanicsWithError(t, "invalid value for NewValue: float64 (+Inf)", func() { NewValueFromNative(math.Inf(1)) })
	assert.PanicsWithError(t, "invalid value for NewValue: float64 (-Inf)", func() { NewValueFromNative(math.Inf(-1)) })
	assert.PanicsWithError(t, "invalid value for NewValue: string (\"\\xed\\x9f\\xc1\")", func() { NewValueFromNative(string([]byte{237, 159, 193})) })
}
