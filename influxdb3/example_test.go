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
	"fmt"
	"time"

	"github.com/influxdata/line-protocol/v2/lineprotocol"
)

func ExamplePoint_AddFieldFromValue() {
	p := NewPoint("measurement", map[string]string{}, map[string]interface{}{}, time.Now())
	p.AddFieldFromValue("hello", NewValueFromString("world"))
	p.AddFieldFromValue("float", NewValueFromFloat(55.101))
	p.AddFieldFromValue("time.Time", NewValueFromTime(time.Date(2020, time.March, 20, 10, 30, 23, 123456789, time.UTC)))
	p.SetTimestamp(time.Date(2020, time.March, 20, 10, 30, 23, 123456789, time.UTC))
	line, _ := p.MarshalBinary(lineprotocol.Nanosecond)
	fmt.Println(string(line))
	// Output: measurement float=55.101,hello="world",time.Time="2020-03-20T10:30:23.123456789Z" 1584700223123456789
}

func ExampleNewValueFromStringer() {
	p := NewPoint("measurement", map[string]string{}, map[string]interface{}{}, time.Now())
	p.AddFieldFromValue("Supports time.Duration", NewValueFromStringer(4*time.Hour))
	p.SetTimestamp(time.Date(2020, time.March, 20, 10, 30, 23, 123456789, time.UTC))
	line, _ := p.MarshalBinary(lineprotocol.Nanosecond)
	fmt.Println(string(line))
	// Output: measurement Supports\ time.Duration="4h0m0s" 1584700223123456789
}
