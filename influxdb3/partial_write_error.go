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
	"encoding/json"
	"fmt"
	"strings"
)

// PartialWriteLineError describes a single line-level write failure returned by /api/v3/write_lp.
type PartialWriteLineError struct {
	// ErrorMessage describes why the line failed.
	ErrorMessage string `json:"error_message"`
	// LineNumber is a 1-based line index in the submitted payload.
	LineNumber int `json:"line_number"`
	// OriginalLine is the line content reported by server.
	OriginalLine string `json:"original_line"`
}

// PartialWriteError represents a /api/v3/write_lp error that carries per-line failure details.
type PartialWriteError struct {
	ServerError
	LineErrors []PartialWriteLineError
}

// Unwrap allows errors.As(err, &serverErr) where serverErr is *ServerError
// when the original error is a *PartialWriteError.
func (e *PartialWriteError) Unwrap() error {
	if e == nil {
		return nil
	}
	return &e.ServerError
}

func parsePartialWriteDataArray(raw json.RawMessage) ([]PartialWriteLineError, []string, bool) {
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, nil, false
	}
	if len(items) == 0 {
		return nil, nil, false
	}

	var lineErrors []PartialWriteLineError
	allTyped := true
	for _, item := range items {
		lineError, ok := parsePartialWriteLineError(item)
		if !ok || lineError.ErrorMessage == "" {
			allTyped = false
			continue
		}
		lineErrors = append(lineErrors, lineError)
	}

	if allTyped {
		return lineErrors, formatPartialWriteLineErrorDetails(lineErrors), true
	}

	details := make([]string, 0, len(items))
	for _, item := range items {
		detail := strings.TrimSpace(string(item))
		if detail != "" && !strings.EqualFold(detail, "null") {
			details = append(details, detail)
		}
	}
	return lineErrors, details, true
}

func formatPartialWriteLineErrorDetails(lineErrors []PartialWriteLineError) []string {
	details := make([]string, 0, len(lineErrors))
	for _, lineError := range lineErrors {
		if lineError.LineNumber != 0 {
			if lineError.OriginalLine != "" {
				details = append(details, fmt.Sprintf(
					"line %d: %s (%s)",
					lineError.LineNumber,
					lineError.ErrorMessage,
					lineError.OriginalLine,
				))
			} else {
				details = append(details, fmt.Sprintf(
					"line %d: %s",
					lineError.LineNumber,
					lineError.ErrorMessage,
				))
			}
		} else if lineError.ErrorMessage != "" {
			details = append(details, lineError.ErrorMessage)
		}
	}
	return details
}

func parsePartialWriteLineError(raw json.RawMessage) (PartialWriteLineError, bool) {
	var lineError PartialWriteLineError
	if err := json.Unmarshal(raw, &lineError); err != nil {
		return PartialWriteLineError{}, false
	}

	if lineError.LineNumber == 0 && lineError.ErrorMessage == "" && lineError.OriginalLine == "" {
		return PartialWriteLineError{}, false
	}

	return lineError, true
}
