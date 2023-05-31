package influx

import "strings"

// ReplaceURLProtocolWithPort removes the "http://" or "https://" protocol from the given URL and replaces it with the port number.
// Currently, Apache Arrow does not support the "http://" or "https://" protocol in the URL, so this function is used to remove it.
// If a port number is already present in the URL, only the protocol is removed.
// The function also returns a boolean value indicating whether the communication is safe or unsafe.
// - If the URL starts with "https://", the communication is considered safe, and the returned boolean value will be true.
// - If the URL starts with "http://", the communication is considered unsafe, and the returned boolean value will be false.
// - If the URL does not start with either "http://" or "https://", the returned boolean value will be nil.
//
// Parameters:
//   - url: The URL to process.
// Returns:
//   - The modified URL with the protocol replaced by the port.
//   - A boolean value indicating the safety of communication (true for safe, false for unsafe) or nil if not detected.
func ReplaceURLProtocolWithPort(url string) (string, *bool) {
	url = strings.TrimSuffix(url, "/")
	var safe *bool

	if strings.HasPrefix(url, "http://") {
		url = strings.TrimPrefix(url, "http://")
		safe = new(bool)
		*safe = false
		if strings.Count(url, ":") == 0 {
			url = url + ":80"
		}
	} else if strings.HasPrefix(url, "https://") {
		url = strings.TrimPrefix(url, "https://")
		safe = new(bool)
		*safe = true
		if strings.Count(url, ":") == 0 {
			url = url + ":443"
		}
	}

	return url, safe
}
