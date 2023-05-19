package influx

import "strings"

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
