// TODO: check if this is enough go-ish

package influx

import "strings"

func ReplaceURLProtocolWithPort(url string) string {
	url = strings.TrimSuffix(url, "/")

	if strings.HasPrefix(url, "http://") {
		url = strings.TrimPrefix(url, "http://")
		if strings.Count(url, ":") == 0 {
			url = url + ":80"
		}
	} else if strings.HasPrefix(url, "https://") {
		url = strings.TrimPrefix(url, "https://")
		if strings.Count(url, ":") == 0 {
			url = url + ":443"
		}
	}

	return url
}
