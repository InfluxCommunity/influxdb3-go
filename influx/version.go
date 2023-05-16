// Copyright 2021 InfluxData, Inc. All rights reserved.
// Use of this source code is governed by MIT
// license that can be found in the LICENSE file.

package influx

import (
	"runtime"
)

// version defines current version
const version = "1.0.0"

// userAgent header value
const userAgent = "influxdb3-go/" + version + " (" + runtime.GOOS + "; " + runtime.GOARCH + ")"
