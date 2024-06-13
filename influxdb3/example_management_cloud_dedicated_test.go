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
	"log"
	"net/url"
	"os"
)

func ExampleDedicatedClient_CreateDatabase() {
	managementToken := os.Getenv("INFLUX_MANAGEMENT_TOKEN")
	accountID := os.Getenv("INFLUX_ACCOUNT_ID")
	clusterID := os.Getenv("INFLUX_CLUSTER_ID")
	managementAPIURL, err := url.Parse(os.Getenv("INFLUX_MANAGEMENT_API_URL"))
	if err != nil {
		log.Fatal(err)
	}

	client, err := NewFromEnv()
	if err != nil {
		panic(err)
	}

	cloudDedicatedConfig := CloudDedicatedClientConfig{
		AccountID:        accountID,
		ClusterID:        clusterID,
		ManagementToken:  managementToken,
		ManagementAPIURL: managementAPIURL,
	}

	defer client.Close()

	cloudDedicatedClient := NewCloudDedicatedClient(client)

	if err := cloudDedicatedClient.CreateDatabase(context.Background(), &cloudDedicatedConfig, &Database{}); err != nil {
		log.Fatal(err)
	}
}
