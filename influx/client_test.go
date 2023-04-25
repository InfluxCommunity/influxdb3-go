package influx

import (
	"github.com/bonitoo-io/influxdb-client-go-3/influx/configs"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestClient(t *testing.T) {
	t.Run("create client", func(t *testing.T) {
		host := "https://eu-central-1-1.aws.cloud2.influxdata.com/"
		database := "my-database"
		token := "my-token"

		client, _ := NewClient(configs.ClientConfigs{
			Host:     &host,
			Database: &database,
			Token:    &token},
		)

		assert.Equal(t, client.host, host)
		assert.Equal(t, client.database, database)
		assert.Equal(t, client.token, token)
	})
}
