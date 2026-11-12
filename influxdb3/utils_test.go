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
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"software.sslmate.com/src/go-pkcs12"
)

func TestLoadX509CertPool(t *testing.T) {
	t.Run("valid certificates PEM file", func(t *testing.T) {
		t.Parallel()
		certPath := filepath.Join("testdata", "valid_certs.pem")
		pool, err := loadX509CertPool(certPath)
		require.NoError(t, err)
		assert.NotNil(t, pool)
	})

	t.Run("valid crt file", func(t *testing.T) {
		t.Parallel()
		certPath := filepath.Join("testdata", "certificates", "client.crt")
		pool, err := loadX509CertPool(certPath)
		require.NoError(t, err)
		assert.NotNil(t, pool)
	})

	t.Run("invalid certificates PEM file", func(t *testing.T) {
		t.Parallel()
		certPath := filepath.Join("testdata", "invalid_certs.pem")
		pool, err := loadX509CertPool(certPath)
		require.NoError(t, err)
		assert.NotNil(t, pool)
	})

	t.Run("empty certificate file", func(t *testing.T) {
		t.Parallel()
		tempDir := t.TempDir()
		emptyFile := filepath.Join(tempDir, "empty.pem")
		err := os.WriteFile(emptyFile, []byte{}, 0600)
		require.NoError(t, err)

		pool, err := loadX509CertPool(emptyFile)
		require.NoError(t, err)
		assert.NotNil(t, pool)
	})

	t.Run("non-existent certificate file", func(t *testing.T) {
		t.Parallel()
		certPath := filepath.Join("testdata", "non_existing_file.pem")
		pool, err := loadX509CertPool(certPath)
		require.Error(t, err)
		assert.Nil(t, pool)
		assert.Contains(t, err.Error(), "error reading")
	})
}

func TestLoadCertificate(t *testing.T) {
	t.Run("valid PEM cert and key pair", func(t *testing.T) {
		t.Parallel()
		certFile := filepath.Join("testdata", "certificates", "client.crt")
		keyFile := filepath.Join("testdata", "certificates", "client.key")

		cert, err := loadCertificate(certFile, keyFile, "")
		require.NoError(t, err)
		assert.NotEmpty(t, cert.Certificate)
		assert.NotNil(t, cert.PrivateKey)
	})

	t.Run("valid PKCS#12 certificate file", func(t *testing.T) {
		t.Parallel()
		p12File := filepath.Join("testdata", "certificates", "client.p12")

		cert, err := loadCertificate(p12File, "", "changeit")
		require.NoError(t, err)
		assert.NotEmpty(t, cert.Certificate)
		assert.NotNil(t, cert.PrivateKey)
		assert.NotNil(t, cert.Leaf)
	})

	t.Run("PKCS#12 with invalid password", func(t *testing.T) {
		t.Parallel()
		p12File := filepath.Join("testdata", "certificates", "client.p12")

		cert, err := loadCertificate(p12File, "", "wrongpassword")
		require.Error(t, err)
		assert.Empty(t, cert.Certificate)
	})

	t.Run("PKCS#12 non-existent file", func(t *testing.T) {
		t.Parallel()
		p12File := filepath.Join("testdata", "certificates", "non_existing.p12")

		cert, err := loadCertificate(p12File, "", "changeit")
		require.Error(t, err)
		assert.Empty(t, cert.Certificate)
	})

	t.Run("PEM cert non-existent cert file", func(t *testing.T) {
		t.Parallel()
		certFile := filepath.Join("testdata", "certificates", "non_existing.crt")
		keyFile := filepath.Join("testdata", "certificates", "client.key")

		cert, err := loadCertificate(certFile, keyFile, "")
		require.Error(t, err)
		assert.Empty(t, cert.Certificate)
	})

	t.Run("PEM cert non-existent key file", func(t *testing.T) {
		t.Parallel()
		certFile := filepath.Join("testdata", "certificates", "client.crt")
		keyFile := filepath.Join("testdata", "certificates", "non_existing.key")

		cert, err := loadCertificate(certFile, keyFile, "")
		require.Error(t, err)
		assert.Empty(t, cert.Certificate)
	})

	t.Run("PEM cert mismatched key", func(t *testing.T) {
		t.Parallel()
		certFile := filepath.Join("testdata", "certificates", "client.crt")
		keyFile := filepath.Join("testdata", "certificates", "influxdb.key")

		cert, err := loadCertificate(certFile, keyFile, "")
		require.Error(t, err)
		assert.Empty(t, cert.Certificate)
	})
}

func TestProcessPKCS12(t *testing.T) {
	testCases := []struct {
		name     string
		file     string
		password string
		wantErr  bool
	}{
		{
			name:     "valid client.p12 with password",
			file:     filepath.Join("testdata", "certificates", "client.p12"),
			password: "changeit",
			wantErr:  false,
		},
		{
			name:     "valid influxdb.p12 with password",
			file:     filepath.Join("testdata", "certificates", "influxdb.p12"),
			password: "changeit",
			wantErr:  false,
		},
		{
			name:     "valid other-server.p12 with password",
			file:     filepath.Join("testdata", "certificates", "other-server.p12"),
			password: "changeit",
			wantErr:  false,
		},
		{
			name:     "incorrect password",
			file:     filepath.Join("testdata", "certificates", "client.p12"),
			password: "wrongpassword",
			wantErr:  true,
		},
		{
			name:     "non-existent file",
			file:     filepath.Join("testdata", "certificates", "does_not_exist.p12"),
			password: "changeit",
			wantErr:  true,
		},
		{
			name:     "invalid file content",
			file:     filepath.Join("testdata", "invalid_certs.pem"),
			password: "changeit",
			wantErr:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cert, err := processPKCS12(tc.file, tc.password)
			if tc.wantErr {
				require.Error(t, err)
				assert.Empty(t, cert.Certificate)
				assert.Nil(t, cert.PrivateKey)
				assert.Nil(t, cert.Leaf)
			} else {
				require.NoError(t, err)
				assert.NotEmpty(t, cert.Certificate)
				assert.NotNil(t, cert.PrivateKey)
				assert.NotNil(t, cert.Leaf)
				assert.Equal(t, cert.Leaf.Raw, cert.Certificate[0])
			}
		})
	}

	t.Run("corrupted pkcs12 file content", func(t *testing.T) {
		t.Parallel()
		tempDir := t.TempDir()
		corruptFile := filepath.Join(tempDir, "corrupt.p12")
		err := os.WriteFile(corruptFile, []byte("random corrupted data"), 0600)
		require.NoError(t, err)

		cert, err := processPKCS12(corruptFile, "changeit")
		require.Error(t, err)
		assert.Empty(t, cert.Certificate)
	})

	t.Run("pkcs12 with ca certificates chain", func(t *testing.T) {
		t.Parallel()
		// Generate CA key and certificate
		caKey, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)

		caTemplate := &x509.Certificate{
			SerialNumber:          big.NewInt(1),
			Subject:               pkix.Name{CommonName: "Test CA"},
			NotBefore:             time.Now().Add(-1 * time.Hour),
			NotAfter:              time.Now().Add(24 * time.Hour),
			IsCA:                  true,
			KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
			BasicConstraintsValid: true,
		}
		caCertBytes, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
		require.NoError(t, err)
		caCert, err := x509.ParseCertificate(caCertBytes)
		require.NoError(t, err)

		// Generate Leaf key and certificate signed by CA
		leafKey, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)

		leafTemplate := &x509.Certificate{
			SerialNumber: big.NewInt(2),
			Subject:      pkix.Name{CommonName: "Test Leaf"},
			NotBefore:    time.Now().Add(-1 * time.Hour),
			NotAfter:     time.Now().Add(24 * time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		}
		leafCertBytes, err := x509.CreateCertificate(rand.Reader, leafTemplate, caCert, &leafKey.PublicKey, caKey)
		require.NoError(t, err)
		leafCert, err := x509.ParseCertificate(leafCertBytes)
		require.NoError(t, err)

		// Encode PKCS#12 bundle with CA cert included
		p12Data, err := pkcs12.Encode(rand.Reader, leafKey, leafCert, []*x509.Certificate{caCert}, "testpass")
		require.NoError(t, err)

		tempDir := t.TempDir()
		p12File := filepath.Join(tempDir, "chain.p12")
		err = os.WriteFile(p12File, p12Data, 0600)
		require.NoError(t, err)

		cert, err := processPKCS12(p12File, "testpass")
		require.NoError(t, err)
		assert.Len(t, cert.Certificate, 2)
		assert.Equal(t, leafCert.Raw, cert.Certificate[0])
		assert.Equal(t, caCert.Raw, cert.Certificate[1])
		assert.Equal(t, leafCert.Raw, cert.Leaf.Raw)
		assert.Equal(t, leafKey, cert.PrivateKey)
	})
}
