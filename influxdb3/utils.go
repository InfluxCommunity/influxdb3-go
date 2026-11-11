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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"software.sslmate.com/src/go-pkcs12"
)

func loadX509CertPool(certPath string) (*x509.CertPool, error) {
	certPool, err := x509.SystemCertPool()
	if err != nil {
		return nil, fmt.Errorf("x509: %w", err)
	}

	// #nosec G304
	certs, err := os.ReadFile(certPath)
	if err != nil {
		return nil, fmt.Errorf("error reading %s: %w", certPath, err)
	}

	ok := certPool.AppendCertsFromPEM(certs)
	if !ok {
		slog.Warn("No valid certificates found in " + certPath)
	}
	return certPool, nil
}

func loadCertificate(certFile string, keyFile string, password string) (tls.Certificate, error) {
	if strings.HasSuffix(certFile, ".p12") {
		cert, err := processPKCS12(certFile, password)
		if err != nil {
			return tls.Certificate{}, err
		}
		return cert, nil
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return tls.Certificate{}, err
	}
	return cert, nil
}

func processPKCS12(path string, password string) (tls.Certificate, error) {
	// #nosec G304
	data, err := os.ReadFile(path)
	if err != nil {
		return tls.Certificate{}, err
	}
	privateKey, cert, caCerts, err := pkcs12.DecodeChain(data, password)
	if err != nil {
		return tls.Certificate{}, err
	}

	certBytes := [][]byte{cert.Raw}
	for _, ca := range caCerts {
		certBytes = append(certBytes, ca.Raw)
	}

	return tls.Certificate{
		Certificate: certBytes,
		PrivateKey:  privateKey,
		Leaf:        cert,
	}, nil
}
