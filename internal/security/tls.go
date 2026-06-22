package security

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
)

// TLSConfig describes the mTLS material (mounted from Secrets in K8s, a dev CA in Docker; design/15).
type TLSConfig struct {
	CertFile        string
	KeyFile         string
	CAFile          string
	InsecureDevMode bool
}

// ErrPlaintextRequiresDevMode is returned when TLS material is absent and dev mode is off.
var ErrPlaintextRequiresDevMode = errors.New("security: plaintext transport requires security.insecureDevMode=true")

// ServerTLS builds a server *tls.Config requiring and verifying client certificates (mTLS). It
// returns (nil, nil) when InsecureDevMode is set and no cert is configured (plaintext is allowed
// only then), and an error if certs are missing without dev mode.
func (c TLSConfig) ServerTLS() (*tls.Config, error) {
	if c.CertFile == "" || c.KeyFile == "" || c.CAFile == "" {
		if c.InsecureDevMode {
			return nil, nil
		}
		return nil, ErrPlaintextRequiresDevMode
	}
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, err
	}
	pool, err := loadCAPool(c.CAFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert, // mTLS: every client must present a trusted cert
		MinVersion:   tls.VersionTLS13,
	}, nil
}

// ClientTLS builds a client *tls.Config presenting the client cert and trusting the CA.
func (c TLSConfig) ClientTLS() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, err
	}
	pool, err := loadCAPool(c.CAFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: pool, MinVersion: tls.VersionTLS13}, nil
}

func loadCAPool(caFile string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, errors.New("security: no CA certificates parsed from " + caFile)
	}
	return pool, nil
}
