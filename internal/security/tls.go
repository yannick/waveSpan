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

// cheapHandshakeCurves prefers the fastest secure ECDHE groups (design/27). X25519 first, P-256 as
// the interop fallback — both are far cheaper per handshake than legacy larger groups.
var cheapHandshakeCurves = []tls.CurveID{tls.X25519, tls.CurveP256}

// ServerTLS builds a server *tls.Config requiring and verifying client certificates (mTLS), tuned
// for cheap handshakes (TLS 1.3, fast curves, server-side session tickets; HTTP/2 is added by
// net/http automatically). It returns
// (nil, nil) when InsecureDevMode is set and no cert is configured (plaintext is allowed only then),
// and an error if certs are missing without dev mode. Use this for machine link classes:
// gateway->data, data<->data, and gossip.
func (c TLSConfig) ServerTLS() (*tls.Config, error) {
	return c.serverTLS(tls.RequireAndVerifyClientCert)
}

// ServerTLSOptionalClient is like ServerTLS but only verifies a client certificate when one is
// presented (tls.VerifyClientCertIfGiven). It is for the admin port, which serves both machine
// operators (who present a client cert) and the human-facing embedded UI in a browser (which does
// not, and is authorised by the admin-token identity middleware instead). The channel is still
// encrypted; only the mandatory-client-cert requirement is relaxed.
func (c TLSConfig) ServerTLSOptionalClient() (*tls.Config, error) {
	return c.serverTLS(tls.VerifyClientCertIfGiven)
}

func (c TLSConfig) serverTLS(clientAuth tls.ClientAuthType) (*tls.Config, error) {
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
		Certificates:     []tls.Certificate{cert},
		ClientCAs:        pool,
		ClientAuth:       clientAuth,
		MinVersion:       tls.VersionTLS13,
		CurvePreferences: cheapHandshakeCurves,
		// HTTP/2 is negotiated automatically: net/http calls http2.ConfigureServer when the server
		// has a TLSConfig, appending "h2" to NextProtos. SessionTicketsDisabled defaults to false, so
		// the server issues TLS 1.3 session tickets and a returning client resumes without a full
		// handshake. Both are left at their defaults deliberately.
	}, nil
}

// ClientTLS builds a client *tls.Config presenting the client cert and trusting the CA, tuned to
// match ServerTLS: TLS 1.3, fast curves, and a per-identity session-resumption cache. HTTP/2 is
// enabled by the transport, not here (see ClientSessionCache note below).
func (c TLSConfig) ClientTLS() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, err
	}
	pool, err := loadCAPool(c.CAFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates:     []tls.Certificate{cert},
		RootCAs:          pool,
		MinVersion:       tls.VersionTLS13,
		CurvePreferences: cheapHandshakeCurves,
		// A session cache scoped to THIS client identity: NewHTTPClient builds one ClientTLS and
		// reuses its transport for every peer, so reconnects still resume; but two different client
		// identities never share a cache (which would let one resume the other's authenticated
		// session and skip client-cert verification). design/27 "Session resumption".
		ClientSessionCache: tls.NewLRUClientSessionCache(0),
		// HTTP/2 is enabled by the client transport via ForceAttemptHTTP2 (security.NewHTTPClient),
		// which appends "h2" to NextProtos for us; setting it here would force h2 onto raw transports
		// that cannot speak it.
	}, nil
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
