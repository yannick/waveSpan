package security

import (
	"crypto/tls"
	"encoding/pem"
	"net/http"
	"path/filepath"
	"testing"
)

func TestNewHTTPClientDevPlaintext(t *testing.T) {
	// insecureDevMode + no certs -> a pooled plaintext client (no TLS config), same reachability as
	// before, but with keepalive/pooling applied.
	client, err := TLSConfig{InsecureDevMode: true}.NewHTTPClient(DefaultTransportTuning())
	if err != nil {
		t.Fatalf("dev-mode client should build: %v", err)
	}
	tr, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("expected *http.Transport, got %T", client.Transport)
	}
	if tr.TLSClientConfig != nil {
		t.Fatal("dev-mode plaintext client must not carry a TLS config")
	}
	if tr.MaxIdleConnsPerHost != DefaultTransportTuning().MaxIdleConnsPerHost {
		t.Fatalf("pooling not applied: MaxIdleConnsPerHost=%d", tr.MaxIdleConnsPerHost)
	}
	if !tr.ForceAttemptHTTP2 {
		t.Fatal("HTTP/2 should be attempted for multiplexing")
	}
	if client.Timeout != 0 {
		t.Fatal("no blanket Client.Timeout: long-lived subscription streams must not be cut")
	}
}

func TestNewHTTPClientRequiresDevModeOrCerts(t *testing.T) {
	// no certs and not dev mode -> refuse to build a plaintext client (production must use mTLS).
	if _, err := (TLSConfig{}).NewHTTPClient(DefaultTransportTuning()); err == nil {
		t.Fatal("plaintext client without dev mode must error")
	}
}

func TestNewHTTPClientMTLSResumption(t *testing.T) {
	dir := t.TempDir()
	ca, caKey := genCA(t)
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.Raw})
	cliCert, cliKey := genLeaf(t, ca, caKey, "client", false)
	writeFiles(t, dir, map[string][]byte{"ca.pem": caPEM, "cli.pem": cliCert, "cli.key": cliKey})

	c := TLSConfig{CertFile: filepath.Join(dir, "cli.pem"), KeyFile: filepath.Join(dir, "cli.key"), CAFile: filepath.Join(dir, "ca.pem")}
	client, err := c.NewHTTPClient(DefaultTransportTuning())
	if err != nil {
		t.Fatalf("mTLS client should build: %v", err)
	}
	tr := client.Transport.(*http.Transport)
	if tr.TLSClientConfig == nil {
		t.Fatal("mTLS client must carry a TLS config")
	}
	if tr.TLSClientConfig.ClientSessionCache == nil {
		t.Fatal("session resumption cache must be set so reconnects skip the full handshake")
	}
	if tr.TLSClientConfig.MinVersion != tls.VersionTLS13 {
		t.Fatalf("want TLS 1.3 floor, got %x", tr.TLSClientConfig.MinVersion)
	}
}

func TestServerTLSCheapHandshakeSettings(t *testing.T) {
	dir := t.TempDir()
	ca, caKey := genCA(t)
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.Raw})
	srvCert, srvKey := genLeaf(t, ca, caKey, "server", true)
	writeFiles(t, dir, map[string][]byte{"ca.pem": caPEM, "srv.pem": srvCert, "srv.key": srvKey})
	c := TLSConfig{CertFile: filepath.Join(dir, "srv.pem"), KeyFile: filepath.Join(dir, "srv.key"), CAFile: filepath.Join(dir, "ca.pem")}

	cfg, err := c.ServerTLS()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatal("data/gossip server must require+verify client certs (mTLS)")
	}
	if cfg.SessionTicketsDisabled {
		t.Fatal("server session tickets must stay enabled for resumption")
	}
	if len(cfg.CurvePreferences) == 0 || cfg.CurvePreferences[0] != tls.X25519 {
		t.Fatalf("fastest curve should be preferred first: %v", cfg.CurvePreferences)
	}

	// Admin/browser variant only verifies a cert when one is presented.
	adminCfg, err := c.ServerTLSOptionalClient()
	if err != nil {
		t.Fatal(err)
	}
	if adminCfg.ClientAuth != tls.VerifyClientCertIfGiven {
		t.Fatalf("admin port should not force a client cert (browser UI), got %v", adminCfg.ClientAuth)
	}
}
