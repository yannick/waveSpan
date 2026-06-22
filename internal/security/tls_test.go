package security

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// genCA creates a self-signed CA and returns its cert + key.
func genCA(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"},
		NotBefore: time.Unix(0, 0), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true,
	}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	cert, _ := x509.ParseCertificate(der)
	return cert, key
}

// genLeaf signs a leaf cert (server or client) with the CA.
func genLeaf(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, cn string, server bool) (certPEM, keyPEM []byte) {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Unix(0, 0), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature,
	}
	if server {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		tmpl.DNSNames = []string{"localhost"}
		tmpl.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	} else {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	keyDER, _ := x509.MarshalPKCS8PrivateKey(key)
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM
}

func writeFiles(t *testing.T, dir string, files map[string][]byte) {
	t.Helper()
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestInsecureDevModeGated(t *testing.T) {
	// no certs + dev mode -> plaintext allowed (nil config, nil error)
	cfg, err := TLSConfig{InsecureDevMode: true}.ServerTLS()
	if err != nil || cfg != nil {
		t.Fatalf("dev mode should allow plaintext: cfg=%v err=%v", cfg, err)
	}
	// no certs, no dev mode -> error (production requires mTLS)
	if _, err := (TLSConfig{}).ServerTLS(); err == nil {
		t.Fatal("plaintext without dev mode must error")
	}
}

func TestMTLSHandshake(t *testing.T) {
	dir := t.TempDir()
	ca, caKey := genCA(t)
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.Raw})
	srvCert, srvKey := genLeaf(t, ca, caKey, "server", true)
	cliCert, cliKey := genLeaf(t, ca, caKey, "client", false)
	writeFiles(t, dir, map[string][]byte{
		"ca.pem": caPEM, "srv.pem": srvCert, "srv.key": srvKey, "cli.pem": cliCert, "cli.key": cliKey,
	})

	srvCfg, err := TLSConfig{CertFile: filepath.Join(dir, "srv.pem"), KeyFile: filepath.Join(dir, "srv.key"), CAFile: filepath.Join(dir, "ca.pem")}.ServerTLS()
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) }))
	ts.TLS = srvCfg
	ts.StartTLS()
	t.Cleanup(ts.Close)

	// valid client cert -> handshake succeeds
	cliCfg, err := TLSConfig{CertFile: filepath.Join(dir, "cli.pem"), KeyFile: filepath.Join(dir, "cli.key"), CAFile: filepath.Join(dir, "ca.pem")}.ClientTLS()
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: cliCfg}}
	resp, err := client.Get(ts.URL)
	if err != nil {
		t.Fatalf("mTLS handshake with valid cert failed: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if string(body) != "ok" {
		t.Fatalf("unexpected body %q", body)
	}

	// client cert from a DIFFERENT CA -> rejected
	ca2, ca2Key := genCA(t)
	ca2PEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca2.Raw})
	badCert, badKey := genLeaf(t, ca2, ca2Key, "intruder", false)
	writeFiles(t, dir, map[string][]byte{"ca2.pem": ca2PEM, "bad.pem": badCert, "bad.key": badKey})
	badCfg, _ := TLSConfig{CertFile: filepath.Join(dir, "bad.pem"), KeyFile: filepath.Join(dir, "bad.key"), CAFile: filepath.Join(dir, "ca.pem")}.ClientTLS()
	badClient := &http.Client{Transport: &http.Transport{TLSClientConfig: badCfg}}
	if _, err := badClient.Get(ts.URL); err == nil {
		t.Fatal("client cert from an untrusted CA must be rejected by mTLS")
	}
}
