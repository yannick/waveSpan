package rpcopts

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// testCA builds a self-signed CA and issues one leaf usable as both server and client cert
// (SAN 127.0.0.1), returning matching server and client tls.Configs for mutual TLS.
func testMTLSConfigs(t *testing.T) (server, client *tls.Config) {
	t.Helper()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTpl, caTpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, _ := x509.ParseCertificate(caDER)

	leafKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	leafTpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "test-node"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTpl, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	leaf := tls.Certificate{Certificate: [][]byte{leafDER}, PrivateKey: leafKey}
	pool := x509.NewCertPool()
	pool.AddCert(caCert)

	server = &tls.Config{
		Certificates: []tls.Certificate{leaf}, ClientCAs: pool,
		ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13,
	}
	client = &tls.Config{
		Certificates: []tls.Certificate{leaf}, RootCAs: pool, MinVersion: tls.VersionTLS13,
	}
	return server, client
}

// TestGRPCConnMTLS proves the pooled client actually completes a mutual-TLS handshake against a
// client-cert-requiring server (design/37 P1.6 — previously GRPCConn dialed plaintext and could
// not connect to an mTLS server at all). codes.Unimplemented means the RPC crossed the encrypted
// channel and reached dispatch; a handshake failure surfaces as Unavailable.
func TestGRPCConnMTLS(t *testing.T) {
	serverTLS, clientTLS := testMTLSConfigs(t)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	// Swap in fresh package state: the pool is global and ConfigureDialTLS refuses to run after
	// other tests have dialed.
	grpcConnMu.Lock()
	oldPool, oldTLS := grpcConnPool, dialTLS
	grpcConnPool, dialTLS = map[string]*grpc.ClientConn{}, nil
	grpcConnMu.Unlock()
	t.Cleanup(func() {
		grpcConnMu.Lock()
		grpcConnPool, dialTLS = oldPool, oldTLS
		grpcConnMu.Unlock()
	})

	ConfigureDialTLS(clientTLS)
	cc, err := GRPCConn(lis.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = cc.Invoke(ctx, "/wavespan.test.NoSuchService/NoSuchMethod", &wavespanv1.GetMembershipRequest{}, &wavespanv1.GetMembershipResponse{})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("RPC over mTLS = %v (code %v), want Unimplemented (proves the handshake + channel)", err, status.Code(err))
	}
}

// TestConfigureDialTLSRefusesLateInstall pins the misuse guard: installing TLS after a conn was
// already dialed would leave a silently-plaintext pooled conn behind, so it must panic.
func TestConfigureDialTLSRefusesLateInstall(t *testing.T) {
	grpcConnMu.Lock()
	oldPool, oldTLS := grpcConnPool, dialTLS
	grpcConnPool, dialTLS = map[string]*grpc.ClientConn{}, nil
	grpcConnMu.Unlock()
	t.Cleanup(func() {
		grpcConnMu.Lock()
		grpcConnPool, dialTLS = oldPool, oldTLS
		grpcConnMu.Unlock()
	})

	if _, err := GRPCConn("127.0.0.1:60009"); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("ConfigureDialTLS after a dial did not panic")
		}
	}()
	ConfigureDialTLS(&tls.Config{})
}
