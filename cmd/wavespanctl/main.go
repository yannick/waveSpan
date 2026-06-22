// Command wavespanctl is the WaveSpan admin/client CLI. It talks to a data pod's KvService over
// Connect (data port, default :7800) and reads admin endpoints over HTTP (admin port, :7900).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"connectrpc.com/connect"
	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
	"github.com/cwire/wavespan/proto/wavespan/v1/wavespanv1connect"
)

// version is overridden at build time via -ldflags "-X main.version=...".
var version = "0.0.0-dev"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "wavespanctl:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		usage()
		return fmt.Errorf("no command")
	}
	switch args[0] {
	case "version":
		fmt.Printf("wavespanctl %s\n", version)
		return nil
	case "kv":
		return kvCmd(args[1:])
	case "members":
		return membersCmd(args[1:])
	case "-h", "--help", "help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func kvClient(addr string) wavespanv1connect.KvServiceClient {
	return wavespanv1connect.NewKvServiceClient(http.DefaultClient, "http://"+addr)
}

func kvCmd(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: wavespanctl kv <put|get|delete|scan>")
	}
	fs := flag.NewFlagSet("kv", flag.ContinueOnError)
	addr := fs.String("addr", "localhost:7800", "data-port address host:port")
	ttl := fs.Int64("ttl", 0, "TTL in milliseconds (put only; 0 = none)")
	mode := fs.String("mode", "cache-fast", "scan mode: cache-fast|routed|cache-complete|local")
	sub := args[0]
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	rest := fs.Args()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c := kvClient(*addr)

	switch sub {
	case "put":
		if len(rest) != 3 {
			return fmt.Errorf("usage: wavespanctl kv put <namespace> <key> <value>")
		}
		req := &wavespanv1.PutRequest{Namespace: rest[0], Key: []byte(rest[1]), Value: []byte(rest[2]), RequireOriginPlusOne: true}
		if *ttl > 0 {
			req.TtlMs = ttl
		}
		resp, err := c.Put(ctx, connect.NewRequest(req))
		if err != nil {
			return err
		}
		fmt.Printf("ok  acked_nearby_replicas=%d version=%s\n", resp.Msg.GetAckedNearbyReplicas(), verStr(resp.Msg.GetVersion()))
		return nil
	case "get":
		if len(rest) != 2 {
			return fmt.Errorf("usage: wavespanctl kv get <namespace> <key>")
		}
		resp, err := c.Get(ctx, connect.NewRequest(&wavespanv1.GetRequest{Namespace: rest[0], Key: []byte(rest[1])}))
		if err != nil {
			return err
		}
		if !resp.Msg.GetFound() {
			fmt.Printf("(not found)  source=%s\n", resp.Msg.GetMeta().GetSource())
			return nil
		}
		fmt.Printf("%s\n# source=%s version=%s\n", resp.Msg.GetValue(), resp.Msg.GetMeta().GetSource(), verStr(resp.Msg.GetMeta().GetObservedVersion()))
		return nil
	case "delete":
		if len(rest) != 2 {
			return fmt.Errorf("usage: wavespanctl kv delete <namespace> <key>")
		}
		resp, err := c.Delete(ctx, connect.NewRequest(&wavespanv1.DeleteRequest{Namespace: rest[0], Key: []byte(rest[1]), RequireOriginPlusOne: true}))
		if err != nil {
			return err
		}
		fmt.Printf("ok  acked_nearby_replicas=%d\n", resp.Msg.GetAckedNearbyReplicas())
		return nil
	case "scan":
		if len(rest) != 1 {
			return fmt.Errorf("usage: wavespanctl kv scan <namespace>")
		}
		stream, err := c.Scan(ctx, connect.NewRequest(&wavespanv1.ScanRequest{Namespace: rest[0], Mode: scanMode(*mode)}))
		if err != nil {
			return err
		}
		for stream.Receive() {
			switch m := stream.Msg().Msg.(type) {
			case *wavespanv1.ScanResponse_Header:
				fmt.Printf("# mode=%s completeness=%s\n", m.Header.GetMode(), m.Header.GetCompleteness())
			case *wavespanv1.ScanResponse_Row:
				fmt.Printf("%s\t%s\n", m.Row.GetKey(), m.Row.GetValue())
			case *wavespanv1.ScanResponse_Trailer:
				fmt.Printf("# rows=%d completeness=%s\n", m.Trailer.GetRowsReturned(), m.Trailer.GetFinalCompleteness())
			}
		}
		return stream.Err()
	default:
		return fmt.Errorf("unknown kv subcommand %q", sub)
	}
}

func scanMode(s string) wavespanv1.ScanMode {
	switch s {
	case "routed":
		return wavespanv1.ScanMode_ROUTED_EVENTUAL
	case "cache-complete":
		return wavespanv1.ScanMode_CACHE_COMPLETE
	case "local":
		return wavespanv1.ScanMode_LOCAL_ONLY
	default:
		return wavespanv1.ScanMode_CACHE_FAST
	}
}

func verStr(v *wavespanv1.Version) string {
	if v == nil {
		return "-"
	}
	return strconv.FormatUint(v.GetHlcPhysicalMs(), 10) + "." + strconv.FormatUint(uint64(v.GetHlcLogical()), 10) + "@" + v.GetWriterMemberId()
}

func membersCmd(args []string) error {
	fs := flag.NewFlagSet("members", flag.ContinueOnError)
	addr := fs.String("admin", "localhost:7900", "admin-port address host:port")
	if err := fs.Parse(args); err != nil {
		return err
	}
	resp, err := http.Get("http://" + *addr + "/admin/membership")
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	var pretty any
	if json.Unmarshal(body, &pretty) == nil {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(pretty)
	}
	fmt.Println(string(body))
	return nil
}

func usage() {
	fmt.Fprint(os.Stderr, `usage: wavespanctl <command>

commands:
  version                                   print the client version
  kv put <ns> <key> <value> [--addr] [--ttl ms]
  kv get <ns> <key> [--addr]
  kv delete <ns> <key> [--addr]
  kv scan <ns> [--addr] [--mode cache-fast|routed|cache-complete|local]
  members [--admin host:port]               show cluster membership

flags:
  --addr   data-port address (default localhost:7800)
  --admin  admin-port address (default localhost:7900)
`)
}
