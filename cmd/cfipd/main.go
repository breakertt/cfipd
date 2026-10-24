package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/netip"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

var version, commit, buildDate = "dev", "unknown", "unknown"

func usage() {
	fmt.Fprintf(os.Stderr, `usage: cfipd <command> [flags]

daemon:
  run                     run the daemon in foreground
  test                    check the configuration and exit
  version                 print release and build information

control (talks to the running daemon over the unix socket):
  status                  overview
  actives                 list active IPs
  standbys                list standby IPs
  candidates              list recently seen candidates
  prefixes                top /24 prefix stats
  promote <ip>            force promotion test for an IP
  quarantine <ip>         cool an IP down (bounded candidate cooldown)
  scan                    trigger one discovery scan now
  relay                   relay traffic stats per active
  clients                 relay connections and traffic per client IP
  import <file>           seed candidates from cfst result.csv or IP list

web dashboard (read-only, separate process; talks to the daemon over the unix socket):
  web [-listen addr]      serve the dashboard + JSON API (default 127.0.0.1:8080);
                          web_password in the config requires a password

flags: -config <path> (default: $CFIPD_CONFIG_PATH, else ./cfipd.json or
       /etc/cfipd.json if present, else built-in defaults)
`)
	os.Exit(2)
}

// findConfig picks the config file when -config is absent: $CFIPD_CONFIG_PATH
// if set, else the first existing of ./cfipd.json and /etc/cfipd.json, else
// none (built-in defaults).
func findConfig() string {
	if p := os.Getenv("CFIPD_CONFIG_PATH"); p != "" {
		return p
	}
	for _, p := range []string{"cfipd.json", "/etc/cfipd.json"} {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if len(os.Args) < 2 {
		usage()
	}
	fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	cfgPath := fs.String("config", findConfig(), "config file path")
	listen := fs.String("listen", "127.0.0.1:8080", "web listen address (web command only)")
	fs.Parse(os.Args[2:])
	source := *cfgPath
	if source == "" {
		source = "built-in defaults"
	}
	if os.Args[1] == "test" {
		if fs.NArg() > 0 {
			fmt.Fprintln(os.Stderr, "cfipd test takes no arguments; name the file with -config PATH")
			os.Exit(2)
		}
		if _, err := loadConfig(*cfgPath, true); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Printf("config %s: ok\n", source)
		return
	}

	cfg, err := loadConfig(*cfgPath, false)
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	if cmd := os.Args[1]; cmd == "run" || cmd == "web" {
		log.Printf("config: %s", source)
		if _, err := loadConfig(*cfgPath, true); err != nil {
			log.Printf("config warning: %v (continuing; cfipd test shows the same)", err)
		}
	}

	switch os.Args[1] {
	case "version":
		fmt.Printf("cfipd %s (commit %s, built %s)\n", version, commit, buildDate)
	case "run":
		store, err := OpenStore(cfg.DataDir)
		if err != nil {
			log.Fatalf("store: %v", err)
		}
		defer store.Close()
		d := newDaemon(cfg, store)
		if err := store.Load(d.pool, d.scan); err != nil {
			log.Printf("load state: %v (starting fresh)", err)
		}
		// Persist migration bounds and the PK observation deadline immediately,
		// not 5 minutes later or only at graceful shutdown.
		if err := store.Save(d.pool, d.scan); err != nil {
			log.Printf("save after load: %v", err)
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		if err := d.run(ctx); err != nil {
			log.Fatalf("run: %v", err)
		}

	case "status", "actives", "standbys", "candidates", "prefixes", "scan", "relay", "clients":
		ctlPrint(cfg.Socket, "/"+os.Args[1], "")

	case "web":
		auth := "off"
		if cfg.WebPassword != "" {
			auth = "on"
		}
		log.Printf("web dashboard on http://%s (daemon socket %s, password %s)", *listen, cfg.Socket, auth)
		if err := serveWeb(cfg.Socket, *listen, cfg.WebPassword); err != nil {
			log.Fatalf("web: %v", err)
		}

	case "import":
		if fs.NArg() < 1 {
			log.Fatal("usage: cfipd import <file>   # cfst result.csv or plain IP list")
		}
		importFile(cfg.Socket, fs.Arg(0))

	case "promote", "quarantine":
		if fs.NArg() < 1 {
			log.Fatalf("usage: cfipd %s <ip>", os.Args[1])
		}
		ctlPrint(cfg.Socket, "/"+os.Args[1], fs.Arg(0))

	default:
		usage()
	}
}

func ctlPrint(sock, path, ip string) {
	raw, err := ctlGet(sock, path, ip)
	if err != nil {
		log.Fatalf("ctl: %v", err)
	}
	switch path {
	case "/actives", "/standbys", "/candidates":
		if err := printTable(raw); err != nil {
			log.Fatalf("%v", err)
		}
	default:
		var v any
		if err := json.Unmarshal(raw, &v); err == nil {
			if pretty, err := json.MarshalIndent(v, "", "  "); err == nil {
				fmt.Println(string(pretty))
				return
			}
		}
		fmt.Printf("%s", raw)
	}
}

// importFile seeds candidates from a cfst result.csv or a plain IP list.
func importFile(sock, path string) {
	b, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("import: %v", err)
	}
	added, exists := 0, 0
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.TrimSpace(strings.Split(line, ",")[0])
		if _, err := netip.ParseAddr(f); err != nil {
			continue
		}
		raw, err := ctlGet(sock, "/add", f)
		if err != nil {
			log.Printf("add %s: %v", f, err)
			continue
		}
		if strings.Contains(string(raw), "added") {
			added++
		} else {
			exists++
		}
	}
	fmt.Printf("imported %d new, %d already known\n", added, exists)
}
