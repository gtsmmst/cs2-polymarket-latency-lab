// gotv_watcher.go — follows a live CS2 CSTV+ (GOTV) HTTP broadcast and POSTs
// game events (kills, round ends, bomb events) to the Python ingest sidecar
// (watchers/gotv_ingest.py), which stamps them into the SQLite events bus.
//
// NOTE: a custom io.Reader is used instead of the library's NewCSTVBroadcastParser,
// because some broadcast hosts sit behind Cloudflare and reject the default
// Go-http-client User-Agent. A browser User-Agent is sent on every request.
//
// Usage:
//
//	go run gotv_watcher.go -url "https://<cstv-host>/<broadcast-path>" -match 12345 -v
//
// The broadcast URL is the same one you would pass to `playcast` in the CS2 console.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	demoinfocs "github.com/markus-wa/demoinfocs-golang/v5/pkg/demoinfocs"
	common "github.com/markus-wa/demoinfocs-golang/v5/pkg/demoinfocs/common"
	events "github.com/markus-wa/demoinfocs-golang/v5/pkg/demoinfocs/events"
)

const browserUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

var (
	urlFlag     = flag.String("url", "", "CSTV+ broadcast URL (required)")
	ingestFlag  = flag.String("ingest", "http://127.0.0.1:8766/api/ingest_gotv", "Python ingest URL")
	matchIDFlag = flag.String("match", "", "match_id label stored with every event (optional)")
	verboseFlag = flag.Bool("v", false, "verbose: print every event to stdout")
)

var ingestClient = &http.Client{Timeout: 2 * time.Second}

// =====================================================================
// HTTP helpers with a browser User-Agent
// =====================================================================

var cstvClient = &http.Client{Timeout: 15 * time.Second}

func httpGetUA(rawURL string) (*http.Response, error) {
	req, err := http.NewRequest("GET", rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", browserUA)
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	return cstvClient.Do(req)
}

// =====================================================================
// CSTV reader — mirrors the library's broadcast reader, but sets the UA
// on every request. Protocol: /sync -> /{signup}/start -> /{frag}/full ->
// /{frag}/delta, /{frag+1}/delta, ...
// =====================================================================

type cstvSync struct {
	Tick           int     `json:"tick"`
	EndTick        int     `json:"endtick"`
	MaxTick        int     `json:"maxtick"`
	RtDelay        float64 `json:"rtdelay"`
	RcvAge         float64 `json:"rcvage"`
	Fragment       int     `json:"fragment"`
	SignupFragment int     `json:"signup_fragment"`
	Tps            int     `json:"tps"`
	Map            string  `json:"map"`
	TokenRedirect  string  `json:"token_redirect"`
}

type cstvReader struct {
	baseURL string
	frag    int
	buf     bytes.Buffer
	timeout time.Duration
}

func newCSTVReader(rawURL string, timeout time.Duration) (*cstvReader, error) {
	rawURL = strings.TrimRight(rawURL, "/")

	// 1) /sync
	syncURL := rawURL + "/sync"
	log.Printf("CSTV: GET %s", syncURL)
	resp, err := httpGetUA(syncURL)
	if err != nil {
		return nil, fmt.Errorf("sync GET: %w", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("sync returned %d (body: %.200s)", resp.StatusCode, string(body))
	}

	// Make sure this is JSON and not a Cloudflare challenge page.
	if len(body) > 0 && (body[0] == '<' || bytes.Contains(body[:min(200, len(body))], []byte("<html"))) {
		return nil, fmt.Errorf("sync returned HTML instead of JSON (Cloudflare block?), first 200 bytes: %.200s", string(body))
	}

	var s cstvSync
	if err := json.Unmarshal(body, &s); err != nil {
		return nil, fmt.Errorf("sync JSON parse: %w (body: %.200s)", err, string(body))
	}

	log.Printf("CSTV sync OK: map=%s tick=%d fragment=%d rtdelay=%.2fs tps=%d token_redirect=%s",
		s.Map, s.Tick, s.Fragment, s.RtDelay, s.Tps, s.TokenRedirect)

	// 2) base + token_redirect
	if s.TokenRedirect != "" {
		joined, err := url.JoinPath(rawURL, s.TokenRedirect)
		if err != nil {
			return nil, fmt.Errorf("join token_redirect: %w", err)
		}
		rawURL = joined
		log.Printf("CSTV: redirected base = %s", rawURL)
	}

	r := &cstvReader{
		baseURL: rawURL,
		frag:    s.Fragment,
		timeout: timeout,
	}

	// 3) /start
	startURL := fmt.Sprintf("%s/%d/start", rawURL, s.SignupFragment)
	log.Printf("CSTV: GET %s", startURL)
	startResp, err := httpGetUA(startURL)
	if err != nil {
		return nil, fmt.Errorf("start GET: %w", err)
	}
	n, err := io.Copy(&r.buf, startResp.Body)
	startResp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("start body read: %w", err)
	}
	log.Printf("CSTV start: %d bytes (status %d)", n, startResp.StatusCode)

	// 4) /full
	fullURL := fmt.Sprintf("%s/%d/full", rawURL, s.Fragment)
	log.Printf("CSTV: GET %s", fullURL)
	fullResp, err := httpGetUA(fullURL)
	if err != nil {
		return nil, fmt.Errorf("full GET: %w", err)
	}
	n2, err := io.Copy(&r.buf, fullResp.Body)
	fullResp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("full body read: %w", err)
	}
	log.Printf("CSTV full: %d bytes (status %d). Total buffered: %d", n2, fullResp.StatusCode, r.buf.Len())

	return r, nil
}

func (c *cstvReader) Read(p []byte) (n int, err error) {
	n, err = c.buf.Read(p)
	nFails := 0
	backoff := time.Second

	for n < len(p) && errors.Is(err, io.EOF) {
		deltaURL := fmt.Sprintf("%s/%d/delta", c.baseURL, c.frag)
		resp, gerr := httpGetUA(deltaURL)
		if gerr != nil {
			return n, fmt.Errorf("delta GET %s: %w", deltaURL, gerr)
		}

		if resp.StatusCode != 200 {
			resp.Body.Close()
			time.Sleep(backoff)
			backoff = time.Duration(float64(backoff) * 1.5)
			nFails++
			if nFails == 8 || backoff > c.timeout {
				return n, fmt.Errorf("%w: end of CSTV stream after %d delta fails", io.EOF, nFails)
			}
			continue
		}

		written, cerr := io.Copy(&c.buf, resp.Body)
		resp.Body.Close()
		if cerr != nil {
			return n, fmt.Errorf("delta body: %w", cerr)
		}
		if written == 0 {
			// empty fragment — wait for the next one
			time.Sleep(500 * time.Millisecond)
			continue
		}

		c.frag++
		backoff = time.Second

		n2, _ := c.buf.Read(p[n:])
		n += n2
		nFails = 0
	}

	if errors.Is(err, io.EOF) {
		err = nil
	}
	return n, err
}

// =====================================================================
// Sending events to the Python ingest sidecar
// =====================================================================

type ingestEvent struct {
	Kind    string         `json:"kind"`
	MatchID string         `json:"match_id,omitempty"`
	Tick    int            `json:"tick"`
	Payload map[string]any `json:"payload"`
}

func send(kind string, payload map[string]any, tick int) {
	ev := ingestEvent{Kind: kind, MatchID: *matchIDFlag, Tick: tick, Payload: payload}
	body, err := json.Marshal(ev)
	if err != nil {
		log.Printf("marshal: %v", err)
		return
	}
	resp, err := ingestClient.Post(*ingestFlag, "application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf("ingest POST failed (%s): %v", kind, err)
		return
	}
	resp.Body.Close()
	if *verboseFlag {
		fmt.Printf("[gotv] %s %v\n", kind, payload)
	}
}

func formatPlayer(p *common.Player) string {
	if p == nil {
		return "?"
	}
	switch p.Team {
	case common.TeamTerrorists:
		return "[T]" + p.Name
	case common.TeamCounterTerrorists:
		return "[CT]" + p.Name
	}
	return p.Name
}

func teamString(t common.Team) string {
	switch t {
	case common.TeamTerrorists:
		return "T"
	case common.TeamCounterTerrorists:
		return "CT"
	}
	return "?"
}

// =====================================================================
// main
// =====================================================================

func main() {
	flag.Parse()

	if *urlFlag == "" {
		fmt.Println("Error: -url <CSTV+ broadcast URL> is required")
		fmt.Println(`Example: gotv_watcher -url "https://<cstv-host>/<broadcast-path>" -match 12345`)
		os.Exit(2)
	}

	log.Printf("CSTV+ watcher starting. url=%s ingest=%s match=%s", *urlFlag, *ingestFlag, *matchIDFlag)

	reader, err := newCSTVReader(*urlFlag, 60*time.Second)
	if err != nil {
		log.Fatalf("CSTV reader init failed: %v", err)
	}
	log.Printf("CSTV reader ready, starting demoinfocs parser (format=CSTVBroadcast)...")

	// NewParserWithConfig with Format=DemoFormatCSTVBroadcast is required:
	// a broadcast stream has no HL2DEMO magic header at the start.
	cfg := demoinfocs.DefaultParserConfig
	cfg.Format = demoinfocs.DemoFormatCSTVBroadcast
	cfg.CSTVTimeout = 60 * time.Second
	p := demoinfocs.NewParserWithConfig(reader, cfg)
	defer p.Close()

	// Match started
	p.RegisterEventHandler(func(e events.MatchStart) {
		send("match_start", map[string]any{}, p.GameState().IngameTick())
	})

	// Kill
	p.RegisterEventHandler(func(e events.Kill) {
		gs := p.GameState()
		payload := map[string]any{
			"killer":     formatPlayer(e.Killer),
			"victim":     formatPlayer(e.Victim),
			"weapon":     fmt.Sprintf("%v", e.Weapon),
			"is_hs":      e.IsHeadshot,
			"penetrated": e.PenetratedObjects > 0,
			"score_ct":   gs.TeamCounterTerrorists().Score(),
			"score_t":    gs.TeamTerrorists().Score(),
		}
		if e.Killer != nil {
			payload["killer_team"] = teamString(e.Killer.Team)
		}
		if e.Victim != nil {
			payload["victim_team"] = teamString(e.Victim.Team)
		}
		send("kill", payload, gs.IngameTick())
	})

	// Round end
	p.RegisterEventHandler(func(e events.RoundEnd) {
		gs := p.GameState()
		ct := gs.TeamCounterTerrorists().Score()
		t := gs.TeamTerrorists().Score()
		switch e.Winner {
		case common.TeamCounterTerrorists:
			ct++
		case common.TeamTerrorists:
			t++
		}
		send("round_end", map[string]any{
			"winner":    teamString(e.Winner),
			"reason":    fmt.Sprintf("%v", e.Reason),
			"score_ct":  ct,
			"score_t":   t,
			"round_num": ct + t,
		}, gs.IngameTick())
	})

	// Bomb events
	p.RegisterEventHandler(func(e events.BombPlanted) {
		gs := p.GameState()
		payload := map[string]any{"site": fmt.Sprintf("%v", e.Site)}
		if e.Player != nil {
			payload["planter"] = formatPlayer(e.Player)
		}
		send("bomb_planted", payload, gs.IngameTick())
	})
	p.RegisterEventHandler(func(e events.BombDefused) {
		gs := p.GameState()
		payload := map[string]any{"site": fmt.Sprintf("%v", e.Site)}
		if e.Player != nil {
			payload["defuser"] = formatPlayer(e.Player)
		}
		send("bomb_defused", payload, gs.IngameTick())
	})
	p.RegisterEventHandler(func(e events.BombExplode) {
		gs := p.GameState()
		send("bomb_exploded", map[string]any{"site": fmt.Sprintf("%v", e.Site)}, gs.IngameTick())
	})

	// Heartbeat (every 5 s) so the dashboard can tell the parser is alive
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			gs := p.GameState()
			if gs == nil {
				continue
			}
			send("tick", map[string]any{
				"ingame_tick": gs.IngameTick(),
				"score_ct":    gs.TeamCounterTerrorists().Score(),
				"score_t":     gs.TeamTerrorists().Score(),
			}, gs.IngameTick())
		}
	}()

	log.Println("Parsing broadcast — Ctrl+C to stop")
	if err := p.ParseToEnd(); err != nil {
		log.Fatalf("ParseToEnd failed: %v", err)
	}
	log.Println("Broadcast ended")
}
