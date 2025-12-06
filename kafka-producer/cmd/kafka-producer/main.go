package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type producerConfig struct {
	logFile   string
	interval  time.Duration
	batchSize int
	clientID  string
	stdout    bool
}

type logEvent struct {
	Timestamp time.Time `json:"@timestamp"`
	Level     string    `json:"level"`
	App       string    `json:"app"`
	Message   string    `json:"message"`
	Path      string    `json:"path"`
	Status    int       `json:"status"`
	LatencyMS int       `json:"latency_ms"`
	Host      string    `json:"host"`
	RequestID string    `json:"request_id"`
	UserID    string    `json:"user_id"`
}

func main() {
	cfg := parseConfig()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	logFile, writer, err := openLogFile(cfg.logFile)
	if err != nil {
		logger.Error("cannot open log file", "path", cfg.logFile, "error", err)
		os.Exit(1)
	}
	defer logFile.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	logger.Info("log generator started",
		"logFile", cfg.logFile,
		"interval", cfg.interval,
		"batch", cfg.batchSize,
		"stdout", cfg.stdout,
	)

	ticker := time.NewTicker(cfg.interval)
	defer ticker.Stop()

	var counter int64
	for {
		select {
		case <-ctx.Done():
			writer.Flush()
			logger.Info("shutting down generator")
			return
		case t := <-ticker.C:
			events := buildBatch(cfg.batchSize, counter, t)
			if err := writeBatch(writer, events, cfg.stdout); err != nil {
				logger.Error("failed to write batch", "error", err)
			} else {
				logger.Info("batch written",
					"count", len(events),
					"logFile", cfg.logFile,
					"ts", t.Format(time.RFC3339),
				)
			}
			counter += int64(len(events))
		}
	}
}

func parseConfig() producerConfig {
	logFile := flag.String("log-file", envOrDefault("LOG_FILE", "./logs/app.log"), "path to write generated logs")
	interval := flag.Duration("interval", envOrDuration("INTERVAL", 500*time.Millisecond), "how often to write a batch")
	batch := flag.Int("batch", envOrInt("BATCH", 10), "messages per batch")
	clientID := flag.String("client-id", envOrDefault("CLIENT_ID", "kafka-producer"), "client identifier for logs")
	stdout := flag.Bool("stdout", envOrBool("STDOUT", false), "also print to stdout")
	flag.Parse()

	return producerConfig{
		logFile:   *logFile,
		interval:  *interval,
		batchSize: *batch,
		clientID:  *clientID,
		stdout:    *stdout,
	}
}

func openLogFile(path string) (*os.File, *bufio.Writer, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, nil, err
	}
	return f, bufio.NewWriterSize(f, 1<<20), nil
}

func writeBatch(w *bufio.Writer, events []logEvent, stdout bool) error {
	for _, ev := range events {
		payload, err := json.Marshal(ev)
		if err != nil {
			return err
		}
		if _, err := w.Write(payload); err != nil {
			return err
		}
		if err := w.WriteByte('\n'); err != nil {
			return err
		}
		if stdout {
			fmt.Println(string(payload))
		}
	}
	return w.Flush()
}

func buildBatch(size int, counter int64, now time.Time) []logEvent {
	if size <= 0 {
		size = 1
	}
	events := make([]logEvent, size)
	for i := 0; i < size; i++ {
		events[i] = mockLog(counter+int64(i), now)
	}
	return events
}

func mockLog(seq int64, now time.Time) logEvent {
	levels := []string{"info", "debug", "warn", "error"}
	apps := []string{"checkout", "auth", "search", "billing"}
	paths := []string{"/api/v1/login", "/api/v1/checkout", "/api/v1/search", "/api/v1/payments"}
	hosts := []string{"api-1", "api-2", "api-3"}

	return logEvent{
		Timestamp: now,
		Level:     levels[rand.Intn(len(levels))],
		App:       apps[rand.Intn(len(apps))],
		Message:   randomMessage(),
		Path:      paths[rand.Intn(len(paths))],
		Status:    sampleStatus(),
		LatencyMS: 20 + rand.Intn(400),
		Host:      hosts[rand.Intn(len(hosts))],
		RequestID: fmt.Sprintf("req-%d", seq),
		UserID:    fmt.Sprintf("user-%03d", rand.Intn(500)),
	}
}

func randomMessage() string {
	msgs := []string{
		"processed request",
		"validation failed",
		"cache hit",
		"cache miss, querying db",
		"calling downstream service",
		"retrying after backoff",
		"success",
	}
	return msgs[rand.Intn(len(msgs))]
}

func sampleStatus() int {
	choices := []int{200, 200, 200, 201, 202, 204, 400, 401, 404, 500}
	return choices[rand.Intn(len(choices))]
}

func envOrDefault(key, def string) string {
	if val := strings.TrimSpace(os.Getenv(key)); val != "" {
		return val
	}
	return def
}

func envOrDuration(key string, def time.Duration) time.Duration {
	if val := strings.TrimSpace(os.Getenv(key)); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
	}
	return def
}

func envOrInt(key string, def int) int {
	if val := strings.TrimSpace(os.Getenv(key)); val != "" {
		if v, err := strconv.Atoi(val); err == nil {
			return v
		}
	}
	return def
}

func envOrBool(key string, def bool) bool {
	if val := strings.TrimSpace(os.Getenv(key)); val != "" {
		switch strings.ToLower(val) {
		case "1", "true", "yes", "y", "on":
			return true
		case "0", "false", "no", "n", "off":
			return false
		}
	}
	return def
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
