package es

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/kevwan/go-stash/stash/config"
	"github.com/olivere/elastic/v7"
	"github.com/rogpeppe/go-internal/semver"
)

const es8Version = "8.0.0"

type (
	Writer struct {
		docType      string
		esVersion    string
		client       *elastic.Client
		maxChunkSize int
		logger       *slog.Logger

		mu            sync.Mutex
		buffer        []valueWithIndex
		bufferSize    int
		flushInterval time.Duration
		stopCh        chan struct{}
		wg            sync.WaitGroup
		closeOnce     sync.Once
	}

	valueWithIndex struct {
		index string
		val   string
	}
)

// NewWriter builds a buffered bulk writer using the provided Elasticsearch client.
// The caller is responsible for configuring the client (URLs, auth, gzip, etc.).
func NewWriter(client *elastic.Client, c config.ElasticSearchConf, logger *slog.Logger) (*Writer, error) {
	version, err := client.ElasticsearchVersion(c.Hosts[0])
	if err != nil {
		return nil, err
	}

	writer := &Writer{
		docType:       c.DocType,
		client:        client,
		esVersion:     version,
		maxChunkSize:  c.MaxChunkBytes,
		logger:        logger,
		flushInterval: time.Second,
		stopCh:        make(chan struct{}),
	}
	writer.wg.Add(1)
	go writer.flushLoop()
	return writer, nil
}

func (w *Writer) Write(index, val string) error {
	w.mu.Lock()
	w.buffer = append(w.buffer, valueWithIndex{
		index: index,
		val:   val,
	})
	w.bufferSize += len(val)
	shouldFlush := w.bufferSize >= w.maxChunkSize
	w.mu.Unlock()

	if shouldFlush {
		w.Flush()
	}
	return nil
}

func (w *Writer) Flush() {
	batch := w.popBuffer()
	if len(batch) == 0 {
		return
	}
	w.execute(batch)
}

func (w *Writer) Close() {
	w.closeOnce.Do(func() {
		close(w.stopCh)
		w.wg.Wait()
		w.Flush()
	})
}

func (w *Writer) flushLoop() {
	defer w.wg.Done()
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			w.Flush()
		case <-w.stopCh:
			return
		}
	}
}

func (w *Writer) popBuffer() []valueWithIndex {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.buffer) == 0 {
		return nil
	}
	batch := make([]valueWithIndex, len(w.buffer))
	copy(batch, w.buffer)
	w.buffer = w.buffer[:0]
	w.bufferSize = 0
	return batch
}

func (w *Writer) execute(vals []valueWithIndex) {
	var bulk = w.client.Bulk()
	for _, pair := range vals {
		req := elastic.NewBulkIndexRequest().Index(pair.index)
		if isSupportType(w.esVersion) && len(w.docType) > 0 {
			req = req.Type(w.docType)
		}
		req = req.Doc(pair.val)
		bulk.Add(req)
	}
	resp, err := bulk.Do(context.Background())
	if err != nil {
		w.logger.Error("bulk write failed", "error", err)
		return
	}

	if !resp.Errors {
		return
	}

	for _, imap := range resp.Items {
		for _, item := range imap {
			if item.Error == nil {
				continue
			}
			w.logger.Error("bulk item failed", "error", item.Error)
		}
	}
}

func isSupportType(version string) bool {
	return semver.Compare(version, es8Version) < 0
}
