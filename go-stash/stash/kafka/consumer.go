package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"

	"github.com/kevwan/go-stash/stash/config"
	"github.com/kevwan/go-stash/stash/handler"
)

type Consumer struct {
	cfg      config.KafkaConf
	topic    string
	handler  *handler.MessageHandler
	logger   *slog.Logger
	ctx      context.Context
	cancel   context.CancelFunc
	readers  []*kafkago.Reader
	messages chan kafkaTask

	readWg sync.WaitGroup
	procWg sync.WaitGroup
}

type kafkaTask struct {
	reader *kafkago.Reader
	msg    kafkago.Message
}

func NewConsumer(parent context.Context, cfg config.KafkaConf, topic string, handler *handler.MessageHandler, logger *slog.Logger) (*Consumer, error) {
	ctx, cancel := context.WithCancel(parent)
	if cfg.Processors <= 0 {
		cfg.Processors = 1
	}
	bufferSize := cfg.Processors * 2
	if bufferSize < 1 {
		bufferSize = 1
	}
	c := &Consumer{
		cfg:      cfg,
		topic:    topic,
		handler:  handler,
		logger:   logger,
		ctx:      ctx,
		cancel:   cancel,
		messages: make(chan kafkaTask, bufferSize),
	}
	if err := c.initReaders(); err != nil {
		cancel()
		return nil, err
	}
	return c, nil
}

func (c *Consumer) initReaders() error {
	totalReaders := c.cfg.Conns * c.cfg.Consumers
	if totalReaders <= 0 {
		totalReaders = 1
	}
	startOffset := kafkago.LastOffset
	if c.cfg.Offset == "first" {
		startOffset = kafkago.FirstOffset
	}

	for i := 0; i < totalReaders; i++ {
		readerConfig := kafkago.ReaderConfig{
			Brokers:     c.cfg.Brokers,
			GroupID:     c.cfg.Group,
			Topic:       c.topic,
			MinBytes:    c.cfg.MinBytes,
			MaxBytes:    c.cfg.MaxBytes,
			StartOffset: startOffset,
		}
		if c.cfg.Username != "" && c.cfg.Password != "" {
			readerConfig.Dialer = &kafkago.Dialer{
				SASLMechanism: plain.Mechanism{
					Username: c.cfg.Username,
					Password: c.cfg.Password,
				},
			}
		}
		if c.cfg.CaFile != "" {
			dialer := readerConfig.Dialer
			if dialer == nil {
				dialer = &kafkago.Dialer{}
			}
			tlsConfig, err := tlsFromFile(c.cfg.CaFile)
			if err != nil {
				return err
			}
			dialer.TLS = tlsConfig
			readerConfig.Dialer = dialer
		}
		reader := kafkago.NewReader(readerConfig)
		c.readers = append(c.readers, reader)
	}
	return nil
}

func (c *Consumer) Start() {
	for _, reader := range c.readers {
		c.readWg.Add(1)
		go c.readLoop(reader)
	}

	c.procWg.Add(c.cfg.Processors)
	for i := 0; i < c.cfg.Processors; i++ {
		go c.processLoop()
	}

	go func() {
		c.readWg.Wait()
		close(c.messages)
	}()
}

func (c *Consumer) Stop(ctx context.Context) error {
	c.cancel()
	for _, reader := range c.readers {
		reader.Close()
	}

	done := make(chan struct{})
	go func() {
		c.readWg.Wait()
		c.procWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Consumer) readLoop(reader *kafkago.Reader) {
	defer c.readWg.Done()
	for {
		msg, err := reader.FetchMessage(c.ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, io.EOF) {
				return
			}
			select {
			case <-c.ctx.Done():
				return
			default:
				c.logger.Warn("kafka fetch failed", "topic", c.topic, "error", err)
				time.Sleep(500 * time.Millisecond)
				continue
			}
		}

		select {
		case <-c.ctx.Done():
			return
		case c.messages <- kafkaTask{reader: reader, msg: msg}:
		}
	}
}

func (c *Consumer) processLoop() {
	defer c.procWg.Done()
	for task := range c.messages {
		if err := c.handler.Consume(c.ctx, string(task.msg.Key), string(task.msg.Value)); err != nil {
			c.logger.Error("handler failed", "topic", c.topic, "error", err)
			continue
		}

		if err := task.reader.CommitMessages(context.Background(), task.msg); err != nil {
			c.logger.Error("commit failed", "topic", c.topic, "error", err)
		}
	}
}

func (c *Consumer) Name() string {
	if c.cfg.Name != "" {
		return c.cfg.Name
	}
	return fmt.Sprintf("%s-%s", c.cfg.Group, c.topic)
}

func tlsFromFile(path string) (*tls.Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("failed to append certs from %s", path)
	}

	return &tls.Config{
		RootCAs:            pool,
		InsecureSkipVerify: true,
	}, nil
}
