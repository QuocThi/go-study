package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/kevwan/go-stash/stash/config"
	"github.com/kevwan/go-stash/stash/es"
	"github.com/kevwan/go-stash/stash/filter"
	"github.com/kevwan/go-stash/stash/handler"
	"github.com/kevwan/go-stash/stash/kafka"
	"github.com/olivere/elastic/v7"
)

var (
	configFile = flag.String("f", "etc/config.yaml", "Specify the config file")
	httpAddr   = flag.String("http.addr", "", "Override HTTP bind address")
)

type pipeline struct {
	name      string
	writer    *es.Writer
	consumers []*kafka.Consumer
}

func (p *pipeline) start() {
	for _, consumer := range p.consumers {
		consumer.Start()
	}
}

func (p *pipeline) stop(ctx context.Context, logger *slog.Logger) {
	var wg sync.WaitGroup
	for _, consumer := range p.consumers {
		wg.Add(1)
		go func(c *kafka.Consumer) {
			defer wg.Done()
			if err := c.Stop(ctx); err != nil && ctx.Err() == nil {
				logger.Error("failed to stop consumer", "consumer", c.Name(), "error", err)
			}
		}(consumer)
	}
	wg.Wait()
	p.writer.Close()
	logger.Info("pipeline stopped", "pipeline", p.name)
}

func main() {
	flag.Parse()

	cfg, err := config.Load(*configFile)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}
	if *httpAddr != "" {
		cfg.HTTP.Addr = *httpAddr
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pipelines, err := buildPipelines(ctx, cfg, logger)
	if err != nil {
		logger.Error("failed to build pipelines", "error", err)
		os.Exit(1)
	}
	for _, p := range pipelines {
		p.start()
	}

	engine := gin.New()
	engine.Use(gin.Logger(), gin.Recovery())
	engine.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	engine.GET("/clusters", func(c *gin.Context) {
		c.JSON(http.StatusOK, buildClusterResponse(cfg))
	})

	server := &http.Server{
		Addr:    cfg.HTTP.Addr,
		Handler: engine,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http server error", "error", err)
			stop()
		}
	}()

	logger.Info("go-stash started", "http_addr", cfg.HTTP.Addr)
	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.GracePeriod)
	defer cancel()

	for _, p := range pipelines {
		p.stop(shutdownCtx, logger)
	}

	serverCtx, cancelServer := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelServer()
	if err := server.Shutdown(serverCtx); err != nil {
		logger.Error("failed to stop http server", "error", err)
	}
}

func buildPipelines(ctx context.Context, cfg config.Config, logger *slog.Logger) ([]*pipeline, error) {
	var pipelines []*pipeline
	cleanup := func() {
		for _, p := range pipelines {
			p.writer.Close()
			for _, consumer := range p.consumers {
				_ = consumer.Stop(context.Background())
			}
		}
	}

	for idx, cluster := range cfg.Clusters {
		client, err := elastic.NewClient(
			elastic.SetSniff(false),
			elastic.SetURL(cluster.Output.ElasticSearch.Hosts...),
			elastic.SetBasicAuth(cluster.Output.ElasticSearch.Username, cluster.Output.ElasticSearch.Password),
		)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("cluster %d elasticsearch client: %w", idx, err)
		}

		writer, err := es.NewWriter(cluster.Output.ElasticSearch, logger)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("cluster %d writer: %w", idx, err)
		}

		loc := time.Local
		if tz := cluster.Output.ElasticSearch.TimeZone; tz != "" {
			loc, err = time.LoadLocation(tz)
			if err != nil {
				writer.Close()
				cleanup()
				return nil, fmt.Errorf("cluster %d timezone: %w", idx, err)
			}
		}

		indexer := es.NewIndex(client, cluster.Output.ElasticSearch.Index, loc, logger)
		handle := handler.NewHandler(writer, indexer)
		handle.AddFilters(filter.CreateFilters(cluster)...)
		handle.AddFilters(filter.AddUriFieldFilter("url", "uri"))

		var consumers []*kafka.Consumer
		for _, topic := range cluster.Input.Kafka.Topics {
			consumer, err := kafka.NewConsumer(ctx, cluster.Input.Kafka, topic, handle, logger)
			if err != nil {
				writer.Close()
				cleanup()
				return nil, fmt.Errorf("cluster %d topic %s: %w", idx, topic, err)
			}
			consumers = append(consumers, consumer)
		}
		name := cluster.Input.Kafka.Name
		if name == "" {
			name = cluster.Output.ElasticSearch.Index
		}
		pipelines = append(pipelines, &pipeline{
			name:      name,
			writer:    writer,
			consumers: consumers,
		})
	}
	return pipelines, nil
}

func buildClusterResponse(cfg config.Config) []map[string]interface{} {
	var resp []map[string]interface{}
	for _, cluster := range cfg.Clusters {
		name := cluster.Input.Kafka.Name
		if name == "" {
			name = cluster.Output.ElasticSearch.Index
		}
		resp = append(resp, map[string]interface{}{
			"name":       name,
			"group":      cluster.Input.Kafka.Group,
			"topics":     cluster.Input.Kafka.Topics,
			"es_hosts":   cluster.Output.ElasticSearch.Hosts,
			"es_index":   cluster.Output.ElasticSearch.Index,
			"time_zone":  cluster.Output.ElasticSearch.TimeZone,
			"processors": cluster.Input.Kafka.Processors,
		})
	}
	return resp
}
