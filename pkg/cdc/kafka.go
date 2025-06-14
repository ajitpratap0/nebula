package cdc

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	jsonpool "github.com/ajitpratap0/nebula/pkg/json"
	stringpool "github.com/ajitpratap0/nebula/pkg/strings"
	"go.uber.org/zap"
)

// KafkaProducer provides Kafka integration for CDC events with exactly-once semantics
type KafkaProducer struct {
	config KafkaConfig
	logger *zap.Logger

	// Kafka client and producer
	client        sarama.Client
	producer      sarama.SyncProducer
	asyncProducer sarama.AsyncProducer

	// Transaction support for exactly-once
	transactional bool
	transactionID string

	// Topic management
	topicMapping map[string]string // table -> topic mapping
	topicMutex   sync.RWMutex

	// Message serialization
	serializer MessageSerializer

	// Metrics and monitoring
	metrics      KafkaMetrics
	metricsMutex sync.RWMutex

	// State management
	running int32
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// KafkaConsumer provides Kafka CDC event consumption
type KafkaConsumer struct {
	config KafkaConfig
	logger *zap.Logger

	// Kafka client and consumer
	client        sarama.Client
	consumerGroup sarama.ConsumerGroup

	// Event processing
	eventHandler EventHandler
	batchHandler BatchEventHandler
	processor    *StreamProcessor

	// Topic subscriptions
	topics []string

	// Message deserialization
	deserializer MessageDeserializer

	// Metrics
	metrics      KafkaMetrics
	metricsMutex sync.RWMutex

	// State management
	running int32
	stopCh  chan struct{}
}

// KafkaConfig contains Kafka-specific configuration
type KafkaConfig struct {
	Brokers               []string `json:"brokers"`
	SecurityProtocol      string   `json:"security_protocol"`
	SASLMechanism         string   `json:"sasl_mechanism"`
	SASLUsername          string   `json:"sasl_username"`
	SASLPassword          string   `json:"sasl_password"`
	EnableTLS             bool     `json:"enable_tls"`
	TLSInsecureSkipVerify bool     `json:"tls_insecure_skip_verify"`

	// Producer settings
	ProducerAcks        string `json:"producer_acks"` // all, 1, 0
	ProducerRetries     int    `json:"producer_retries"`
	ProducerBatchSize   int    `json:"producer_batch_size"`
	ProducerLingerMS    int    `json:"producer_linger_ms"`
	ProducerCompression string `json:"producer_compression"` // none, gzip, snappy, lz4
	EnableIdempotence   bool   `json:"enable_idempotence"`
	TransactionalID     string `json:"transactional_id"`

	// Consumer settings
	ConsumerGroupID     string `json:"consumer_group_id"`
	AutoOffsetReset     string `json:"auto_offset_reset"` // earliest, latest
	EnableAutoCommit    bool   `json:"enable_auto_commit"`
	SessionTimeoutMS    int    `json:"session_timeout_ms"`
	HeartbeatIntervalMS int    `json:"heartbeat_interval_ms"`
	MaxPollRecords      int    `json:"max_poll_records"`

	// Topic settings
	TopicPrefix  string            `json:"topic_prefix"`
	TopicSuffix  string            `json:"topic_suffix"`
	TopicMapping map[string]string `json:"topic_mapping"`
	DefaultTopic string            `json:"default_topic"`

	// Message settings
	MessageFormat   string `json:"message_format"` // json, avro, protobuf
	IncludeSchema   bool   `json:"include_schema"`
	CompressionType string `json:"compression_type"`

	// Exactly-once settings
	ExactlyOnce          bool `json:"exactly_once"`
	TransactionTimeoutMS int  `json:"transaction_timeout_ms"`
}

// KafkaMetrics contains Kafka-specific metrics
type KafkaMetrics struct {
	MessagesProduced      int64         `json:"messages_produced"`
	MessagesConsumed      int64         `json:"messages_consumed"`
	MessagesFailed        int64         `json:"messages_failed"`
	MessagesRetried       int64         `json:"messages_retried"`
	BytesProduced         int64         `json:"bytes_produced"`
	BytesConsumed         int64         `json:"bytes_consumed"`
	ProducerLatency       time.Duration `json:"producer_latency"`
	ConsumerLatency       time.Duration `json:"consumer_latency"`
	TransactionsCommitted int64         `json:"transactions_committed"`
	TransactionsAborted   int64         `json:"transactions_aborted"`
	LastProducedTime      time.Time     `json:"last_produced_time"`
	LastConsumedTime      time.Time     `json:"last_consumed_time"`
}

// MessageSerializer defines the interface for message serialization
type MessageSerializer interface {
	Serialize(event ChangeEvent) ([]byte, error)
	SerializeKey(event ChangeEvent) ([]byte, error)
	ContentType() string
}

// MessageDeserializer defines the interface for message deserialization
type MessageDeserializer interface {
	Deserialize(data []byte) (ChangeEvent, error)
	ContentType() string
}

// JSONMessageSerializer provides JSON serialization
type JSONMessageSerializer struct{}

// JSONMessageDeserializer provides JSON deserialization
type JSONMessageDeserializer struct{}

// KafkaMessage represents a message sent to/from Kafka
type KafkaMessage struct {
	Key       string            `json:"key"`
	Value     ChangeEvent       `json:"value"`
	Headers   map[string]string `json:"headers"`
	Timestamp time.Time         `json:"timestamp"`
}

// NewKafkaProducer creates a new Kafka producer
func NewKafkaProducer(config KafkaConfig, logger *zap.Logger) *KafkaProducer {
	kp := &KafkaProducer{
		config:        config,
		logger:        logger.With(zap.String("component", "kafka_producer")),
		topicMapping:  make(map[string]string),
		serializer:    &JSONMessageSerializer{},
		stopCh:        make(chan struct{}),
		transactional: config.ExactlyOnce && config.TransactionalID != "",
	}

	// Setup topic mapping
	for table, topic := range config.TopicMapping {
		kp.topicMapping[table] = topic
	}

	return kp
}

// Connect establishes connection to Kafka
func (kp *KafkaProducer) Connect() error {
	if atomic.LoadInt32(&kp.running) == 1 {
		return fmt.Errorf("producer is already connected")
	}

	// Build Kafka configuration
	kafkaConfig := kp.buildSaramaConfig()

	// Create Kafka client
	var err error
	kp.client, err = sarama.NewClient(kp.config.Brokers, kafkaConfig)
	if err != nil {
		return fmt.Errorf("failed to create Kafka client: %w", err)
	}

	// Create producer based on transaction support
	if kp.transactional {
		kp.producer, err = sarama.NewSyncProducerFromClient(kp.client)
		if err != nil {
			return fmt.Errorf("failed to create sync producer: %w", err)
		}

		kp.transactionID = kp.config.TransactionalID
		kp.logger.Info("created transactional Kafka producer",
			zap.String("transaction_id", kp.transactionID))
	} else {
		kp.asyncProducer, err = sarama.NewAsyncProducerFromClient(kp.client)
		if err != nil {
			return fmt.Errorf("failed to create async producer: %w", err)
		}

		// Start goroutine to handle async producer responses
		kp.wg.Add(1)
		go kp.handleAsyncProducerMessages()

		kp.logger.Info("created async Kafka producer")
	}

	atomic.StoreInt32(&kp.running, 1)

	kp.logger.Info("connected to Kafka",
		zap.Strings("brokers", kp.config.Brokers),
		zap.Bool("transactional", kp.transactional))

	return nil
}

// ProduceEvent produces a single CDC event to Kafka
func (kp *KafkaProducer) ProduceEvent(ctx context.Context, event ChangeEvent) error {
	return kp.ProduceEvents(ctx, []ChangeEvent{event})
}

// ProduceEvents produces multiple CDC events to Kafka with exactly-once semantics
func (kp *KafkaProducer) ProduceEvents(ctx context.Context, events []ChangeEvent) error {
	if atomic.LoadInt32(&kp.running) == 0 {
		return fmt.Errorf("producer is not connected")
	}

	start := time.Now()
	defer func() {
		kp.updateMetrics(func(m *KafkaMetrics) {
			m.ProducerLatency = time.Since(start)
			m.LastProducedTime = time.Now()
		})
	}()

	if kp.transactional {
		return kp.produceEventsTransactional(ctx, events)
	} else {
		return kp.produceEventsAsync(ctx, events)
	}
}

// produceEventsTransactional produces events with exactly-once semantics
func (kp *KafkaProducer) produceEventsTransactional(ctx context.Context, events []ChangeEvent) error {
	// Begin transaction
	err := kp.producer.BeginTxn()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if err != nil {
			if abortErr := kp.producer.AbortTxn(); abortErr != nil {
				kp.logger.Error("failed to abort transaction", zap.Error(abortErr))
			}
			kp.updateMetrics(func(m *KafkaMetrics) {
				m.TransactionsAborted++
			})
		}
	}()

	// Produce all messages within the transaction
	messages := make([]*sarama.ProducerMessage, 0, len(events))

	for _, event := range events {
		message, err := kp.buildProducerMessage(event)
		if err != nil {
			return fmt.Errorf("failed to build producer message: %w", err)
		}
		messages = append(messages, message)
	}

	// Send all messages
	for _, message := range messages {
		partition, offset, err := kp.producer.SendMessage(message)
		if err != nil {
			return fmt.Errorf("failed to send message: %w", err)
		}

		kp.logger.Debug("produced message",
			zap.String("topic", message.Topic),
			zap.Int32("partition", partition),
			zap.Int64("offset", offset))
	}

	// Commit transaction
	err = kp.producer.CommitTxn()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	kp.updateMetrics(func(m *KafkaMetrics) {
		m.MessagesProduced += int64(len(events))
		m.TransactionsCommitted++
	})

	return nil
}

// produceEventsAsync produces events asynchronously
func (kp *KafkaProducer) produceEventsAsync(ctx context.Context, events []ChangeEvent) error {
	for _, event := range events {
		message, err := kp.buildProducerMessage(event)
		if err != nil {
			return fmt.Errorf("failed to build producer message: %w", err)
		}

		select {
		case kp.asyncProducer.Input() <- message:
			// Message sent successfully
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	kp.updateMetrics(func(m *KafkaMetrics) {
		m.MessagesProduced += int64(len(events))
	})

	return nil
}

// buildProducerMessage builds a Kafka producer message from a CDC event
func (kp *KafkaProducer) buildProducerMessage(event ChangeEvent) (*sarama.ProducerMessage, error) {
	// Determine topic
	topic := kp.getTopicForEvent(event)

	// Serialize message key
	key, err := kp.serializer.SerializeKey(event)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize message key: %w", err)
	}

	// Serialize message value
	value, err := kp.serializer.Serialize(event)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize message value: %w", err)
	}

	// Build headers
	headers := []sarama.RecordHeader{
		{Key: []byte("source"), Value: []byte(string(event.Source.ConnectorType))},
		{Key: []byte("operation"), Value: []byte(string(event.Operation))},
		{Key: []byte("database"), Value: []byte(event.Database)},
		{Key: []byte("table"), Value: []byte(event.Table)},
		{Key: []byte("content-type"), Value: []byte(kp.serializer.ContentType())},
	}

	if event.TransactionID != "" {
		headers = append(headers, sarama.RecordHeader{
			Key: []byte("transaction-id"), Value: []byte(event.TransactionID),
		})
	}

	message := &sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.ByteEncoder(key),
		Value:     sarama.ByteEncoder(value),
		Headers:   headers,
		Timestamp: event.Timestamp,
	}

	kp.updateMetrics(func(m *KafkaMetrics) {
		m.BytesProduced += int64(len(key) + len(value))
	})

	return message, nil
}

// getTopicForEvent determines the Kafka topic for an event
func (kp *KafkaProducer) getTopicForEvent(event ChangeEvent) string {
	// Check explicit mapping first
	kp.topicMutex.RLock()
	if topic, exists := kp.topicMapping[event.Table]; exists {
		kp.topicMutex.RUnlock()
		return topic
	}

	// Use pooled string building for database.table lookup
	fullTableName := stringpool.Sprintf("%s.%s", event.Database, event.Table)
	if topic, exists := kp.topicMapping[fullTableName]; exists {
		kp.topicMutex.RUnlock()
		return topic
	}
	kp.topicMutex.RUnlock()

	// Use default topic if configured
	if kp.config.DefaultTopic != "" {
		return kp.config.DefaultTopic
	}

	// Generate topic name from table using pooled string building
	builder := stringpool.GetBuilder(stringpool.Small)
	defer stringpool.PutBuilder(builder, stringpool.Small)

	// Build topic name efficiently
	if kp.config.TopicPrefix != "" {
		builder.WriteString(kp.config.TopicPrefix)
	}
	builder.WriteString(event.Table)
	if kp.config.TopicSuffix != "" {
		builder.WriteString(kp.config.TopicSuffix)
	}

	// Replace dots with underscores for Kafka topic naming
	topic := strings.ReplaceAll(builder.String(), ".", "_")

	return topic
}

// handleAsyncProducerMessages handles async producer success/error messages
func (kp *KafkaProducer) handleAsyncProducerMessages() {
	defer kp.wg.Done()

	for {
		select {
		case success := <-kp.asyncProducer.Successes():
			kp.logger.Debug("message produced successfully",
				zap.String("topic", success.Topic),
				zap.Int32("partition", success.Partition),
				zap.Int64("offset", success.Offset))

		case err := <-kp.asyncProducer.Errors():
			kp.logger.Error("failed to produce message",
				zap.String("topic", err.Msg.Topic),
				zap.Error(err.Err))

			kp.updateMetrics(func(m *KafkaMetrics) {
				m.MessagesFailed++
			})

		case <-kp.stopCh:
			return
		}
	}
}

// buildSaramaConfig builds Sarama configuration from KafkaConfig
func (kp *KafkaProducer) buildSaramaConfig() *sarama.Config {
	config := sarama.NewConfig()

	// Producer settings
	switch kp.config.ProducerAcks {
	case "all", "-1":
		config.Producer.RequiredAcks = sarama.WaitForAll
	case "1":
		config.Producer.RequiredAcks = sarama.WaitForLocal
	case "0":
		config.Producer.RequiredAcks = sarama.NoResponse
	default:
		config.Producer.RequiredAcks = sarama.WaitForAll
	}

	config.Producer.Retry.Max = kp.config.ProducerRetries
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	// Compression
	switch kp.config.ProducerCompression {
	case "gzip":
		config.Producer.Compression = sarama.CompressionGZIP
	case "snappy":
		config.Producer.Compression = sarama.CompressionSnappy
	case "lz4":
		config.Producer.Compression = sarama.CompressionLZ4
	default:
		config.Producer.Compression = sarama.CompressionNone
	}

	// Idempotence and transactions
	if kp.config.EnableIdempotence {
		config.Producer.Idempotent = true
		config.Net.MaxOpenRequests = 1
	}

	if kp.transactional {
		config.Producer.Transaction.ID = kp.config.TransactionalID
		config.Producer.Transaction.Timeout = time.Duration(kp.config.TransactionTimeoutMS) * time.Millisecond
		config.Producer.Idempotent = true
		config.Net.MaxOpenRequests = 1
	}

	// Security settings
	if kp.config.SecurityProtocol == "SASL_SSL" || kp.config.SecurityProtocol == "SSL" {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = &tls.Config{
			InsecureSkipVerify: kp.config.TLSInsecureSkipVerify,
		}
	}

	if kp.config.SASLMechanism != "" {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = kp.config.SASLUsername
		config.Net.SASL.Password = kp.config.SASLPassword

		switch kp.config.SASLMechanism {
		case "PLAIN":
			config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		case "SCRAM-SHA-256":
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		case "SCRAM-SHA-512":
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		}
	}

	return config
}

// Close closes the Kafka producer
func (kp *KafkaProducer) Close() error {
	if !atomic.CompareAndSwapInt32(&kp.running, 1, 0) {
		return fmt.Errorf("producer is not running")
	}

	close(kp.stopCh)

	// Close producers
	if kp.producer != nil {
		if err := kp.producer.Close(); err != nil {
			kp.logger.Error("failed to close sync producer", zap.Error(err))
		}
	}

	if kp.asyncProducer != nil {
		if err := kp.asyncProducer.Close(); err != nil {
			kp.logger.Error("failed to close async producer", zap.Error(err))
		}
	}

	// Close client
	if kp.client != nil {
		if err := kp.client.Close(); err != nil {
			kp.logger.Error("failed to close Kafka client", zap.Error(err))
		}
	}

	// Wait for goroutines
	kp.wg.Wait()

	kp.logger.Info("Kafka producer closed")

	return nil
}

// GetMetrics returns Kafka producer metrics
func (kp *KafkaProducer) GetMetrics() KafkaMetrics {
	kp.metricsMutex.RLock()
	defer kp.metricsMutex.RUnlock()

	return kp.metrics
}

// updateMetrics updates the metrics
func (kp *KafkaProducer) updateMetrics(updateFn func(*KafkaMetrics)) {
	kp.metricsMutex.Lock()
	defer kp.metricsMutex.Unlock()

	updateFn(&kp.metrics)
}

// JSONMessageSerializer implementation

// Serialize serializes a ChangeEvent to JSON
func (s *JSONMessageSerializer) Serialize(event ChangeEvent) ([]byte, error) {
	return jsonpool.Marshal(event)
}

// SerializeKey serializes the event key
func (s *JSONMessageSerializer) SerializeKey(event ChangeEvent) ([]byte, error) {
	// Use a combination of database, table, and primary key as the message key
	key := map[string]interface{}{
		"database": event.Database,
		"table":    event.Table,
	}

	// Add primary key information if available in the event
	if event.After != nil {
		if id, exists := event.After["id"]; exists {
			key["id"] = id
		}
	} else if event.Before != nil {
		if id, exists := event.Before["id"]; exists {
			key["id"] = id
		}
	}

	return jsonpool.Marshal(key)
}

// ContentType returns the content type
func (s *JSONMessageSerializer) ContentType() string {
	return "application/json"
}

// JSONMessageDeserializer implementation

// Deserialize deserializes JSON data to a ChangeEvent
func (d *JSONMessageDeserializer) Deserialize(data []byte) (ChangeEvent, error) {
	var event ChangeEvent
	err := jsonpool.Unmarshal(data, &event)
	return event, err
}

// ContentType returns the content type
func (d *JSONMessageDeserializer) ContentType() string {
	return "application/json"
}

// NewKafkaConsumer creates a new Kafka consumer
func NewKafkaConsumer(config KafkaConfig, logger *zap.Logger) *KafkaConsumer {
	return &KafkaConsumer{
		config:       config,
		logger:       logger.With(zap.String("component", "kafka_consumer")),
		deserializer: &JSONMessageDeserializer{},
		stopCh:       make(chan struct{}),
	}
}

// Subscribe subscribes to Kafka topics and starts consuming
func (kc *KafkaConsumer) Subscribe(topics []string, handler EventHandler) error {
	if atomic.LoadInt32(&kc.running) == 1 {
		return fmt.Errorf("consumer is already running")
	}

	kc.topics = topics
	kc.eventHandler = handler

	// Build Kafka configuration
	kafkaConfig := kc.buildSaramaConfig()

	// Create Kafka client
	var err error
	kc.client, err = sarama.NewClient(kc.config.Brokers, kafkaConfig)
	if err != nil {
		return fmt.Errorf("failed to create Kafka client: %w", err)
	}

	// Create consumer group
	kc.consumerGroup, err = sarama.NewConsumerGroupFromClient(kc.config.ConsumerGroupID, kc.client)
	if err != nil {
		return fmt.Errorf("failed to create consumer group: %w", err)
	}

	atomic.StoreInt32(&kc.running, 1)

	// Start consuming
	go kc.consume()

	kc.logger.Info("subscribed to Kafka topics",
		zap.Strings("topics", topics),
		zap.String("consumer_group", kc.config.ConsumerGroupID))

	return nil
}

// consume runs the consumer loop
func (kc *KafkaConsumer) consume() {
	ctx := context.Background()

	for {
		select {
		case <-kc.stopCh:
			return
		default:
			err := kc.consumerGroup.Consume(ctx, kc.topics, kc)
			if err != nil {
				kc.logger.Error("consumer group error", zap.Error(err))
				time.Sleep(time.Second)
			}
		}
	}
}

// buildSaramaConfig builds Sarama configuration for consumer
func (kc *KafkaConsumer) buildSaramaConfig() *sarama.Config {
	config := sarama.NewConfig()

	// Consumer settings
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	switch kc.config.AutoOffsetReset {
	case "earliest":
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	case "latest":
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	default:
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	config.Consumer.Group.Session.Timeout = time.Duration(kc.config.SessionTimeoutMS) * time.Millisecond
	config.Consumer.Group.Heartbeat.Interval = time.Duration(kc.config.HeartbeatIntervalMS) * time.Millisecond

	// Auto-commit settings
	if kc.config.EnableAutoCommit {
		config.Consumer.Offsets.AutoCommit.Enable = true
	}

	return config
}

// Setup implements sarama.ConsumerGroupHandler
func (kc *KafkaConsumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup implements sarama.ConsumerGroupHandler
func (kc *KafkaConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim implements sarama.ConsumerGroupHandler
func (kc *KafkaConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}

			if err := kc.processMessage(session, message); err != nil {
				kc.logger.Error("failed to process message", zap.Error(err))
				kc.updateMetrics(func(m *KafkaMetrics) {
					m.MessagesFailed++
				})
			}

		case <-session.Context().Done():
			return nil
		}
	}
}

// processMessage processes a single Kafka message
func (kc *KafkaConsumer) processMessage(session sarama.ConsumerGroupSession, message *sarama.ConsumerMessage) error {
	start := time.Now()

	defer func() {
		kc.updateMetrics(func(m *KafkaMetrics) {
			m.MessagesConsumed++
			m.BytesConsumed += int64(len(message.Key) + len(message.Value))
			m.ConsumerLatency = time.Since(start)
			m.LastConsumedTime = time.Now()
		})
	}()

	// Deserialize message
	event, err := kc.deserializer.Deserialize(message.Value)
	if err != nil {
		return fmt.Errorf("failed to deserialize message: %w", err)
	}

	// Process event with handler
	if kc.eventHandler != nil {
		if err := kc.eventHandler(session.Context(), event); err != nil {
			return fmt.Errorf("event handler failed: %w", err)
		}
	}

	// Mark message as processed
	session.MarkMessage(message, "")

	kc.logger.Debug("processed Kafka message",
		zap.String("topic", message.Topic),
		zap.Int32("partition", message.Partition),
		zap.Int64("offset", message.Offset),
		zap.String("operation", string(event.Operation)),
		zap.String("table", event.Table))

	return nil
}

// Close closes the Kafka consumer
func (kc *KafkaConsumer) Close() error {
	if !atomic.CompareAndSwapInt32(&kc.running, 1, 0) {
		return fmt.Errorf("consumer is not running")
	}

	close(kc.stopCh)

	// Close consumer group
	if kc.consumerGroup != nil {
		if err := kc.consumerGroup.Close(); err != nil {
			kc.logger.Error("failed to close consumer group", zap.Error(err))
		}
	}

	// Close client
	if kc.client != nil {
		if err := kc.client.Close(); err != nil {
			kc.logger.Error("failed to close Kafka client", zap.Error(err))
		}
	}

	kc.logger.Info("Kafka consumer closed")

	return nil
}

// GetMetrics returns Kafka consumer metrics
func (kc *KafkaConsumer) GetMetrics() KafkaMetrics {
	kc.metricsMutex.RLock()
	defer kc.metricsMutex.RUnlock()

	return kc.metrics
}

// updateMetrics updates the metrics
func (kc *KafkaConsumer) updateMetrics(updateFn func(*KafkaMetrics)) {
	kc.metricsMutex.Lock()
	defer kc.metricsMutex.Unlock()

	updateFn(&kc.metrics)
}
