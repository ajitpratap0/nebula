// Package config provides connector-specific configurations that embed BaseConfig
package config

// CSVSourceConfig contains configuration for CSV source connectors
type CSVSourceConfig struct {
	BaseConfig `yaml:",inline" json:",inline"`
	
	// CSV-specific configuration
	FilePath     string `yaml:"file_path" json:"file_path" required:"true"`
	Delimiter    string `yaml:"delimiter" json:"delimiter" default:","`
	Quote        string `yaml:"quote" json:"quote" default:"\""`
	Escape       string `yaml:"escape" json:"escape" default:"\""`
	Comment      string `yaml:"comment" json:"comment" default:""`
	SkipRows     int    `yaml:"skip_rows" json:"skip_rows" default:"0"`
	HasHeader    bool   `yaml:"has_header" json:"has_header" default:"true"`
	Encoding     string `yaml:"encoding" json:"encoding" default:"utf-8"`
	NullValues   []string `yaml:"null_values" json:"null_values"`
	TrimSpaces   bool   `yaml:"trim_spaces" json:"trim_spaces" default:"true"`
}

// CSVDestinationConfig contains configuration for CSV destination connectors
type CSVDestinationConfig struct {
	BaseConfig `yaml:",inline" json:",inline"`
	
	// CSV-specific configuration
	OutputPath   string `yaml:"output_path" json:"output_path" required:"true"`
	Delimiter    string `yaml:"delimiter" json:"delimiter" default:","`
	Quote        string `yaml:"quote" json:"quote" default:"\""`
	Escape       string `yaml:"escape" json:"escape" default:"\""`
	WriteHeader  bool   `yaml:"write_header" json:"write_header" default:"true"`
	Encoding     string `yaml:"encoding" json:"encoding" default:"utf-8"`
	LineTerminator string `yaml:"line_terminator" json:"line_terminator" default:"\n"`
	CreateDirs   bool   `yaml:"create_dirs" json:"create_dirs" default:"true"`
}

// GoogleAdsSourceConfig contains configuration for Google Ads API source connector
type GoogleAdsSourceConfig struct {
	BaseConfig `yaml:",inline" json:",inline"`
	
	// Google Ads API configuration
	DeveloperToken   string   `yaml:"developer_token" json:"developer_token" required:"true"`
	ClientID         string   `yaml:"client_id" json:"client_id" required:"true"`
	ClientSecret     string   `yaml:"client_secret" json:"client_secret" required:"true"`
	RefreshToken     string   `yaml:"refresh_token" json:"refresh_token" required:"true"`
	CustomerIDs      []string `yaml:"customer_ids" json:"customer_ids" required:"true"`
	LoginCustomerID  string   `yaml:"login_customer_id" json:"login_customer_id"`
	
	// Query configuration
	Query            string `yaml:"query" json:"query" required:"true"`
	PageSize         int    `yaml:"page_size" json:"page_size" default:"10000"`
	MaxResults       int64  `yaml:"max_results" json:"max_results" default:"0"` // 0 = no limit
	
	// API behavior
	IncludeZeroImpressions bool `yaml:"include_zero_impressions" json:"include_zero_impressions" default:"false"`
	UseRawEnumValues      bool `yaml:"use_raw_enum_values" json:"use_raw_enum_values" default:"true"`
	ValidateOnly          bool `yaml:"validate_only" json:"validate_only" default:"false"`
}

// MetaAdsSourceConfig contains configuration for Meta Ads API source connector
type MetaAdsSourceConfig struct {
	BaseConfig `yaml:",inline" json:",inline"`
	
	// Meta Ads API configuration
	AppID         string   `yaml:"app_id" json:"app_id" required:"true"`
	AppSecret     string   `yaml:"app_secret" json:"app_secret" required:"true"`
	AccessToken   string   `yaml:"access_token" json:"access_token" required:"true"`
	AccountIDs    []string `yaml:"account_ids" json:"account_ids" required:"true"`
	
	// Query configuration
	Fields        []string `yaml:"fields" json:"fields" required:"true"`
	Level         string   `yaml:"level" json:"level" default:"campaign"`
	TimeRange     string   `yaml:"time_range" json:"time_range" default:"last_30_days"`
	DatePreset    string   `yaml:"date_preset" json:"date_preset"`
	TimeIncrement int      `yaml:"time_increment" json:"time_increment" default:"1"`
	
	// API behavior
	Limit         int    `yaml:"limit" json:"limit" default:"25"`
	BreakdownType string `yaml:"breakdown_type" json:"breakdown_type"`
	ActionType    string `yaml:"action_type" json:"action_type"`
}

// SnowflakeDestinationConfig contains configuration for Snowflake destination connector
type SnowflakeDestinationConfig struct {
	BaseConfig `yaml:",inline" json:",inline"`
	
	// Snowflake connection configuration
	Account   string `yaml:"account" json:"account" required:"true"`
	User      string `yaml:"user" json:"user" required:"true"`
	Password  string `yaml:"password" json:"password" required:"true"`
	Database  string `yaml:"database" json:"database" required:"true"`
	Schema    string `yaml:"schema" json:"schema" required:"true"`
	Warehouse string `yaml:"warehouse" json:"warehouse" required:"true"`
	Role      string `yaml:"role" json:"role"`
	
	// Connection pool settings
	ConnectionPoolSize int `yaml:"connection_pool_size" json:"connection_pool_size" default:"8"`
	MaxIdleConns      int `yaml:"max_idle_conns" json:"max_idle_conns" default:"2"`
	MaxOpenConns      int `yaml:"max_open_conns" json:"max_open_conns" default:"10"`
	ConnMaxLifetime   int `yaml:"conn_max_lifetime" json:"conn_max_lifetime" default:"3600"` // seconds
	
	// Stage configuration
	StageName         string `yaml:"stage_name" json:"stage_name" default:"NEBULA_STAGE"`
	StagePrefix       string `yaml:"stage_prefix" json:"stage_prefix" default:"uploads/"`
	UseExternalStage  bool   `yaml:"use_external_stage" json:"use_external_stage" default:"false"`
	ExternalStageType string `yaml:"external_stage_type" json:"external_stage_type" default:"S3"`
	ExternalStageURL  string `yaml:"external_stage_url" json:"external_stage_url"`
	
	// Performance configuration
	ParallelUploads     int    `yaml:"parallel_uploads" json:"parallel_uploads" default:"16"`
	MicroBatchSize      int    `yaml:"micro_batch_size" json:"micro_batch_size" default:"200000"`
	MicroBatchTimeout   int    `yaml:"micro_batch_timeout" json:"micro_batch_timeout" default:"5"` // seconds
	FilesPerCopy        int    `yaml:"files_per_copy" json:"files_per_copy" default:"10"`
	MaxFileSize         int64  `yaml:"max_file_size" json:"max_file_size" default:"524288000"` // 500MB
	FileFormat          string `yaml:"file_format" json:"file_format" default:"CSV"`
	CompressionType     string `yaml:"compression_type" json:"compression_type" default:"GZIP"`
	AsyncCopy           bool   `yaml:"async_copy" json:"async_copy" default:"true"`
	
	// Table configuration
	CreateTable       bool   `yaml:"create_table" json:"create_table" default:"true"`
	TablePrefix       string `yaml:"table_prefix" json:"table_prefix"`
	TableSuffix       string `yaml:"table_suffix" json:"table_suffix"`
	TruncateTable     bool   `yaml:"truncate_table" json:"truncate_table" default:"false"`
	MergeMode         string `yaml:"merge_mode" json:"merge_mode" default:"APPEND"` // APPEND, UPSERT, REPLACE
	UpsertKeys        []string `yaml:"upsert_keys" json:"upsert_keys"`
}

// BigQueryDestinationConfig contains configuration for BigQuery destination connector
type BigQueryDestinationConfig struct {
	BaseConfig `yaml:",inline" json:",inline"`
	
	// BigQuery connection configuration
	ProjectID         string `yaml:"project_id" json:"project_id" required:"true"`
	Dataset           string `yaml:"dataset" json:"dataset" required:"true"`
	CredentialsJSON   string `yaml:"credentials_json" json:"credentials_json"`
	CredentialsPath   string `yaml:"credentials_path" json:"credentials_path"`
	Location          string `yaml:"location" json:"location" default:"US"`
	
	// Table configuration
	Table           string   `yaml:"table" json:"table" required:"true"`
	CreateTable     bool     `yaml:"create_table" json:"create_table" default:"true"`
	TablePrefix     string   `yaml:"table_prefix" json:"table_prefix"`
	TableSuffix     string   `yaml:"table_suffix" json:"table_suffix"`
	PartitionField  string   `yaml:"partition_field" json:"partition_field"`
	PartitionType   string   `yaml:"partition_type" json:"partition_type" default:"DAY"`
	ClusteringFields []string `yaml:"clustering_fields" json:"clustering_fields"`
	
	// Loading configuration
	WriteMode         string `yaml:"write_mode" json:"write_mode" default:"WRITE_APPEND"` // WRITE_APPEND, WRITE_TRUNCATE, WRITE_EMPTY
	LoadMethod        string `yaml:"load_method" json:"load_method" default:"STREAMING"` // STREAMING, LOAD_JOBS
	StreamingInserts  bool   `yaml:"streaming_inserts" json:"streaming_inserts" default:"true"`
	MicroBatchSize    int    `yaml:"micro_batch_size" json:"micro_batch_size" default:"1000"`
	MaxBadRecords     int    `yaml:"max_bad_records" json:"max_bad_records" default:"0"`
	IgnoreUnknownFields bool `yaml:"ignore_unknown_fields" json:"ignore_unknown_fields" default:"false"`
	
	// Load Job configuration (when using LOAD_JOBS)
	LoadJobTimeout    int    `yaml:"load_job_timeout" json:"load_job_timeout" default:"300"` // seconds
	TempBucket        string `yaml:"temp_bucket" json:"temp_bucket"`
	TempPrefix        string `yaml:"temp_prefix" json:"temp_prefix" default:"tmp/"`
	SourceFormat      string `yaml:"source_format" json:"source_format" default:"NEWLINE_DELIMITED_JSON"`
	Compression       string `yaml:"compression" json:"compression" default:"GZIP"`
}

// S3DestinationConfig contains configuration for S3 destination connector
type S3DestinationConfig struct {
	BaseConfig `yaml:",inline" json:",inline"`
	
	// S3 connection configuration
	Region          string `yaml:"region" json:"region" required:"true"`
	Bucket          string `yaml:"bucket" json:"bucket" required:"true"`
	AccessKeyID     string `yaml:"access_key_id" json:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key" json:"secret_access_key"`
	SessionToken    string `yaml:"session_token" json:"session_token"`
	Endpoint        string `yaml:"endpoint" json:"endpoint"` // For S3-compatible services
	UseSSL          bool   `yaml:"use_ssl" json:"use_ssl" default:"true"`
	
	// Object configuration
	KeyPrefix         string `yaml:"key_prefix" json:"key_prefix"`
	KeySuffix         string `yaml:"key_suffix" json:"key_suffix"`
	KeyTemplate       string `yaml:"key_template" json:"key_template"`
	PartitionBy       []string `yaml:"partition_by" json:"partition_by"`
	FileFormat        string `yaml:"file_format" json:"file_format" default:"parquet"` // parquet, json, csv, avro, orc
	Compression       string `yaml:"compression" json:"compression" default:"snappy"`
	
	// Performance configuration
	UploadConcurrency int    `yaml:"upload_concurrency" json:"upload_concurrency" default:"10"`
	PartSize          int64  `yaml:"part_size" json:"part_size" default:"5242880"` // 5MB
	MaxRetries        int    `yaml:"max_retries" json:"max_retries" default:"3"`
	AsyncBatching     bool   `yaml:"async_batching" json:"async_batching" default:"true"`
	BatchTimeout      int    `yaml:"batch_timeout" json:"batch_timeout" default:"300"` // seconds
	
	// Storage class and lifecycle
	StorageClass      string `yaml:"storage_class" json:"storage_class" default:"STANDARD"`
	ServerSideEncryption string `yaml:"server_side_encryption" json:"server_side_encryption"`
	KMSKeyID          string `yaml:"kms_key_id" json:"kms_key_id"`
}

// PostgreSQLCDCSourceConfig contains configuration for PostgreSQL CDC source connector
type PostgreSQLCDCSourceConfig struct {
	BaseConfig `yaml:",inline" json:",inline"`
	
	// PostgreSQL connection configuration
	Host     string `yaml:"host" json:"host" required:"true"`
	Port     int    `yaml:"port" json:"port" default:"5432"`
	Database string `yaml:"database" json:"database" required:"true"`
	Username string `yaml:"username" json:"username" required:"true"`
	Password string `yaml:"password" json:"password" required:"true"`
	SSLMode  string `yaml:"ssl_mode" json:"ssl_mode" default:"prefer"`
	
	// CDC configuration
	SlotName        string   `yaml:"slot_name" json:"slot_name" required:"true"`
	PublicationName string   `yaml:"publication_name" json:"publication_name" required:"true"`
	Tables          []string `yaml:"tables" json:"tables"`
	PluginName      string   `yaml:"plugin_name" json:"plugin_name" default:"pgoutput"`
	
	// Replication configuration
	StartLSN          string `yaml:"start_lsn" json:"start_lsn"`
	MaxWALSize        int64  `yaml:"max_wal_size" json:"max_wal_size" default:"1073741824"` // 1GB
	StatusInterval    int    `yaml:"status_interval" json:"status_interval" default:"10"` // seconds
	RestartOnFailure  bool   `yaml:"restart_on_failure" json:"restart_on_failure" default:"true"`
	FlushLSNInterval  int    `yaml:"flush_lsn_interval" json:"flush_lsn_interval" default:"1000"` // milliseconds
	
	// Schema handling
	IncludeSchema     bool   `yaml:"include_schema" json:"include_schema" default:"true"`
	SnapshotMode      string `yaml:"snapshot_mode" json:"snapshot_mode" default:"initial"`
	DecodeBeforeImage bool   `yaml:"decode_before_image" json:"decode_before_image" default:"true"`
}

// MySQLCDCSourceConfig contains configuration for MySQL CDC source connector
type MySQLCDCSourceConfig struct {
	BaseConfig `yaml:",inline" json:",inline"`
	
	// MySQL connection configuration
	Host     string `yaml:"host" json:"host" required:"true"`
	Port     int    `yaml:"port" json:"port" default:"3306"`
	Database string `yaml:"database" json:"database" required:"true"`
	Username string `yaml:"username" json:"username" required:"true"`
	Password string `yaml:"password" json:"password" required:"true"`
	
	// CDC configuration
	ServerID      uint32   `yaml:"server_id" json:"server_id" required:"true"`
	Tables        []string `yaml:"tables" json:"tables"`
	IncludeTables []string `yaml:"include_tables" json:"include_tables"`
	ExcludeTables []string `yaml:"exclude_tables" json:"exclude_tables"`
	
	// Binlog configuration
	BinlogFile     string `yaml:"binlog_file" json:"binlog_file"`
	BinlogPosition uint32 `yaml:"binlog_position" json:"binlog_position"`
	GTIDSet        string `yaml:"gtid_set" json:"gtid_set"`
	HeartbeatPeriod int   `yaml:"heartbeat_period" json:"heartbeat_period" default:"30"` // seconds
	
	// Schema handling
	IncludeSchema bool   `yaml:"include_schema" json:"include_schema" default:"true"`
	SnapshotMode  string `yaml:"snapshot_mode" json:"snapshot_mode" default:"initial"`
}

// ApplicationConfig is the main application configuration that uses BaseConfig principles
type ApplicationConfig struct {
	// Application-level settings
	Name        string `yaml:"name" json:"name" default:"nebula"`
	Environment string `yaml:"environment" json:"environment" default:"development"`
	Version     string `yaml:"version" json:"version" default:"1.0.0"`
	
	// Server configuration
	Server struct {
		Host        string `yaml:"host" json:"host" default:"0.0.0.0"`
		Port        int    `yaml:"port" json:"port" default:"8080"`
		MetricsPort int    `yaml:"metrics_port" json:"metrics_port" default:"9090"`
		HealthPort  int    `yaml:"health_port" json:"health_port" default:"8081"`
		GracefulShutdownTimeout int `yaml:"graceful_shutdown_timeout" json:"graceful_shutdown_timeout" default:"30"` // seconds
	} `yaml:"server" json:"server"`
	
	// Global configuration
	Global struct {
		LogLevel      string `yaml:"log_level" json:"log_level" default:"info"`
		TempDirectory string `yaml:"temp_directory" json:"temp_directory" default:"/tmp/nebula"`
		MaxMemoryMB   int    `yaml:"max_memory_mb" json:"max_memory_mb" default:"2048"`
		DataDirectory string `yaml:"data_directory" json:"data_directory" default:"./data"`
	} `yaml:"global" json:"global"`
	
	// Source and destination configurations using the new pattern
	Sources      map[string]interface{} `yaml:"sources" json:"sources"`
	Destinations map[string]interface{} `yaml:"destinations" json:"destinations"`
}