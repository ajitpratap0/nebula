package metaads

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/ajitpratap0/nebula/pkg/config"
	"github.com/ajitpratap0/nebula/pkg/connector/base"
	"github.com/ajitpratap0/nebula/pkg/connector/core"
	"github.com/ajitpratap0/nebula/pkg/errors"
	jsonpool "github.com/ajitpratap0/nebula/pkg/json"
	"github.com/ajitpratap0/nebula/pkg/models"
	"github.com/ajitpratap0/nebula/pkg/pool"
	stringpool "github.com/ajitpratap0/nebula/pkg/strings"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
)

// MetaAdsSource is a production-ready Meta Ads source connector using BaseConnector
type MetaAdsSource struct {
	*base.BaseConnector

	// Meta Marketing API configuration
	config       *MetaAdsConfig
	httpClient   *http.Client
	oauth2Config *oauth2.Config
	accessToken  string

	// API state management
	accountIDs       []string
	fields           []string
	dateRange        string
	level            string
	pageSize         int
	maxConcurrency   int
	streamingEnabled bool

	// Schema and state
	schema           *core.Schema
	lastCursor       string
	totalRecords     int64
	processedRecords int64
	isComplete       bool

	// Connection pooling
	requestPool  sync.Pool
	responsePool sync.Pool
}

// MetaAdsConfig holds Meta Marketing API configuration
type MetaAdsConfig struct {
	AccessToken      string   `json:"access_token"`
	AppID            string   `json:"app_id"`
	AppSecret        string   `json:"app_secret"`
	AccountIDs       []string `json:"account_ids"`
	Fields           []string `json:"fields"`
	Level            string   `json:"level"`       // campaign, adset, ad, creative
	DateRange        string   `json:"date_range"`  // Custom date range or preset
	DatePreset       string   `json:"date_preset"` // yesterday, last_7_days, etc.
	PageSize         int      `json:"page_size"`
	MaxConcurrency   int      `json:"max_concurrency"`
	StreamingEnabled bool     `json:"streaming_enabled"`
	APIVersion       string   `json:"api_version"` // Default: v18.0
}

// MetaAdsResponse represents the API response structure
type MetaAdsResponse struct {
	Data   []MetaAdsResult `json:"data"`
	Paging *PagingInfo     `json:"paging,omitempty"`
}

// MetaAdsResult represents a single result from Meta Marketing API
type MetaAdsResult struct {
	ID           string                 `json:"id"`
	AccountID    string                 `json:"account_id,omitempty"`
	CampaignID   string                 `json:"campaign_id,omitempty"`
	AdsetID      string                 `json:"adset_id,omitempty"`
	AdID         string                 `json:"ad_id,omitempty"`
	Name         string                 `json:"name,omitempty"`
	Status       string                 `json:"status,omitempty"`
	Objective    string                 `json:"objective,omitempty"`
	CreatedTime  string                 `json:"created_time,omitempty"`
	UpdatedTime  string                 `json:"updated_time,omitempty"`
	Insights     *InsightsData          `json:"insights,omitempty"`
	CustomFields map[string]interface{} `json:"-"` // For additional fields
}

// InsightsData represents performance metrics
type InsightsData struct {
	Impressions   string `json:"impressions,omitempty"`
	Clicks        string `json:"clicks,omitempty"`
	Spend         string `json:"spend,omitempty"`
	CPM           string `json:"cpm,omitempty"`
	CPC           string `json:"cpc,omitempty"`
	CTR           string `json:"ctr,omitempty"`
	Conversions   string `json:"conversions,omitempty"`
	CostPerResult string `json:"cost_per_result,omitempty"`
	Reach         string `json:"reach,omitempty"`
	Frequency     string `json:"frequency,omitempty"`
	DateStart     string `json:"date_start,omitempty"`
	DateStop      string `json:"date_stop,omitempty"`
}

// PagingInfo represents pagination information
type PagingInfo struct {
	Cursors *CursorInfo `json:"cursors,omitempty"`
	Next    string      `json:"next,omitempty"`
}

// CursorInfo represents cursor-based pagination
type CursorInfo struct {
	Before string `json:"before,omitempty"`
	After  string `json:"after,omitempty"`
}

// NewMetaAdsSource creates a new Meta Ads source connector
func NewMetaAdsSource(name string, config *config.BaseConfig) (core.Source, error) {
	// Create base connector with production features
	base := base.NewBaseConnector("meta_ads", core.ConnectorTypeSource, "1.0.0")

	source := &MetaAdsSource{
		BaseConnector:    base,
		pageSize:         1000,       // Default page size
		maxConcurrency:   5,          // Default concurrency
		streamingEnabled: true,       // Default streaming enabled
		level:            "campaign", // Default level
	}

	// Initialize connection pools for request/response objects
	source.requestPool = sync.Pool{
		New: func() interface{} {
			return &http.Request{}
		},
	}
	source.responsePool = sync.Pool{
		New: func() interface{} {
			return &MetaAdsResponse{}
		},
	}

	return source, nil
}

// Initialize initializes the Meta Ads source connector
func (s *MetaAdsSource) Initialize(ctx context.Context, config *config.BaseConfig) error {
	// Initialize base connector first (circuit breakers, rate limiting, health checks)
	if err := s.BaseConnector.Initialize(ctx, config); err != nil {
		return errors.Wrap(err, errors.ErrorTypeConfig, "failed to initialize base connector")
	}

	// Validate and extract configuration
	if err := s.validateAndExtractConfig(config); err != nil {
		return err
	}

	// Initialize HTTP client with circuit breaker protection
	if err := s.ExecuteWithCircuitBreaker(func() error {
		return s.initializeHTTPClient()
	}); err != nil {
		return errors.Wrap(err, errors.ErrorTypeConnection, "failed to initialize HTTP client")
	}

	// Validate access token
	if err := s.validateAccessToken(ctx); err != nil {
		return errors.Wrap(err, errors.ErrorTypeAuthentication, "failed to validate access token")
	}

	// Discover schema based on level and fields
	if err := s.discoverSchema(ctx); err != nil {
		return errors.Wrap(err, errors.ErrorTypeData, "failed to discover schema")
	}

	// Update health status
	s.UpdateHealth(true, map[string]interface{}{
		"access_token_valid": true,
		"accounts_count":     len(s.accountIDs),
		"level":              s.level,
		"fields_count":       len(s.fields),
		"streaming_enabled":  s.streamingEnabled,
	})

	s.GetLogger().Info("Meta Ads source initialized successfully",
		zap.Strings("account_ids", s.accountIDs),
		zap.String("level", s.level),
		zap.Strings("fields", s.fields),
		zap.String("date_range", s.dateRange),
		zap.Int("page_size", s.pageSize),
		zap.Int("max_concurrency", s.maxConcurrency))

	return nil
}

// validateAndExtractConfig validates and extracts Meta Ads configuration
func (s *MetaAdsSource) validateAndExtractConfig(config *config.BaseConfig) error {
	if config == nil {
		return errors.New(errors.ErrorTypeConfig, "configuration is required")
	}

	// For now, we'll use a simple approach and expect properties in Security.Credentials
	properties := config.Security.Credentials
	if properties == nil {
		return errors.New(errors.ErrorTypeConfig, "credentials are required")
	}

	// Extract configuration with validation
	metaAdsConfig := &MetaAdsConfig{}

	// Required fields
	if accessToken, ok := properties["access_token"]; ok && accessToken != "" {
		metaAdsConfig.AccessToken = accessToken
	} else {
		return errors.New(errors.ErrorTypeConfig, "access_token is required")
	}

	if appID, ok := properties["app_id"]; ok && appID != "" {
		metaAdsConfig.AppID = appID
	} else {
		return errors.New(errors.ErrorTypeConfig, "app_id is required")
	}

	if appSecret, ok := properties["app_secret"]; ok && appSecret != "" {
		metaAdsConfig.AppSecret = appSecret
	} else {
		return errors.New(errors.ErrorTypeConfig, "app_secret is required")
	}

	// Account IDs - for simplicity, expect comma-separated string
	if accountIDsStr, ok := properties["account_ids"]; ok && accountIDsStr != "" {
		metaAdsConfig.AccountIDs = stringpool.Split(accountIDsStr, ",")
		for i, id := range metaAdsConfig.AccountIDs {
			metaAdsConfig.AccountIDs[i] = stringpool.TrimSpace(id)
		}
	}

	if len(metaAdsConfig.AccountIDs) == 0 {
		return errors.New(errors.ErrorTypeConfig, "at least one account_id is required")
	}

	// Fields to extract - expect comma-separated string
	if fieldsStr, ok := properties["fields"]; ok && fieldsStr != "" {
		metaAdsConfig.Fields = stringpool.Split(fieldsStr, ",")
		for i, field := range metaAdsConfig.Fields {
			metaAdsConfig.Fields[i] = stringpool.TrimSpace(field)
		}
	}

	// Default fields if none specified
	if len(metaAdsConfig.Fields) == 0 {
		metaAdsConfig.Fields = s.getDefaultFields()
	}

	// Optional fields with defaults
	if level, ok := properties["level"]; ok && level != "" {
		metaAdsConfig.Level = level
	} else {
		metaAdsConfig.Level = s.level // Use default
	}

	if dateRange, ok := properties["date_range"]; ok && dateRange != "" {
		metaAdsConfig.DateRange = dateRange
	} else if datePreset, ok := properties["date_preset"]; ok && datePreset != "" {
		metaAdsConfig.DatePreset = datePreset
		metaAdsConfig.DateRange = datePreset
	} else {
		metaAdsConfig.DateRange = "last_7_days" // Default
	}

	metaAdsConfig.PageSize = config.Performance.BatchSize
	if metaAdsConfig.PageSize <= 0 {
		metaAdsConfig.PageSize = s.pageSize // Use default
	}

	metaAdsConfig.MaxConcurrency = config.Performance.MaxConcurrency
	if metaAdsConfig.MaxConcurrency <= 0 {
		metaAdsConfig.MaxConcurrency = s.maxConcurrency // Use default
	}

	metaAdsConfig.StreamingEnabled = config.Performance.EnableStreaming

	if apiVersion, ok := properties["api_version"]; ok && apiVersion != "" {
		metaAdsConfig.APIVersion = apiVersion
	} else {
		metaAdsConfig.APIVersion = "v18.0" // Default
	}

	// Store configuration
	s.config = metaAdsConfig
	s.accountIDs = metaAdsConfig.AccountIDs
	s.fields = metaAdsConfig.Fields
	s.level = metaAdsConfig.Level
	s.dateRange = metaAdsConfig.DateRange
	s.pageSize = metaAdsConfig.PageSize
	s.maxConcurrency = metaAdsConfig.MaxConcurrency
	s.streamingEnabled = metaAdsConfig.StreamingEnabled
	s.accessToken = metaAdsConfig.AccessToken

	return nil
}

// getDefaultFields returns default fields based on level
func (s *MetaAdsSource) getDefaultFields() []string {
	switch s.level {
	case "campaign":
		return []string{"id", "name", "status", "objective", "created_time", "updated_time"}
	case "adset":
		return []string{"id", "name", "status", "campaign_id", "created_time", "updated_time"}
	case "ad":
		return []string{"id", "name", "status", "adset_id", "created_time", "updated_time"}
	case "creative":
		return []string{"id", "name", "status", "created_time", "updated_time"}
	default:
		return []string{"id", "name", "status", "created_time", "updated_time"}
	}
}

// initializeHTTPClient initializes the HTTP client
func (s *MetaAdsSource) initializeHTTPClient() error {
	// Create HTTP client with timeouts and rate limiting
	s.httpClient = &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	return nil
}

// validateAccessToken validates the access token by making a test API call
func (s *MetaAdsSource) validateAccessToken(ctx context.Context) error {
	// Apply rate limiting
	if err := s.RateLimit(ctx); err != nil {
		return err
	}

	// Test token with a simple API call using URLBuilder
	ub := stringpool.NewURLBuilder(stringpool.Sprintf("https://graph.facebook.com/%s/me", s.config.APIVersion))
	defer ub.Close()
	testURL := ub.AddParam("access_token", s.accessToken).String()

	req, err := http.NewRequestWithContext(ctx, "GET", testURL, nil)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeConnection, "failed to create test request")
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeConnection, "test request failed")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Use pooled buffer for error response reading
		errorBuffer := stringpool.GetBuilder(stringpool.Small)
		defer stringpool.PutBuilder(errorBuffer, stringpool.Small)

		io.Copy(errorBuffer, resp.Body)
		return errors.New(errors.ErrorTypeAuthentication,
			stringpool.Sprintf("access token validation failed: %d %s", resp.StatusCode, errorBuffer.String()))
	}

	s.GetLogger().Debug("Access token validated successfully")
	return nil
}

// Discover implements schema discovery for Meta Marketing API
func (s *MetaAdsSource) Discover(ctx context.Context) (*core.Schema, error) {
	if s.schema != nil {
		return s.schema, nil
	}

	if err := s.discoverSchema(ctx); err != nil {
		return nil, err
	}

	return s.schema, nil
}

// discoverSchema creates schema based on level and fields
func (s *MetaAdsSource) discoverSchema(ctx context.Context) error {
	// Create schema based on level and fields
	schema := &core.Schema{
		Name:        stringpool.Sprintf("meta_ads_%s", s.level),
		Description: stringpool.Sprintf("Meta Ads API data for %s level", s.level),
		Fields:      make([]core.Field, 0, len(s.fields)+10), // Extra space for insights
	}

	// Add basic fields
	for _, fieldName := range s.fields {
		field := core.Field{
			Name:        fieldName,
			Type:        s.inferFieldType(fieldName),
			Description: stringpool.Sprintf("Meta Ads field: %s", fieldName),
			Nullable:    true,
		}
		schema.Fields = append(schema.Fields, field)
	}

	// Add common insights fields if insights are included
	insightsFields := []string{
		"impressions", "clicks", "spend", "cpm", "cpc", "ctr",
		"conversions", "cost_per_result", "reach", "frequency",
		"date_start", "date_stop",
	}

	for _, insightField := range insightsFields {
		field := core.Field{
			Name:        stringpool.Sprintf("insights_%s", insightField),
			Type:        s.inferFieldType(insightField),
			Description: stringpool.Sprintf("Meta Ads insights field: %s", insightField),
			Nullable:    true,
		}
		schema.Fields = append(schema.Fields, field)
	}

	// Always add account_id field
	schema.Fields = append(schema.Fields, core.Field{
		Name:        "account_id",
		Type:        core.FieldTypeString,
		Description: "Meta Ads Account ID",
		Nullable:    false,
	})

	s.schema = schema
	return nil
}

// inferFieldType infers the field type based on field name
func (s *MetaAdsSource) inferFieldType(fieldName string) core.FieldType {
	fieldName = strings.ToLower(fieldName)

	// Metrics and insights are typically numeric
	if strings.Contains(fieldName, "impressions") || strings.Contains(fieldName, "clicks") ||
		strings.Contains(fieldName, "reach") || strings.Contains(fieldName, "frequency") {
		return core.FieldTypeInt
	}

	// Money and rates are floats
	if strings.Contains(fieldName, "spend") || strings.Contains(fieldName, "cost") ||
		strings.Contains(fieldName, "cpm") || strings.Contains(fieldName, "cpc") ||
		strings.Contains(fieldName, "ctr") || strings.Contains(fieldName, "conversions") {
		return core.FieldTypeFloat
	}

	// IDs are typically strings (can be large numbers)
	if strings.Contains(fieldName, "id") {
		return core.FieldTypeString
	}

	// Dates and timestamps
	if strings.Contains(fieldName, "date") || strings.Contains(fieldName, "time") {
		return core.FieldTypeString
	}

	// Status and other enum fields
	if strings.Contains(fieldName, "status") || strings.Contains(fieldName, "objective") {
		return core.FieldTypeString
	}

	// Default to string for text fields
	return core.FieldTypeString
}

// Read implements streaming read with backpressure support
func (s *MetaAdsSource) Read(ctx context.Context) (*core.RecordStream, error) {
	recordsChan := make(chan *models.Record, s.pageSize)
	errorsChan := make(chan error, 10)

	stream := &core.RecordStream{
		Records: recordsChan,
		Errors:  errorsChan,
	}

	// Start reading in background goroutine
	go func() {
		defer close(recordsChan)
		defer close(errorsChan)

		if err := s.readRecords(ctx, recordsChan, errorsChan); err != nil {
			errorsChan <- err
		}
	}()

	return stream, nil
}

// ReadBatch implements batch reading for better performance
func (s *MetaAdsSource) ReadBatch(ctx context.Context, batchSize int) (*core.BatchStream, error) {
	batchesChan := make(chan []*models.Record, 10)
	errorsChan := make(chan error, 10)

	stream := &core.BatchStream{
		Batches: batchesChan,
		Errors:  errorsChan,
	}

	// Start reading in background goroutine
	go func() {
		defer close(batchesChan)
		defer close(errorsChan)

		if err := s.readBatches(ctx, batchSize, batchesChan, errorsChan); err != nil {
			errorsChan <- err
		}
	}()

	return stream, nil
}

// readRecords reads records from Meta Marketing API with concurrent processing
func (s *MetaAdsSource) readRecords(ctx context.Context, recordsChan chan<- *models.Record, errorsChan chan<- error) error {
	// Process each account concurrently up to maxConcurrency
	semaphore := make(chan struct{}, s.maxConcurrency)
	var wg sync.WaitGroup

	for _, accountID := range s.accountIDs {
		wg.Add(1)
		go func(accID string) {
			defer wg.Done()

			// Acquire semaphore
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			if err := s.readAccountRecords(ctx, accID, recordsChan, errorsChan); err != nil {
				s.GetLogger().Error("Failed to read account records",
					zap.String("account_id", accID),
					zap.Error(err))
				errorsChan <- err
			}
		}(accountID)
	}

	wg.Wait()
	return nil
}

// readBatches reads records in batches for better performance
func (s *MetaAdsSource) readBatches(ctx context.Context, batchSize int, batchesChan chan<- []*models.Record, errorsChan chan<- error) error {
	// Create a temporary channel to collect individual records
	recordsChan := make(chan *models.Record, s.pageSize)

	// Start record collection
	go func() {
		defer close(recordsChan)
		if err := s.readRecords(ctx, recordsChan, errorsChan); err != nil {
			s.GetLogger().Error("Failed to read records for batching", zap.Error(err))
		}
	}()

	// Batch the records
	batch := pool.GetBatchSlice(batchSize)

	defer pool.PutBatchSlice(batch)
	for record := range recordsChan {
		batch = append(batch, record)

		if len(batch) >= batchSize {
			// Send batch
			select {
			case batchesChan <- batch:
				batch = pool.GetBatchSlice(batchSize) // Reset batch
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	// Send remaining records in final batch
	if len(batch) > 0 {
		select {
		case batchesChan <- batch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// readAccountRecords reads all records for a specific account
func (s *MetaAdsSource) readAccountRecords(ctx context.Context, accountID string, recordsChan chan<- *models.Record, errorsChan chan<- error) error {
	cursor := ""

	for {
		// Apply rate limiting before each API call
		if err := s.RateLimit(ctx); err != nil {
			return err
		}

		// Execute API call with circuit breaker protection
		var response *MetaAdsResponse
		if err := s.ExecuteWithCircuitBreaker(func() error {
			var err error
			response, err = s.makeAPIRequest(ctx, accountID, cursor)
			return err
		}); err != nil {
			return errors.Wrap(err, errors.ErrorTypeConnection,
				stringpool.Sprintf("API request failed for account %s", accountID))
		}

		// Process response records
		for _, result := range response.Data {
			record, err := s.convertToRecord(result, accountID)
			if err != nil {
				s.GetLogger().Error("Failed to convert result to record",
					zap.String("account_id", accountID),
					zap.Error(err))
				continue
			}

			select {
			case recordsChan <- record:
				s.processedRecords++
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// Check for more pages
		if response.Paging == nil || response.Paging.Next == "" {
			break
		}

		// Extract cursor from next URL
		if response.Paging.Cursors != nil && response.Paging.Cursors.After != "" {
			cursor = response.Paging.Cursors.After
		} else {
			// Parse cursor from next URL if not in cursors
			if nextURL := response.Paging.Next; nextURL != "" {
				if parsedURL, err := url.Parse(nextURL); err == nil {
					cursor = parsedURL.Query().Get("after")
				}
			}
		}

		// Update progress
		s.ReportProgress(s.processedRecords, s.totalRecords)
	}

	return nil
}

// makeAPIRequest makes a request to Meta Marketing API
func (s *MetaAdsSource) makeAPIRequest(ctx context.Context, accountID, cursor string) (*MetaAdsResponse, error) {
	// Build request URL based on level using optimized string building
	var endpoint string
	switch s.level {
	case "campaign":
		endpoint = "campaigns"
	case "adset":
		endpoint = "adsets"
	case "ad":
		endpoint = "ads"
	case "creative":
		endpoint = "adcreatives"
	default:
		endpoint = "campaigns"
	}

	// Build API URL using URLBuilder for optimized string handling
	ub := stringpool.NewURLBuilder(stringpool.Sprintf("https://graph.facebook.com/%s/act_%s/%s",
		s.config.APIVersion, accountID, endpoint))
	defer ub.Close()

	// Add query parameters
	ub.AddParam("access_token", s.accessToken)
	ub.AddParam("fields", stringpool.JoinPooled(s.fields, ","))
	ub.AddParamInt("limit", s.pageSize)

	// Add date filtering if specified
	if s.dateRange != "" {
		// If it's a preset, use date_preset; otherwise use time_range
		if s.isDatePreset(s.dateRange) {
			ub.AddParam("date_preset", s.dateRange)
		} else {
			ub.AddParam("time_range", s.dateRange)
		}
	}

	// Add cursor for pagination
	if cursor != "" {
		ub.AddParam("after", cursor)
	}

	// Get the full URL
	fullURL := ub.String()

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, "GET", fullURL, nil)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrorTypeConnection, "failed to create HTTP request")
	}

	// Set headers
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "Nebula-Meta-Ads-Connector/1.0")

	// Make request
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrorTypeConnection, "HTTP request failed")
	}
	defer resp.Body.Close()

	// Check for HTTP errors
	if resp.StatusCode != http.StatusOK {
		// Use pooled buffer for error response reading
		errorBuffer := stringpool.GetBuilder(stringpool.Small)
		defer stringpool.PutBuilder(errorBuffer, stringpool.Small)

		io.Copy(errorBuffer, resp.Body)
		return nil, errors.New(errors.ErrorTypeConnection,
			stringpool.Sprintf("API returned status %d: %s", resp.StatusCode, errorBuffer.String()))
	}

	// Parse response
	responseData := s.responsePool.Get().(*MetaAdsResponse)
	defer s.responsePool.Put(responseData)

	decoder := jsonpool.GetDecoder(resp.Body)
	defer jsonpool.PutDecoder(decoder)
	if err := decoder.Decode(responseData); err != nil {
		return nil, errors.Wrap(err, errors.ErrorTypeData, "failed to decode API response")
	}

	// Return a copy since we're putting the original back in the pool
	responseCopy := &MetaAdsResponse{
		Data:   make([]MetaAdsResult, len(responseData.Data)),
		Paging: responseData.Paging,
	}
	copy(responseCopy.Data, responseData.Data)

	return responseCopy, nil
}

// isDatePreset checks if the date range is a Meta Ads preset
func (s *MetaAdsSource) isDatePreset(dateRange string) bool {
	presets := []string{
		"today", "yesterday", "this_month", "last_month", "this_quarter", "last_quarter",
		"this_year", "last_year", "last_3d", "last_7d", "last_14d", "last_28d", "last_30d",
		"last_90d", "last_week_mon_sun", "last_week_sun_sat", "last_quarter_3x30",
		"last_year_3x30", "this_week_mon_today", "this_week_sun_today", "this_month_max",
	}

	for _, preset := range presets {
		if preset == dateRange {
			return true
		}
	}
	return false
}

// convertToRecord converts a Meta Ads result to a models.Record
func (s *MetaAdsSource) convertToRecord(result MetaAdsResult, accountID string) (*models.Record, error) {
	// Get record from memory pool
	record := pool.NewRecordFromPool("meta_ads")

	// Populate record data using SetData to preserve pooled map

	// Add account ID
	record.SetData("account_id", accountID)

	// Add basic fields
	if result.ID != "" {
		record.SetData("id", result.ID)
	}
	if result.Name != "" {
		record.SetData("name", result.Name)
	}
	if result.Status != "" {
		record.SetData("status", result.Status)
	}
	if result.Objective != "" {
		record.SetData("objective", result.Objective)
	}
	if result.CreatedTime != "" {
		record.SetData("created_time", result.CreatedTime)
	}
	if result.UpdatedTime != "" {
		record.SetData("updated_time", result.UpdatedTime)
	}

	// Add level-specific IDs
	if result.CampaignID != "" {
		record.SetData("campaign_id", result.CampaignID)
	}
	if result.AdsetID != "" {
		record.SetData("adset_id", result.AdsetID)
	}
	if result.AdID != "" {
		record.SetData("ad_id", result.AdID)
	}

	// Add insights data if available
	if result.Insights != nil {
		insights := result.Insights
		if insights.Impressions != "" {
			record.SetData("insights_impressions", insights.Impressions)
		}
		if insights.Clicks != "" {
			record.SetData("insights_clicks", insights.Clicks)
		}
		if insights.Spend != "" {
			record.SetData("insights_spend", insights.Spend)
		}
		if insights.CPM != "" {
			record.SetData("insights_cpm", insights.CPM)
		}
		if insights.CPC != "" {
			record.SetData("insights_cpc", insights.CPC)
		}
		if insights.CTR != "" {
			record.SetData("insights_ctr", insights.CTR)
		}
		if insights.Conversions != "" {
			record.SetData("insights_conversions", insights.Conversions)
		}
		if insights.CostPerResult != "" {
			record.SetData("insights_cost_per_result", insights.CostPerResult)
		}
		if insights.Reach != "" {
			record.SetData("insights_reach", insights.Reach)
		}
		if insights.Frequency != "" {
			record.SetData("insights_frequency", insights.Frequency)
		}
		if insights.DateStart != "" {
			record.SetData("insights_date_start", insights.DateStart)
		}
		if insights.DateStop != "" {
			record.SetData("insights_date_stop", insights.DateStop)
		}
	}

	// Add any custom fields
	for key, value := range result.CustomFields {
		record.SetData(stringpool.Sprintf("custom_%s", key), value)
	}

	// Set record properties
	record.ID = stringpool.Sprintf("%s_%s_%d", accountID, result.ID, time.Now().UnixNano())
	record.Metadata.Source = "meta_ads"
	// Data already populated via SetData calls
	record.SetMetadata("account_id", accountID)
	record.SetMetadata("level", s.level)
	record.SetMetadata("extracted_at", time.Now().Unix())
	record.SetMetadata("api_version", s.config.APIVersion)
	record.SetTimestamp(time.Now())

	return record, nil
}

// Close closes the Meta Ads source connector
func (s *MetaAdsSource) Close(ctx context.Context) error {
	s.GetLogger().Info("Closing Meta Ads source connector")

	// Close base connector
	return s.BaseConnector.Close(ctx)
}

// MetaAdsPosition implements core.Position for Meta Ads
type MetaAdsPosition struct {
	LastCursor       string `json:"last_cursor"`
	ProcessedRecords int64  `json:"processed_records"`
}

func (p *MetaAdsPosition) String() string {
	return stringpool.Sprintf("cursor:%s,records:%d", p.LastCursor, p.ProcessedRecords)
}

func (p *MetaAdsPosition) Compare(other core.Position) int {
	if otherPos, ok := other.(*MetaAdsPosition); ok {
		if p.ProcessedRecords < otherPos.ProcessedRecords {
			return -1
		} else if p.ProcessedRecords > otherPos.ProcessedRecords {
			return 1
		}
		return 0
	}
	return 0
}

// GetPosition returns the current position (cursor)
func (s *MetaAdsSource) GetPosition() core.Position {
	return &MetaAdsPosition{
		LastCursor:       s.lastCursor,
		ProcessedRecords: s.processedRecords,
	}
}

// SetPosition sets the position (cursor)
func (s *MetaAdsSource) SetPosition(position core.Position) error {
	if metaAdsPos, ok := position.(*MetaAdsPosition); ok {
		s.lastCursor = metaAdsPos.LastCursor
		s.processedRecords = metaAdsPos.ProcessedRecords
	}
	return nil
}

// GetState returns the current state
func (s *MetaAdsSource) GetState() core.State {
	return core.State{
		"account_ids":       s.accountIDs,
		"level":             s.level,
		"fields":            s.fields,
		"date_range":        s.dateRange,
		"last_cursor":       s.lastCursor,
		"processed_records": s.processedRecords,
		"is_complete":       s.isComplete,
	}
}

// SetState sets the state
func (s *MetaAdsSource) SetState(state core.State) error {
	if accountIDs, ok := state["account_ids"].([]string); ok {
		s.accountIDs = accountIDs
	}
	if level, ok := state["level"].(string); ok {
		s.level = level
	}
	if fields, ok := state["fields"].([]string); ok {
		s.fields = fields
	}
	if dateRange, ok := state["date_range"].(string); ok {
		s.dateRange = dateRange
	}
	if cursor, ok := state["last_cursor"].(string); ok {
		s.lastCursor = cursor
	}
	if records, ok := state["processed_records"].(int64); ok {
		s.processedRecords = records
	}
	if complete, ok := state["is_complete"].(bool); ok {
		s.isComplete = complete
	}
	return nil
}

// SupportsIncremental returns true if incremental sync is supported
func (s *MetaAdsSource) SupportsIncremental() bool {
	return true // Meta Ads supports date-based incremental sync
}

// SupportsRealtime returns true if real-time sync is supported
func (s *MetaAdsSource) SupportsRealtime() bool {
	return false // Meta Ads doesn't support real-time streaming
}

// SupportsBatch returns true if batch operations are supported
func (s *MetaAdsSource) SupportsBatch() bool {
	return true
}

// Subscribe implements CDC/real-time subscription (not supported for Meta Ads)
func (s *MetaAdsSource) Subscribe(ctx context.Context, tables []string) (*core.ChangeStream, error) {
	return nil, errors.New(errors.ErrorTypeCapability, "Meta Ads does not support real-time subscriptions")
}

// Health performs health check
func (s *MetaAdsSource) Health(ctx context.Context) error {
	// Check if access token is valid by making a simple API call
	if s.accessToken == "" {
		return errors.New(errors.ErrorTypeAuthentication, "no access token available")
	}

	// Try to validate token to verify connectivity
	return s.validateAccessToken(ctx)
}

// Metrics returns connector metrics
func (s *MetaAdsSource) Metrics() map[string]interface{} {
	return map[string]interface{}{
		"type":              "meta_ads",
		"accounts_count":    len(s.accountIDs),
		"processed_records": s.processedRecords,
		"level":             s.level,
		"streaming_enabled": s.streamingEnabled,
		"page_size":         s.pageSize,
		"max_concurrency":   s.maxConcurrency,
		"is_complete":       s.isComplete,
	}
}
