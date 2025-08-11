package google_ads

import (
	"context"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ajitpratap0/nebula/pkg/auth"
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

// GoogleAdsSource is a production-ready Google Ads source connector using BaseConnector
type GoogleAdsSource struct {
	*base.BaseConnector

	// Google Ads API configuration
	config       *GoogleAdsConfig
	httpClient   *http.Client
	oauth2Config *oauth2.Config
	accessToken  string
	refreshToken string

	// Dynamic authentication
	authService *auth.PlatformAuthService
	credentials *auth.GoogleAdsCredentials

	// API state management
	customerIDs      []string
	query            string
	pageSize         int
	maxConcurrency   int
	streamingEnabled bool

	// Schema and state
	schema           *core.Schema
	lastPageToken    string
	totalRecords     int64
	processedRecords int64
	isComplete       bool

	// Connection pooling
	requestPool  sync.Pool
	responsePool sync.Pool
}

// GoogleAdsConfig holds Google Ads API configuration
type GoogleAdsConfig struct {
	// Dynamic authentication fields
	AccountID  string `json:"account_id"`   // TMS account ID for token fetching
	Platform   string `json:"platform"`     // Platform name (e.g., "GOOGLE")
	TMSBaseURL string `json:"tms_base_url"` // TMS service base URL
	TMSAPIKey  string `json:"tms_api_key"`  // TMS API key

	// Legacy fields (optional, for backward compatibility)
	DeveloperToken  string `json:"developer_token,omitempty"`
	ClientID        string `json:"client_id,omitempty"`
	ClientSecret    string `json:"client_secret,omitempty"`
	RefreshToken    string `json:"refresh_token,omitempty"`
	LoginCustomerID string `json:"login_customer_id,omitempty"`

	// Query and performance settings
	CustomerIDs      []string `json:"customer_ids"`
	Query            string   `json:"query"`
	PageSize         int      `json:"page_size"`
	MaxConcurrency   int      `json:"max_concurrency"`
	StreamingEnabled bool     `json:"streaming_enabled"`
}

// GoogleAdsResponse represents the API response structure
type GoogleAdsResponse struct {
	Results       []GoogleAdsResult `json:"results"`
	NextPageToken string            `json:"nextPageToken,omitempty"`
	Summary       ResponseSummary   `json:"summary,omitempty"`
}

// GoogleAdsResult represents a single result from Google Ads API
type GoogleAdsResult struct {
	Campaign   interface{} `json:"campaign,omitempty"`
	AdGroup    interface{} `json:"adGroup,omitempty"`
	Keyword    interface{} `json:"keyword,omitempty"`
	Metrics    interface{} `json:"metrics,omitempty"`
	Segments   interface{} `json:"segments,omitempty"`
	CustomerID string      `json:"customer_id"`
}

// ResponseSummary contains response metadata
type ResponseSummary struct {
	TotalResults       int64 `json:"totalResults"`
	TotalRowsRetrieved int   `json:"totalRowsRetrieved"`
}

// NewGoogleAdsSource creates a new Google Ads source connector
func NewGoogleAdsSource(name string, config *config.BaseConfig) (core.Source, error) {
	// Create base connector with production features
	base := base.NewBaseConnector("google_ads", core.ConnectorTypeSource, "1.0.0")

	source := &GoogleAdsSource{
		BaseConnector:    base,
		pageSize:         10000, // Default page size
		maxConcurrency:   5,     // Default concurrency
		streamingEnabled: true,  // Default streaming enabled
	}

	// Initialize connection pools for request/response objects
	source.requestPool = sync.Pool{
		New: func() interface{} {
			return &http.Request{}
		},
	}
	source.responsePool = sync.Pool{
		New: func() interface{} {
			return &GoogleAdsResponse{}
		},
	}

	return source, nil
}

// Initialize initializes the Google Ads source connector
func (s *GoogleAdsSource) Initialize(ctx context.Context, config *config.BaseConfig) error {
	// Initialize base connector first (circuit breakers, rate limiting, health checks)
	if err := s.BaseConnector.Initialize(ctx, config); err != nil {
		return errors.Wrap(err, errors.ErrorTypeConfig, "failed to initialize base connector")
	}

	// Validate and extract configuration
	if err := s.validateAndExtractConfig(config); err != nil {
		return err
	}

	// Initialize authentication (OAuth2 client or dynamic credentials)
	if err := s.ExecuteWithCircuitBreaker(func() error {
		return s.initializeAuthentication(ctx)
	}); err != nil {
		return errors.Wrap(err, errors.ErrorTypeConnection, "failed to initialize authentication")
	}

	// Discover schema based on GAQL query
	if err := s.discoverSchema(ctx); err != nil {
		return errors.Wrap(err, errors.ErrorTypeData, "failed to discover schema")
	}

	// Update health status
	s.UpdateHealth(true, map[string]interface{}{
		"oauth2_configured": true,
		"customers_count":   len(s.customerIDs),
		"query":             s.query,
		"streaming_enabled": s.streamingEnabled,
	})

	s.GetLogger().Info("Google Ads source initialized successfully",
		zap.Strings("customer_ids", s.customerIDs),
		zap.String("query", s.query),
		zap.Int("page_size", s.pageSize),
		zap.Int("max_concurrency", s.maxConcurrency))

	return nil
}

// validateAndExtractConfig validates and extracts Google Ads configuration
func (s *GoogleAdsSource) validateAndExtractConfig(config *config.BaseConfig) error {
	if config == nil {
		return errors.New(errors.ErrorTypeConfig, "configuration is required")
	}

	// For now, we'll use a simple approach and expect properties in Security.Credentials
	properties := config.Security.Credentials
	if properties == nil {
		return errors.New(errors.ErrorTypeConfig, "credentials are required")
	}

	// Extract configuration with validation
	googleAdsConfig := &GoogleAdsConfig{}

	// Check if using dynamic authentication (TMS service)
	tmsBaseURL := os.Getenv("TMS_BASE_URL")
	tmsAPIKey := os.Getenv("TMS_API_KEY")
	if tmsBaseURL != "" && tmsAPIKey != "" {
		// Dynamic authentication mode
		googleAdsConfig.TMSBaseURL = tmsBaseURL
		googleAdsConfig.TMSAPIKey = tmsAPIKey

		if accountID, ok := properties["account_id"]; ok && accountID != "" {
			googleAdsConfig.AccountID = accountID
		} else {
			return errors.New(errors.ErrorTypeConfig, "account_id is required for dynamic authentication")
		}

		if platform, ok := properties["platform"]; ok && platform != "" {
			googleAdsConfig.Platform = platform
		} else {
			googleAdsConfig.Platform = "GOOGLE" // Default to Google
		}

		// Initialize authentication service
		s.authService = auth.NewPlatformAuthService(googleAdsConfig.TMSBaseURL, googleAdsConfig.TMSAPIKey)
	} else {
		// Legacy authentication mode - require all OAuth2 fields
		if developerToken, ok := properties["developer_token"]; ok && developerToken != "" {
			googleAdsConfig.DeveloperToken = developerToken
		} else {
			return errors.New(errors.ErrorTypeConfig, "developer_token is required")
		}

		if clientID, ok := properties["client_id"]; ok && clientID != "" {
			googleAdsConfig.ClientID = clientID
		} else {
			return errors.New(errors.ErrorTypeConfig, "client_id is required")
		}

		if clientSecret, ok := properties["client_secret"]; ok && clientSecret != "" {
			googleAdsConfig.ClientSecret = clientSecret
		} else {
			return errors.New(errors.ErrorTypeConfig, "client_secret is required")
		}

		if refreshToken, ok := properties["refresh_token"]; ok && refreshToken != "" {
			googleAdsConfig.RefreshToken = refreshToken
		} else {
			return errors.New(errors.ErrorTypeConfig, "refresh_token is required")
		}
	}

	if query, ok := properties["query"]; ok && query != "" {
		googleAdsConfig.Query = query
	} else {
		return errors.New(errors.ErrorTypeConfig, "GAQL query is required")
	}

	// Customer IDs - for simplicity, expect comma-separated string
	if customerIDsStr, ok := properties["customer_ids"]; ok && customerIDsStr != "" {
		googleAdsConfig.CustomerIDs = stringpool.Split(customerIDsStr, ",")
		for i, id := range googleAdsConfig.CustomerIDs {
			googleAdsConfig.CustomerIDs[i] = stringpool.TrimSpace(id)
		}
	}

	if len(googleAdsConfig.CustomerIDs) == 0 {
		return errors.New(errors.ErrorTypeConfig, "at least one customer_id is required")
	}

	// Optional fields with defaults
	googleAdsConfig.PageSize = config.Performance.BatchSize
	if googleAdsConfig.PageSize <= 0 {
		googleAdsConfig.PageSize = s.pageSize // Use default
	}

	googleAdsConfig.MaxConcurrency = config.Performance.MaxConcurrency
	if googleAdsConfig.MaxConcurrency <= 0 {
		googleAdsConfig.MaxConcurrency = s.maxConcurrency // Use default
	}

	googleAdsConfig.StreamingEnabled = config.Performance.EnableStreaming

	if loginCustomerID, ok := properties["login_customer_id"]; ok {
		googleAdsConfig.LoginCustomerID = loginCustomerID
	}

	// Store configuration
	s.config = googleAdsConfig
	s.customerIDs = googleAdsConfig.CustomerIDs
	s.query = googleAdsConfig.Query
	s.pageSize = googleAdsConfig.PageSize
	s.maxConcurrency = googleAdsConfig.MaxConcurrency
	s.streamingEnabled = googleAdsConfig.StreamingEnabled
	s.refreshToken = googleAdsConfig.RefreshToken

	return nil
}

// initializeAuthentication initializes authentication (dynamic or legacy OAuth2)
func (s *GoogleAdsSource) initializeAuthentication(ctx context.Context) error {
	if s.authService != nil {
		// Dynamic authentication mode
		return s.initializeDynamicAuth(ctx)
	} else {
		// Legacy OAuth2 mode
		if err := s.initializeOAuth2Client(); err != nil {
			return err
		}
		return s.refreshAccessToken(ctx)
	}
}

// initializeDynamicAuth fetches credentials from TMS service
func (s *GoogleAdsSource) initializeDynamicAuth(ctx context.Context) error {
	// Fetch credentials from TMS service
	creds, err := s.authService.GetGoogleAdsCredentials(ctx, s.config.AccountID)
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeAuthentication, "failed to fetch credentials from TMS")
	}

	// Check if token is expired
	if creds.IsTokenExpired() {
		s.GetLogger().Warn("Access token is expired or will expire soon",
			zap.Time("expires_at", creds.ExpiresAt))
		// Note: In a production system, you might want to refresh the token here
		// For now, we'll use the token as-is and let the API call handle the refresh
	}

	// Store credentials
	s.credentials = creds
	s.accessToken = creds.AccessToken
	s.refreshToken = creds.RefreshToken

	// Update config with fetched credentials for compatibility
	s.config.DeveloperToken = creds.DeveloperToken
	s.config.ClientID = creds.ClientID
	s.config.ClientSecret = creds.ClientSecret
	s.config.RefreshToken = creds.RefreshToken
	s.config.LoginCustomerID = creds.LoginCustomerID

	// Initialize OAuth2 config for potential token refresh
	s.oauth2Config = &oauth2.Config{
		ClientID:     creds.ClientID,
		ClientSecret: creds.ClientSecret,
		Endpoint: oauth2.Endpoint{
			TokenURL: "https://oauth2.googleapis.com/token",
		},
		Scopes: []string{"https://www.googleapis.com/auth/adwords"},
	}

	// Create HTTP client
	s.httpClient = &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	s.GetLogger().Info("Dynamic authentication initialized successfully",
		zap.String("account_id", s.config.AccountID),
		zap.String("platform", s.config.Platform),
		zap.String("login_customer_id", creds.LoginCustomerID),
		zap.Time("expires_at", creds.ExpiresAt))

	return nil
}

// initializeOAuth2Client initializes the OAuth2 configuration
func (s *GoogleAdsSource) initializeOAuth2Client() error {
	s.oauth2Config = &oauth2.Config{
		ClientID:     s.config.ClientID,
		ClientSecret: s.config.ClientSecret,
		Endpoint: oauth2.Endpoint{
			TokenURL: "https://oauth2.googleapis.com/token",
		},
		Scopes: []string{"https://www.googleapis.com/auth/adwords"},
	}

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

// refreshAccessToken obtains a new access token using the refresh token
func (s *GoogleAdsSource) refreshAccessToken(ctx context.Context) error {
	// Apply rate limiting
	if err := s.RateLimit(ctx); err != nil {
		return err
	}

	token := &oauth2.Token{
		RefreshToken: s.refreshToken,
	}

	tokenSource := s.oauth2Config.TokenSource(ctx, token)
	newToken, err := tokenSource.Token()
	if err != nil {
		return errors.Wrap(err, errors.ErrorTypeAuthentication, "failed to refresh access token")
	}

	s.accessToken = newToken.AccessToken

	s.GetLogger().Debug("Access token refreshed successfully")
	return nil
}

// Discover implements schema discovery for Google Ads API
func (s *GoogleAdsSource) Discover(ctx context.Context) (*core.Schema, error) {
	if s.schema != nil {
		return s.schema, nil
	}

	if err := s.discoverSchema(ctx); err != nil {
		return nil, err
	}

	return s.schema, nil
}

// discoverSchema analyzes the GAQL query to determine schema
func (s *GoogleAdsSource) discoverSchema(ctx context.Context) error {
	// Parse GAQL query to extract fields
	fields := s.extractFieldsFromGAQL(s.query)

	// Create schema
	schema := &core.Schema{
		Name:        "google_ads_data",
		Description: "Google Ads API data based on GAQL query",
		Fields:      make([]core.Field, 0, len(fields)),
	}

	// Add extracted fields to schema
	for _, fieldName := range fields {
		field := core.Field{
			Name:        fieldName,
			Type:        s.inferFieldType(fieldName),
			Description: stringpool.Sprintf("Google Ads field: %s", fieldName),
			Nullable:    true,
		}
		schema.Fields = append(schema.Fields, field)
	}

	// Always add customer_id field
	schema.Fields = append(schema.Fields, core.Field{
		Name:        "customer_id",
		Type:        core.FieldTypeString,
		Description: "Google Ads Customer ID",
		Nullable:    false,
	})

	s.schema = schema
	return nil
}

// extractFieldsFromGAQL parses GAQL SELECT clause to extract field names
func (s *GoogleAdsSource) extractFieldsFromGAQL(query string) []string {
	// Simple GAQL parser to extract SELECT fields
	query = strings.TrimSpace(query)

	// Find SELECT clause
	selectIndex := strings.Index(strings.ToUpper(query), "SELECT")
	if selectIndex == -1 {
		return []string{}
	}

	// Find FROM clause
	fromIndex := strings.Index(strings.ToUpper(query), "FROM")
	if fromIndex == -1 {
		fromIndex = len(query)
	}

	// Extract field list
	fieldsPart := strings.TrimSpace(query[selectIndex+6 : fromIndex])
	fieldsList := strings.Split(fieldsPart, ",")

	fields := make([]string, 0, len(fieldsList))
	for _, field := range fieldsList {
		cleanField := strings.TrimSpace(field)
		if cleanField != "" {
			// Convert dot notation to underscore (e.g., campaign.name -> campaign_name)
			cleanField = strings.ReplaceAll(cleanField, ".", "_")
			fields = append(fields, cleanField)
		}
	}

	return fields
}

// inferFieldType infers the field type based on field name
func (s *GoogleAdsSource) inferFieldType(fieldName string) core.FieldType {
	fieldName = strings.ToLower(fieldName)

	// Metrics are typically numeric
	if strings.Contains(fieldName, "metrics") {
		if strings.Contains(fieldName, "cost") || strings.Contains(fieldName, "cpc") ||
			strings.Contains(fieldName, "cpm") || strings.Contains(fieldName, "ctr") ||
			strings.Contains(fieldName, "conversion") {
			return core.FieldTypeFloat
		}
		return core.FieldTypeInt
	}

	// IDs are typically strings (could be large numbers)
	if strings.Contains(fieldName, "id") {
		return core.FieldTypeString
	}

	// Dates and timestamps
	if strings.Contains(fieldName, "date") || strings.Contains(fieldName, "time") {
		return core.FieldTypeString
	}

	// Default to string for text fields
	return core.FieldTypeString
}

// Read implements streaming read with backpressure support
func (s *GoogleAdsSource) Read(ctx context.Context) (*core.RecordStream, error) {
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
func (s *GoogleAdsSource) ReadBatch(ctx context.Context, batchSize int) (*core.BatchStream, error) {
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

// readRecords reads records from Google Ads API with concurrent processing
func (s *GoogleAdsSource) readRecords(ctx context.Context, recordsChan chan<- *models.Record, errorsChan chan<- error) error {
	// Process each customer concurrently up to maxConcurrency
	semaphore := make(chan struct{}, s.maxConcurrency)
	var wg sync.WaitGroup

	for _, customerID := range s.customerIDs {
		wg.Add(1)
		go func(custID string) {
			defer wg.Done()

			// Acquire semaphore
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			if err := s.readCustomerRecords(ctx, custID, recordsChan, errorsChan); err != nil {
				s.GetLogger().Error("Failed to read customer records",
					zap.String("customer_id", custID),
					zap.Error(err))
				errorsChan <- err
			}
		}(customerID)
	}

	wg.Wait()
	return nil
}

// readBatches reads records in batches for better performance
func (s *GoogleAdsSource) readBatches(ctx context.Context, batchSize int, batchesChan chan<- []*models.Record, errorsChan chan<- error) error {
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

// readCustomerRecords reads all records for a specific customer
func (s *GoogleAdsSource) readCustomerRecords(ctx context.Context, customerID string, recordsChan chan<- *models.Record, errorsChan chan<- error) error {
	pageToken := ""

	for {
		// Apply rate limiting before each API call
		if err := s.RateLimit(ctx); err != nil {
			return err
		}

		// Execute API call with circuit breaker protection
		var response *GoogleAdsResponse
		if err := s.ExecuteWithCircuitBreaker(func() error {
			var err error
			response, err = s.makeAPIRequest(ctx, customerID, pageToken)
			return err
		}); err != nil {
			return errors.Wrap(err, errors.ErrorTypeConnection,
				stringpool.Sprintf("API request failed for customer %s", customerID))
		}

		// Process response records
		for _, result := range response.Results {
			record, err := s.convertToRecord(result, customerID)
			if err != nil {
				s.GetLogger().Error("Failed to convert result to record",
					zap.String("customer_id", customerID),
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
		if response.NextPageToken == "" {
			break
		}
		pageToken = response.NextPageToken

		// Update progress
		s.ReportProgress(s.processedRecords, s.totalRecords)
	}

	return nil
}

// makeAPIRequest makes a request to Google Ads API
func (s *GoogleAdsSource) makeAPIRequest(ctx context.Context, customerID, pageToken string) (*GoogleAdsResponse, error) {
	// Build request URL using URLBuilder for optimized string handling
	ub := stringpool.NewURLBuilder("https://googleads.googleapis.com")
	defer ub.Close()
	baseURL := ub.AddPath("v21", "customers", customerID, "googleAds:search").String()

	// Build request body
	requestBody := map[string]interface{}{
		"query": s.query,
	}

	if pageToken != "" {
		requestBody["pageToken"] = pageToken
	}

	// Use pooled buffer for HTTP request body construction
	requestBuffer := stringpool.GetBuilder(stringpool.Small)
	defer stringpool.PutBuilder(requestBuffer, stringpool.Small)

	// Serialize request body directly to pooled buffer
	if err := jsonpool.MarshalToWriter(requestBuffer, requestBody); err != nil {
		return nil, errors.Wrap(err, errors.ErrorTypeData, "failed to marshal request body")
	}

	// Create HTTP request using buffer directly (avoiding string conversion)
	req, err := http.NewRequestWithContext(ctx, "POST", baseURL, strings.NewReader(requestBuffer.String()))
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrorTypeConnection, "failed to create HTTP request")
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", stringpool.Sprintf("Bearer %s", s.accessToken))
	req.Header.Set("developer-token", s.config.DeveloperToken)

	if s.config.LoginCustomerID != "" {
		req.Header.Set("login-customer-id", s.config.LoginCustomerID)
	}

	// Make request
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrorTypeConnection, "HTTP request failed")
	}
	defer resp.Body.Close()

	// Parse response
	responseData := s.responsePool.Get().(*GoogleAdsResponse)
	defer s.responsePool.Put(responseData)

	// Use pooled buffer for HTTP response reading
	responseBuffer := stringpool.GetBuilder(stringpool.Medium)
	defer stringpool.PutBuilder(responseBuffer, stringpool.Medium)

	// Read response body into pooled buffer
	if _, err := io.Copy(responseBuffer, resp.Body); err != nil {
		return nil, errors.Wrap(err, errors.ErrorTypeData, "failed to read response body")
	}

	// Check for HTTP errors
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New(errors.ErrorTypeConnection,
			stringpool.Sprintf("API returned status %d: %s", resp.StatusCode, responseBuffer.String()))
	}

	// Unmarshal response from pooled buffer
	if err := jsonpool.Unmarshal(stringpool.StringToBytes(responseBuffer.String()), responseData); err != nil {
		return nil, errors.Wrap(err, errors.ErrorTypeData, "failed to decode API response")
	}

	// Return a copy since we're putting the original back in the pool
	responseCopy := &GoogleAdsResponse{
		Results:       make([]GoogleAdsResult, len(responseData.Results)),
		NextPageToken: responseData.NextPageToken,
		Summary:       responseData.Summary,
	}
	copy(responseCopy.Results, responseData.Results)

	return responseCopy, nil
}

// convertToRecord converts a Google Ads result to a models.Record
func (s *GoogleAdsSource) convertToRecord(result GoogleAdsResult, customerID string) (*models.Record, error) {
	// Get record from memory pool
	record := pool.NewRecordFromPool("google_ads")

	// Populate record data using SetData to preserve pooled map

	// Add customer ID
	record.SetData("customer_id", customerID)

	// Extract fields based on the result structure
	if result.Campaign != nil {
		if campaignMap, ok := result.Campaign.(map[string]interface{}); ok {
			for key, value := range campaignMap {
				record.SetData(stringpool.Sprintf("campaign_%s", key), value)
			}
		}
	}

	if result.AdGroup != nil {
		if adGroupMap, ok := result.AdGroup.(map[string]interface{}); ok {
			for key, value := range adGroupMap {
				record.SetData(stringpool.Sprintf("adgroup_%s", key), value)
			}
		}
	}

	if result.Keyword != nil {
		if keywordMap, ok := result.Keyword.(map[string]interface{}); ok {
			for key, value := range keywordMap {
				record.SetData(stringpool.Sprintf("keyword_%s", key), value)
			}
		}
	}

	if result.Metrics != nil {
		if metricsMap, ok := result.Metrics.(map[string]interface{}); ok {
			for key, value := range metricsMap {
				record.SetData(stringpool.Sprintf("metrics_%s", key), value)
			}
		}
	}

	if result.Segments != nil {
		if segmentsMap, ok := result.Segments.(map[string]interface{}); ok {
			for key, value := range segmentsMap {
				record.SetData(stringpool.Sprintf("segments_%s", key), value)
			}
		}
	}

	// Set record properties
	record.ID = stringpool.Sprintf("%s_%d", customerID, time.Now().UnixNano())
	record.Metadata.Source = "google_ads"
	// Data already populated via SetData calls
	record.SetMetadata("customer_id", customerID)
	record.SetMetadata("extracted_at", time.Now().Unix())
	record.SetTimestamp(time.Now())

	return record, nil
}

// Additional interface implementations following CLAUDE.md checklist...

// Close closes the Google Ads source connector
func (s *GoogleAdsSource) Close(ctx context.Context) error {
	s.GetLogger().Info("Closing Google Ads source connector")

	// Close base connector
	return s.BaseConnector.Close(ctx)
}

// GoogleAdsPosition implements core.Position for Google Ads
type GoogleAdsPosition struct {
	LastPageToken    string `json:"last_page_token"`
	ProcessedRecords int64  `json:"processed_records"`
}

func (p *GoogleAdsPosition) String() string {
	return stringpool.Sprintf("token:%s,records:%d", p.LastPageToken, p.ProcessedRecords)
}

func (p *GoogleAdsPosition) Compare(other core.Position) int {
	if otherPos, ok := other.(*GoogleAdsPosition); ok {
		if p.ProcessedRecords < otherPos.ProcessedRecords {
			return -1
		} else if p.ProcessedRecords > otherPos.ProcessedRecords {
			return 1
		}
		return 0
	}
	return 0
}

// GetPosition returns the current position (page token)
func (s *GoogleAdsSource) GetPosition() core.Position {
	return &GoogleAdsPosition{
		LastPageToken:    s.lastPageToken,
		ProcessedRecords: s.processedRecords,
	}
}

// SetPosition sets the position (page token)
func (s *GoogleAdsSource) SetPosition(position core.Position) error {
	if googleAdsPos, ok := position.(*GoogleAdsPosition); ok {
		s.lastPageToken = googleAdsPos.LastPageToken
		s.processedRecords = googleAdsPos.ProcessedRecords
	}
	return nil
}

// GetState returns the current state
func (s *GoogleAdsSource) GetState() core.State {
	return core.State{
		"customer_ids":      s.customerIDs,
		"query":             s.query,
		"last_page_token":   s.lastPageToken,
		"processed_records": s.processedRecords,
		"is_complete":       s.isComplete,
	}
}

// SetState sets the state
func (s *GoogleAdsSource) SetState(state core.State) error {
	if customerIDs, ok := state["customer_ids"].([]string); ok {
		s.customerIDs = customerIDs
	}
	if query, ok := state["query"].(string); ok {
		s.query = query
	}
	if token, ok := state["last_page_token"].(string); ok {
		s.lastPageToken = token
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
func (s *GoogleAdsSource) SupportsIncremental() bool {
	return true // Google Ads supports date-based incremental sync
}

// SupportsRealtime returns true if real-time sync is supported
func (s *GoogleAdsSource) SupportsRealtime() bool {
	return false // Google Ads doesn't support real-time streaming
}

// SupportsBatch returns true if batch operations are supported
func (s *GoogleAdsSource) SupportsBatch() bool {
	return true
}

// Subscribe implements CDC/real-time subscription (not supported for Google Ads)
func (s *GoogleAdsSource) Subscribe(ctx context.Context, tables []string) (*core.ChangeStream, error) {
	return nil, errors.New(errors.ErrorTypeCapability, "Google Ads does not support real-time subscriptions")
}

// Health performs health check
func (s *GoogleAdsSource) Health(ctx context.Context) error {
	// Check if access token is valid by making a simple API call
	if s.accessToken == "" {
		return errors.New(errors.ErrorTypeAuthentication, "no access token available")
	}

	// Try to refresh token to verify connectivity
	return s.refreshAccessToken(ctx)
}

// Metrics returns connector metrics
func (s *GoogleAdsSource) Metrics() map[string]interface{} {
	return map[string]interface{}{
		"type":              "google_ads",
		"customers_count":   len(s.customerIDs),
		"processed_records": s.processedRecords,
		"streaming_enabled": s.streamingEnabled,
		"page_size":         s.pageSize,
		"max_concurrency":   s.maxConcurrency,
		"is_complete":       s.isComplete,
	}
}
