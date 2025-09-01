// Package clients provides OAuth2 authentication support
package clients

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/ajitpratap0/nebula/pkg/errors"
	jsonpool "github.com/ajitpratap0/nebula/pkg/json"
	stringpool "github.com/ajitpratap0/nebula/pkg/strings"
	"go.uber.org/zap"
)

// OAuth2Client provides OAuth2 authentication functionality with automatic token
// management, refresh handling, and comprehensive error recovery.
type OAuth2Client struct {
	config       *OAuth2Config
	logger       *zap.Logger
	httpClient   *HTTPClient
	tokenManager *TokenManager

	// Stats
	tokenRequests  int64
	tokenRefreshes int64
	authFailures   int64

	mu sync.RWMutex //nolint:unused // Reserved for thread-safe operations
}

// OAuth2Config configures OAuth2 authentication parameters including endpoints,
// credentials, and token management settings.
type OAuth2Config struct {
	// Client credentials
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	RedirectURL  string `json:"redirect_url,omitempty"`

	// Token endpoints
	AuthURL    string `json:"auth_url"`
	TokenURL   string `json:"token_url"`
	RefreshURL string `json:"refresh_url,omitempty"`

	// Scopes
	Scopes []string `json:"scopes"`

	// Grant type
	GrantType string `json:"grant_type"` // authorization_code, client_credentials, refresh_token

	// Token settings
	AutoRefresh      bool          `json:"auto_refresh"`
	RefreshThreshold time.Duration `json:"refresh_threshold"`
	MaxRetries       int           `json:"max_retries"`
	RetryDelay       time.Duration `json:"retry_delay"`

	// Advanced settings
	UseBasicAuth  bool              `json:"use_basic_auth"`
	CustomHeaders map[string]string `json:"custom_headers,omitempty"`
	CustomParams  map[string]string `json:"custom_params,omitempty"`
}

// Token represents an OAuth2 access token with metadata about expiration,
// refresh capabilities, and associated scopes.
type Token struct {
	AccessToken  string    `json:"access_token"`
	TokenType    string    `json:"token_type"`
	RefreshToken string    `json:"refresh_token,omitempty"`
	ExpiresAt    time.Time `json:"expires_at"`
	Scopes       []string  `json:"scopes,omitempty"`

	// Additional fields
	IDToken string                 `json:"id_token,omitempty"`
	Extra   map[string]interface{} `json:"extra,omitempty"`
}

// TokenManager manages OAuth2 tokens with automatic refresh, thread-safe access,
// and coordinated refresh to prevent token request storms.
type TokenManager struct {
	config     *OAuth2Config
	logger     *zap.Logger
	httpClient *HTTPClient

	// Current token
	currentToken *Token

	// Refresh coordination
	refreshing  bool
	refreshCond *sync.Cond
	lastRefresh time.Time

	// Callbacks
	onTokenRefresh func(*Token)

	mu sync.RWMutex
}

// NewOAuth2Client creates a new OAuth2 client with the given configuration.
// It initializes token management with automatic refresh capabilities.
func NewOAuth2Client(config *OAuth2Config, httpClient *HTTPClient, logger *zap.Logger) *OAuth2Client {
	if config.RefreshThreshold == 0 {
		config.RefreshThreshold = 5 * time.Minute
	}
	if config.RetryDelay == 0 {
		config.RetryDelay = time.Second
	}
	if config.MaxRetries == 0 {
		config.MaxRetries = 3
	}

	client := &OAuth2Client{
		config:     config,
		logger:     logger.With(zap.String("component", "oauth2_client")),
		httpClient: httpClient,
	}

	client.tokenManager = &TokenManager{
		config:      config,
		logger:      client.logger,
		httpClient:  httpClient,
		refreshCond: sync.NewCond(&sync.Mutex{}),
	}

	return client
}

// GetAuthorizationURL returns the authorization URL for the OAuth2 flow
func (oc *OAuth2Client) GetAuthorizationURL(state string) string {
	// Build authorization URL using URLBuilder for optimized string handling
	ub := stringpool.NewURLBuilder(oc.config.AuthURL)
	defer ub.Close() // Ignore close error

	// Add required OAuth2 parameters
	ub.AddParam("client_id", oc.config.ClientID)
	ub.AddParam("response_type", "code")
	ub.AddParam("state", state)

	if oc.config.RedirectURL != "" {
		ub.AddParam("redirect_uri", oc.config.RedirectURL)
	}

	if len(oc.config.Scopes) > 0 {
		ub.AddParam("scope", stringpool.JoinPooled(oc.config.Scopes, " "))
	}

	// Add custom params
	for key, value := range oc.config.CustomParams {
		ub.AddParam(key, value)
	}

	return ub.String()
}

// ExchangeCode exchanges an authorization code for an access token
func (oc *OAuth2Client) ExchangeCode(ctx context.Context, code string) (*Token, error) {
	params := url.Values{
		"grant_type": {"authorization_code"},
		"code":       {code},
	}

	if !oc.config.UseBasicAuth {
		params.Set("client_id", oc.config.ClientID)
		params.Set("client_secret", oc.config.ClientSecret)
	}

	if oc.config.RedirectURL != "" {
		params.Set("redirect_uri", oc.config.RedirectURL)
	}

	token, err := oc.requestToken(ctx, params)
	if err != nil {
		return nil, err
	}

	// Store token in manager
	oc.tokenManager.SetToken(token)

	return token, nil
}

// GetClientCredentialsToken gets a token using client credentials grant
func (oc *OAuth2Client) GetClientCredentialsToken(ctx context.Context) (*Token, error) {
	params := url.Values{
		"grant_type": {"client_credentials"},
	}

	if !oc.config.UseBasicAuth {
		params.Set("client_id", oc.config.ClientID)
		params.Set("client_secret", oc.config.ClientSecret)
	}

	if len(oc.config.Scopes) > 0 {
		params.Set("scope", stringpool.JoinPooled(oc.config.Scopes, " "))
	}

	token, err := oc.requestToken(ctx, params)
	if err != nil {
		return nil, err
	}

	// Store token in manager
	oc.tokenManager.SetToken(token)

	return token, nil
}

// RefreshToken refreshes an access token using a refresh token
func (oc *OAuth2Client) RefreshToken(ctx context.Context, refreshToken string) (*Token, error) {
	refreshURL := oc.config.RefreshURL
	if refreshURL == "" {
		refreshURL = oc.config.TokenURL
	}

	params := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {refreshToken},
	}

	if !oc.config.UseBasicAuth {
		params.Set("client_id", oc.config.ClientID)
		params.Set("client_secret", oc.config.ClientSecret)
	}

	token, err := oc.requestTokenWithURL(ctx, refreshURL, params)
	if err != nil {
		return nil, err
	}

	// Preserve refresh token if not returned
	if token.RefreshToken == "" && refreshToken != "" {
		token.RefreshToken = refreshToken
	}

	// Store token in manager
	oc.tokenManager.SetToken(token)

	return token, nil
}

// GetToken returns a valid access token, refreshing if necessary
func (oc *OAuth2Client) GetToken(ctx context.Context) (*Token, error) {
	return oc.tokenManager.GetToken(ctx)
}

// SetToken manually sets a token
func (oc *OAuth2Client) SetToken(token *Token) {
	oc.tokenManager.SetToken(token)
}

// OnTokenRefresh sets a callback for token refresh events
func (oc *OAuth2Client) OnTokenRefresh(callback func(*Token)) {
	oc.tokenManager.onTokenRefresh = callback
}

// requestToken makes a token request
func (oc *OAuth2Client) requestToken(ctx context.Context, params url.Values) (*Token, error) {
	return oc.requestTokenWithURL(ctx, oc.config.TokenURL, params)
}

// requestTokenWithURL makes a token request to a specific URL
func (oc *OAuth2Client) requestTokenWithURL(ctx context.Context, tokenURL string, params url.Values) (*Token, error) {
	// Add custom params
	for key, value := range oc.config.CustomParams {
		params.Set(key, value)
	}

	// Use pooled buffer for request body construction
	requestBuffer := stringpool.GetBuilder(stringpool.Small)
	defer stringpool.PutBuilder(requestBuffer, stringpool.Small)
	requestBuffer.WriteString(params.Encode())
	body := strings.NewReader(requestBuffer.String())

	headers := map[string]string{
		"Content-Type": "application/x-www-form-urlencoded",
	}

	// Add basic auth if configured
	if oc.config.UseBasicAuth {
		auth := stringpool.Concat(oc.config.ClientID, ":", oc.config.ClientSecret)
		headers["Authorization"] = stringpool.Concat("Basic ", encodeBase64([]byte(auth)))
	}

	// Add custom headers
	for key, value := range oc.config.CustomHeaders {
		headers[key] = value
	}

	// Make request with retries
	var resp *http.Response
	var err error

	for attempt := 0; attempt <= oc.config.MaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(oc.config.RetryDelay * time.Duration(attempt)):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		resp, err = oc.httpClient.Post(ctx, tokenURL, body, headers)
		if err == nil && resp.StatusCode < 500 {
			break
		}

		if resp != nil {
			if err := resp.Body.Close(); err != nil {
				oc.logger.Debug("failed to close response body", zap.Error(err))
			}
		}

		oc.logger.Warn("token request failed, retrying",
			zap.Error(err),
			zap.Int("attempt", attempt+1),
			zap.Int("max_retries", oc.config.MaxRetries))
	}

	if err != nil {
		return nil, errors.Wrap(err, errors.ErrorTypeConnection, "token request failed")
	}
	defer func() { _ = resp.Body.Close() }() // Ignore close error - Response body close errors in defer are usually not actionable

	// Parse response
	if resp.StatusCode != http.StatusOK {
		var errResp OAuth2Error
		decoder := jsonpool.GetDecoder(resp.Body)
		defer jsonpool.PutDecoder(decoder)
		if err := decoder.Decode(&errResp); err == nil {
			return nil, &errResp
		}
		return nil, errors.New(errors.ErrorTypeConnection, stringpool.Sprintf("token request failed with status %d", resp.StatusCode))
	}

	var tokenResp tokenResponse
	decoder := jsonpool.GetDecoder(resp.Body)
	defer jsonpool.PutDecoder(decoder)
	if err := decoder.Decode(&tokenResp); err != nil {
		return nil, errors.Wrap(err, errors.ErrorTypeData, "failed to decode token response")
	}

	// Convert to Token
	token := &Token{
		AccessToken:  tokenResp.AccessToken,
		TokenType:    tokenResp.TokenType,
		RefreshToken: tokenResp.RefreshToken,
		IDToken:      tokenResp.IDToken,
		Extra:        tokenResp.Extra,
	}

	// Calculate expiry
	if tokenResp.ExpiresIn > 0 {
		token.ExpiresAt = time.Now().Add(time.Duration(tokenResp.ExpiresIn) * time.Second)
	} else {
		// Default to 1 hour if not specified
		token.ExpiresAt = time.Now().Add(time.Hour)
	}

	// Parse scopes
	if tokenResp.Scope != "" {
		token.Scopes = strings.Split(tokenResp.Scope, " ")
	}

	oc.logger.Info("token acquired",
		zap.Time("expires_at", token.ExpiresAt),
		zap.Strings("scopes", token.Scopes))

	return token, nil
}

// GetStats returns OAuth2 client statistics
func (oc *OAuth2Client) GetStats() OAuth2Stats {
	return OAuth2Stats{
		TokenRequests:     oc.tokenRequests,
		TokenRefreshes:    oc.tokenRefreshes,
		AuthFailures:      oc.authFailures,
		CurrentTokenValid: oc.tokenManager.IsTokenValid(),
	}
}

// TokenManager implementation

// GetToken returns a valid token, refreshing if necessary
func (tm *TokenManager) GetToken(ctx context.Context) (*Token, error) {
	tm.mu.RLock()
	token := tm.currentToken
	tm.mu.RUnlock()

	if token == nil {
		return nil, errors.New(errors.ErrorTypeAuthentication, "no token available")
	}

	// Check if token needs refresh
	if tm.config.AutoRefresh && tm.shouldRefresh(token) {
		return tm.refreshToken(ctx, token)
	}

	return token, nil
}

// SetToken sets the current token
func (tm *TokenManager) SetToken(token *Token) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tm.currentToken = token
	tm.lastRefresh = time.Now()

	if tm.onTokenRefresh != nil {
		go tm.onTokenRefresh(token)
	}
}

// IsTokenValid checks if the current token is valid
func (tm *TokenManager) IsTokenValid() bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	if tm.currentToken == nil {
		return false
	}

	return time.Now().Before(tm.currentToken.ExpiresAt)
}

// shouldRefresh checks if token should be refreshed
func (tm *TokenManager) shouldRefresh(token *Token) bool {
	if token == nil {
		return false
	}

	// Don't refresh if no refresh token
	if token.RefreshToken == "" && tm.config.GrantType != "client_credentials" {
		return false
	}

	// Check if token is about to expire
	timeUntilExpiry := time.Until(token.ExpiresAt)
	return timeUntilExpiry < tm.config.RefreshThreshold
}

// refreshToken refreshes the token
func (tm *TokenManager) refreshToken(ctx context.Context, oldToken *Token) (*Token, error) {
	// Coordinate refresh to avoid thundering herd
	tm.refreshCond.L.Lock()

	// Check if another goroutine is already refreshing
	if tm.refreshing {
		tm.refreshCond.Wait()
		tm.refreshCond.L.Unlock()

		// After waking up, check if we have a valid token
		tm.mu.RLock()
		token := tm.currentToken
		tm.mu.RUnlock()

		if token != nil && !tm.shouldRefresh(token) {
			return token, nil
		}

		// Token still needs refresh, try again
		return tm.refreshToken(ctx, oldToken)
	}

	// Mark as refreshing
	tm.refreshing = true
	tm.refreshCond.L.Unlock()

	defer func() {
		tm.refreshCond.L.Lock()
		tm.refreshing = false
		tm.refreshCond.Broadcast()
		tm.refreshCond.L.Unlock()
	}()

	// Perform refresh based on grant type
	var newToken *Token
	var err error

	switch tm.config.GrantType {
	case "refresh_token":
		if oldToken.RefreshToken == "" {
			return nil, errors.New(errors.ErrorTypeAuthentication, "no refresh token available")
		}

		// Create a temporary OAuth2Client to avoid circular dependency
		client := &OAuth2Client{
			config:     tm.config,
			logger:     tm.logger,
			httpClient: tm.httpClient,
		}

		newToken, err = client.RefreshToken(ctx, oldToken.RefreshToken)

	case "client_credentials":
		// Re-request token using client credentials
		client := &OAuth2Client{
			config:     tm.config,
			logger:     tm.logger,
			httpClient: tm.httpClient,
		}

		newToken, err = client.GetClientCredentialsToken(ctx)

	default:
		return nil, errors.New(errors.ErrorTypeCapability, stringpool.Sprintf("refresh not supported for grant type: %s", tm.config.GrantType))
	}

	if err != nil {
		tm.logger.Error("token refresh failed", zap.Error(err))
		return nil, err
	}

	// Update token
	tm.SetToken(newToken)

	tm.logger.Info("token refreshed successfully",
		zap.Time("expires_at", newToken.ExpiresAt))

	return newToken, nil
}

// OAuth2Error represents an OAuth2 error response
type OAuth2Error struct {
	ErrorCode        string `json:"error"`
	ErrorDescription string `json:"error_description,omitempty"`
	ErrorURI         string `json:"error_uri,omitempty"`
}

func (e *OAuth2Error) Error() string {
	if e.ErrorDescription != "" {
		return stringpool.Sprintf("%s: %s", e.ErrorCode, e.ErrorDescription)
	}
	return e.ErrorCode
}

// tokenResponse represents a token endpoint response
type tokenResponse struct {
	AccessToken  string                 `json:"access_token"`
	TokenType    string                 `json:"token_type"`
	RefreshToken string                 `json:"refresh_token,omitempty"`
	ExpiresIn    int                    `json:"expires_in,omitempty"`
	Scope        string                 `json:"scope,omitempty"`
	IDToken      string                 `json:"id_token,omitempty"`
	Extra        map[string]interface{} `json:"-"`
}

// OAuth2Stats represents OAuth2 client statistics
type OAuth2Stats struct {
	TokenRequests     int64 `json:"token_requests"`
	TokenRefreshes    int64 `json:"token_refreshes"`
	AuthFailures      int64 `json:"auth_failures"`
	CurrentTokenValid bool  `json:"current_token_valid"`
}

// encodeBase64 encodes data to base64 string
func encodeBase64(data []byte) string {
	// Simple base64 encoding using pooled builder
	const base64Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	result := stringpool.GetBuilder(stringpool.Small)
	defer stringpool.PutBuilder(result, stringpool.Small)
	for i := 0; i < len(data); i += 3 {
		b1 := data[i]
		b2 := byte(0)
		b3 := byte(0)
		if i+1 < len(data) {
			b2 = data[i+1]
		}
		if i+2 < len(data) {
			b3 = data[i+2]
		}

		result.WriteByte(base64Chars[b1>>2])
		result.WriteByte(base64Chars[(b1&0x03)<<4|b2>>4])
		if i+1 < len(data) {
			result.WriteByte(base64Chars[(b2&0x0f)<<2|b3>>6])
		} else {
			result.WriteByte('=')
		}
		if i+2 < len(data) {
			result.WriteByte(base64Chars[b3&0x3f])
		} else {
			result.WriteByte('=')
		}
	}
	return stringpool.Clone(result.String())
}
