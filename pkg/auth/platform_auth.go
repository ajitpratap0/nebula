package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/ajitpratap0/nebula/pkg/errors"
)

// PlatformAuthService handles dynamic token fetching for various platforms
type PlatformAuthService struct {
	baseURL string
	apiKey  string
	client  *http.Client
}

// TokenData represents the token information from the TMS service
type TokenData struct {
	DeveloperToken  string `json:"developerToken"`
	LoginCustomerID string `json:"loginCustomerId"`
	SetToExpireOn   string `json:"setToExpireOn"`
	Token           string `json:"token"`
	ClientSecret    string `json:"clientSecret"`
	ClientID        string `json:"clientId"`
	RefreshToken    string `json:"refreshToken"`
	IsExpired       bool   `json:"isExpired"`
}

// PlatformToken represents the complete token response
type PlatformToken struct {
	TokenData     TokenData `json:"tokenData"`
	TokenUID      string    `json:"tokenUID"`
	SetToExpireOn string    `json:"setToExpireOn"`
	IsMasterToken bool      `json:"isMasterToken"`
	IsPrimary     bool      `json:"isPrimary"`
	IsExpired     bool      `json:"isExpired"`
}

// TokenResponse represents the API response structure
type TokenResponse struct {
	Success bool            `json:"success"`
	Data    []PlatformToken `json:"data"`
}

// GoogleAdsCredentials represents the credentials needed for Google Ads API
type GoogleAdsCredentials struct {
	AccessToken     string
	RefreshToken    string
	ClientID        string
	ClientSecret    string
	DeveloperToken  string
	LoginCustomerID string
	ExpiresAt       time.Time
}

// NewPlatformAuthService creates a new platform authentication service
func NewPlatformAuthService(baseURL, apiKey string) *PlatformAuthService {
	return &PlatformAuthService{
		baseURL: baseURL,
		apiKey:  apiKey,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// GetGoogleAdsCredentials fetches Google Ads credentials for the given account ID
func (p *PlatformAuthService) GetGoogleAdsCredentials(ctx context.Context, accountID string) (*GoogleAdsCredentials, error) {
	url := fmt.Sprintf("%s/v1/api/adAccount/%s/tokens?platform=GOOGLE", p.baseURL, accountID)

	req, err := http.NewRequestWithContext(ctx, "GET", url, http.NoBody)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrorTypeConnection, "failed to create request")
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("x-api-key", p.apiKey)

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrorTypeConnection, "failed to make request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.New(errors.ErrorTypeConnection,
			fmt.Sprintf("API returned status %d", resp.StatusCode))
	}

	var tokenResp TokenResponse
	if decodeErr := json.NewDecoder(resp.Body).Decode(&tokenResp); decodeErr != nil {
		return nil, errors.Wrap(decodeErr, errors.ErrorTypeData, "failed to decode response")
	}

	if !tokenResp.Success || len(tokenResp.Data) == 0 {
		return nil, errors.New(errors.ErrorTypeData, "no tokens found for account")
	}

	// Get the first (master) token
	token := tokenResp.Data[0]

	// Parse expiration time
	expiresAt, err := time.Parse(time.RFC3339, token.TokenData.SetToExpireOn)
	if err != nil {
		return nil, errors.Wrap(err, errors.ErrorTypeData, "failed to parse expiration time")
	}

	return &GoogleAdsCredentials{
		AccessToken:     token.TokenData.Token,
		RefreshToken:    token.TokenData.RefreshToken,
		ClientID:        token.TokenData.ClientID,
		ClientSecret:    token.TokenData.ClientSecret,
		DeveloperToken:  token.TokenData.DeveloperToken,
		LoginCustomerID: token.TokenData.LoginCustomerID,
		ExpiresAt:       expiresAt,
	}, nil
}

// GetPlatformCredentials is a generic method that can be extended for other platforms
func (p *PlatformAuthService) GetPlatformCredentials(ctx context.Context, accountID, platform string) (interface{}, error) {
	switch platform {
	case "GOOGLE":
		return p.GetGoogleAdsCredentials(ctx, accountID)
	default:
		return nil, errors.New(errors.ErrorTypeConfig,
			fmt.Sprintf("unsupported platform: %s", platform))
	}
}

// IsTokenExpired checks if the token is expired or will expire soon (within 5 minutes)
func (creds *GoogleAdsCredentials) IsTokenExpired() bool {
	return time.Now().Add(5 * time.Minute).After(creds.ExpiresAt)
}
