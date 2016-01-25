package osindynamodb

import (
	"github.com/RangelReale/osin"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/url"
	"testing"
	"time"
)

func TestAuthorizeCode(t *testing.T) {
	t.Parallel()
	storageConfig := CreateStorageConfig("TestAuthorizeCode")
	svc := createDynamoDB()
	storage := New(svc, storageConfig)
	err := storage.CreateSchema()
	assert.Nil(t, err, "%s", err)
	defer storage.DropSchema()
	client := &osin.DefaultClient{
		Id:          "1234",
		Secret:      "aabbccdd",
		RedirectUri: "/dev/null",
	}
	err = storage.CreateClient(client)
	assert.Nil(t, err, "%s", err)
	authorizeData := &osin.AuthorizeData{
		Client:      client,
		Code:        "9999",
		ExpiresIn:   3600,
		RedirectUri: "/dev/null",
		CreatedAt:   time.Now(),
	}
	err = storage.SaveAuthorize(authorizeData)
	assert.Nil(t, err, "%s", err)

	// -- -- --
	sconfig := osin.NewServerConfig()
	sconfig.AllowedAuthorizeTypes = osin.AllowedAuthorizeType{osin.CODE}
	server := osin.NewServer(sconfig, storage)
	server.AuthorizeTokenGen = &TestingAuthorizeTokenGen{}
	resp := server.NewResponse()

	req, err := http.NewRequest("GET", "http://localhost:14000/appauth", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Form = make(url.Values)
	req.Form.Set("response_type", string(osin.CODE))
	req.Form.Set("client_id", "1234")
	req.Form.Set("state", "a")

	if ar := server.HandleAuthorizeRequest(resp, req); ar != nil {
		ar.Authorized = true
		server.FinishAuthorizeRequest(resp, req, ar)
	}

	if resp.IsError && resp.InternalError != nil {
		t.Fatalf("Error in response: %s", resp.InternalError)
	}

	if resp.IsError {
		t.Fatalf("Should not be an error")
	}

	if resp.Type != osin.REDIRECT {
		t.Fatalf("Response should be a redirect")
	}

	if d := resp.Output["code"]; d != "1" {
		t.Fatalf("Unexpected authorization code: %s", d)
	}
}

func TestAuthorizeToken(t *testing.T) {
	t.Parallel()
	storageConfig := CreateStorageConfig("TestAuthorizeToken")
	svc := createDynamoDB()
	storage := New(svc, storageConfig)
	err := storage.CreateSchema()
	assert.Nil(t, err, "%s", err)
	defer storage.DropSchema()
	client := &osin.DefaultClient{
		Id:          "1234",
		Secret:      "aabbccdd",
		RedirectUri: "/dev/null",
	}
	err = storage.CreateClient(client)
	assert.Nil(t, err, "%s", err)
	authorizeData := &osin.AuthorizeData{
		Client:      client,
		Code:        "9999",
		ExpiresIn:   3600,
		RedirectUri: "/dev/null",
		CreatedAt:   time.Now(),
	}
	err = storage.SaveAuthorize(authorizeData)
	assert.Nil(t, err, "%s", err)

	// -- -- --
	sconfig := osin.NewServerConfig()
	sconfig.AllowedAuthorizeTypes = osin.AllowedAuthorizeType{osin.TOKEN}
	server := osin.NewServer(sconfig, storage)
	server.AuthorizeTokenGen = &TestingAuthorizeTokenGen{}
	server.AccessTokenGen = &TestingAccessTokenGen{}
	resp := server.NewResponse()

	req, err := http.NewRequest("GET", "http://localhost:14000/appauth", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Form = make(url.Values)
	req.Form.Set("response_type", string(osin.TOKEN))
	req.Form.Set("client_id", "1234")
	req.Form.Set("state", "a")

	if ar := server.HandleAuthorizeRequest(resp, req); ar != nil {
		ar.Authorized = true
		server.FinishAuthorizeRequest(resp, req, ar)
	}

	if resp.IsError && resp.InternalError != nil {
		t.Fatalf("Error in response: %s", resp.InternalError)
	}

	if resp.IsError {
		t.Fatalf("Should not be an error")
	}

	if resp.Type != osin.REDIRECT || !resp.RedirectInFragment {
		t.Fatalf("Response should be a redirect with fragment")
	}

	if d := resp.Output["access_token"]; d != "1" {
		t.Fatalf("Unexpected access token: %s", d)
	}
}
