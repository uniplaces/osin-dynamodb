package osindynamodb

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/RangelReale/osin"
	"github.com/stretchr/testify/assert"
)

func TestAccessAuthorizationCode(t *testing.T) {
	t.Parallel()
	storageConfig := CreateStorageConfig("TestAccessAuthorizationCode")
	svc := createDynamoDB()
	storage := New(svc, storageConfig)
	err := storage.CreateSchema()
	defer storage.DropSchema()
	assert.Nil(t, err, "%s", err)
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
	sconfig.AllowedAccessTypes = osin.AllowedAccessType{osin.AUTHORIZATION_CODE}
	server := osin.NewServer(sconfig, storage)
	server.AccessTokenGen = &TestingAccessTokenGen{}
	resp := server.NewResponse()

	req, err := http.NewRequest("POST", "http://localhost:14000/appauth", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth("1234", "aabbccdd")

	req.Form = make(url.Values)
	req.Form.Set("grant_type", string(osin.AUTHORIZATION_CODE))
	req.Form.Set("code", "9999")
	req.Form.Set("state", "a")
	req.PostForm = make(url.Values)

	if ar := server.HandleAccessRequest(resp, req); ar != nil {
		ar.Authorized = true
		server.FinishAccessRequest(resp, req, ar)
	}

	//fmt.Printf("%+v", resp)

	if resp.IsError && resp.InternalError != nil {
		t.Fatalf("Error in response: %s", resp.InternalError)
	}

	if resp.IsError {
		t.Fatalf("Should not be an error")
	}

	if resp.Type != osin.DATA {
		t.Fatalf("Response should be data")
	}

	if d := resp.Output["access_token"]; d != "1" {
		t.Fatalf("Unexpected access token: %s", d)
	}

	if d := resp.Output["refresh_token"]; d != "r1" {
		t.Fatalf("Unexpected refresh token: %s", d)
	}
}

func TestAccessRefreshToken(t *testing.T) {
	t.Parallel()
	storageConfig := CreateStorageConfig("TestAccessRefreshToken")
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
	accessData := &osin.AccessData{
		Client:       client,
		AccessToken:  "1",
		RefreshToken: "r9999",
		ExpiresIn:    3600,
		RedirectUri:  "/dev/null",
		CreatedAt:    time.Now(),
	}
	err = storage.SaveAccess(accessData)
	assert.Nil(t, err, "%s", err)

	// -- -- --
	sconfig := osin.NewServerConfig()
	sconfig.AllowedAccessTypes = osin.AllowedAccessType{osin.REFRESH_TOKEN}
	server := osin.NewServer(sconfig, storage)
	server.AccessTokenGen = &TestingAccessTokenGen{}
	resp := server.NewResponse()

	req, err := http.NewRequest("POST", "http://localhost:14000/appauth", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth("1234", "aabbccdd")

	req.Form = make(url.Values)
	req.Form.Set("grant_type", string(osin.REFRESH_TOKEN))
	req.Form.Set("refresh_token", "r9999")
	req.Form.Set("state", "a")
	req.PostForm = make(url.Values)

	if ar := server.HandleAccessRequest(resp, req); ar != nil {
		ar.Authorized = true
		server.FinishAccessRequest(resp, req, ar)
	}

	if resp.IsError && resp.InternalError != nil {
		t.Fatalf("Error in response: %s", resp.InternalError)
	}

	if resp.IsError {
		t.Fatalf("Should not be an error")
	}

	if resp.Type != osin.DATA {
		t.Fatalf("Response should be data")
	}

	if d := resp.Output["access_token"]; d != "1" {
		t.Fatalf("Unexpected access token: %s", d)
	}

	if d := resp.Output["refresh_token"]; d != "r1" {
		t.Fatalf("Unexpected refresh token: %s", d)
	}
}

func TestAccessPassword(t *testing.T) {
	t.Parallel()
	storageConfig := CreateStorageConfig("TestAccessPassword")
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
	// -- -- --
	sconfig := osin.NewServerConfig()
	sconfig.AllowedAccessTypes = osin.AllowedAccessType{osin.PASSWORD}
	server := osin.NewServer(sconfig, storage)
	server.AccessTokenGen = &TestingAccessTokenGen{}
	resp := server.NewResponse()

	req, err := http.NewRequest("POST", "http://localhost:14000/appauth", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth("1234", "aabbccdd")

	req.Form = make(url.Values)
	req.Form.Set("grant_type", string(osin.PASSWORD))
	req.Form.Set("username", "testing")
	req.Form.Set("password", "testing")
	req.Form.Set("state", "a")
	req.PostForm = make(url.Values)

	if ar := server.HandleAccessRequest(resp, req); ar != nil {
		ar.Authorized = ar.Username == "testing" && ar.Password == "testing"
		server.FinishAccessRequest(resp, req, ar)
	}

	if resp.IsError && resp.InternalError != nil {
		t.Fatalf("Error in response: %s", resp.InternalError)
	}

	if resp.IsError {
		t.Fatalf("Should not be an error")
	}

	if resp.Type != osin.DATA {
		t.Fatalf("Response should be data")
	}

	if d := resp.Output["access_token"]; d != "1" {
		t.Fatalf("Unexpected access token: %s", d)
	}

	if d := resp.Output["refresh_token"]; d != "r1" {
		t.Fatalf("Unexpected refresh token: %s", d)
	}
}

func TestAccessClientCredentials(t *testing.T) {
	t.Parallel()
	storageConfig := CreateStorageConfig("TestAccessClientCredentials")
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
	// -- -- --
	sconfig := osin.NewServerConfig()
	sconfig.AllowedAccessTypes = osin.AllowedAccessType{osin.CLIENT_CREDENTIALS}
	server := osin.NewServer(sconfig, storage)
	server.AccessTokenGen = &TestingAccessTokenGen{}
	resp := server.NewResponse()

	req, err := http.NewRequest("POST", "http://localhost:14000/appauth", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth("1234", "aabbccdd")

	req.Form = make(url.Values)
	req.Form.Set("grant_type", string(osin.CLIENT_CREDENTIALS))
	req.Form.Set("state", "a")
	req.PostForm = make(url.Values)

	if ar := server.HandleAccessRequest(resp, req); ar != nil {
		ar.Authorized = true
		server.FinishAccessRequest(resp, req, ar)
	}

	if resp.IsError && resp.InternalError != nil {
		t.Fatalf("Error in response: %s", resp.InternalError)
	}

	if resp.IsError {
		t.Fatalf("Should not be an error")
	}

	if resp.Type != osin.DATA {
		t.Fatalf("Response should be data")
	}

	if d := resp.Output["access_token"]; d != "1" {
		t.Fatalf("Unexpected access token: %s", d)
	}

	if d, dok := resp.Output["refresh_token"]; dok {
		t.Fatalf("Refresh token should not be generated: %s", d)
	}
}
