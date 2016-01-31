package osindynamodb

import (
	"encoding/json"
	"github.com/RangelReale/osin"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSchema(t *testing.T) {
	t.Parallel()
	storageConfig := CreateStorageConfig("Schema")
	var err error
	svc := createDynamoDB()
	storage := New(svc, storageConfig)
	err = storage.CreateSchema()
	assert.Nil(t, err, "%s", err)
	err = storage.DropSchema()
	assert.Nil(t, err, "%s", err)
}

func TestClient(t *testing.T) {
	t.Parallel()
	storageConfig := CreateStorageConfig("Client")
	var err error
	svc := createDynamoDB()
	storage := New(svc, storageConfig)
	err = storage.CreateSchema()
	assert.Nil(t, err, "%s", err)
	defer storage.DropSchema()
	client := &osin.DefaultClient{
		Id:     "1234",
		Secret: "aabbccdd",
	}

	got, err := storage.GetClient(client.Id)
	assert.Equal(t, ErrClientNotFound, err)
	assert.Nil(t, got)

	err = storage.CreateClient(client)
	assert.Nil(t, err, "%s", err)

	got, err = storage.GetClient(client.Id)
	assert.Nil(t, err, "%s", err)
	assert.Equal(t, client, got)

	err = storage.RemoveClient(client.Id)
	assert.Nil(t, err, "%s", err)

	got, err = storage.GetClient(client.Id)
	assert.Equal(t, ErrClientNotFound, err)
	assert.Nil(t, got)
}

func TestAccess(t *testing.T) {
	t.Parallel()
	storageConfig := CreateStorageConfig("Access")
	storageConfig.CreateUserData = func() interface{} {
		return &UserDataTest{}
	}
	var err error
	svc := createDynamoDB()
	storage := New(svc, storageConfig)
	err = storage.CreateSchema()
	assert.Nil(t, err, "%s", err)
	defer storage.DropSchema()
	client := &osin.DefaultClient{
		Id:     "1234",
		Secret: "aabbccdd",
	}
	err = storage.CreateClient(client)
	assert.Nil(t, err, "%s", err)
	accessData := &osin.AccessData{
		Client:       client,
		AccessToken:  "1",
		RefreshToken: "r9999",
		ExpiresIn:    3600,
		CreatedAt:    time.Now(),
		UserData: &UserDataTest{
			Username: "kamil@uniplaces.com",
		},
	}

	// When AccessToken is saved, RefreshToken should be saved too, so we check for both
	got, err := storage.LoadAccess(accessData.AccessToken)
	assert.Equal(t, ErrAccessNotFound, err)
	assert.Nil(t, got)
	got, err = storage.LoadRefresh(accessData.RefreshToken)
	assert.Equal(t, ErrRefreshNotFound, err)
	assert.Nil(t, got)

	// We only SaveAccess data
	err = storage.SaveAccess(accessData)
	assert.Nil(t, err, "%s", err)

	got, err = storage.LoadAccess(accessData.AccessToken)
	// We need to convert it to json as pointers inside structs are different
	// and assert library doesn't provide recursive value comparison for structs
	assert.Nil(t, err, "%s", err)
	gotJSON, err := json.Marshal(got)
	assert.Nil(t, err, "%s", err)
	expectedJSON, err := json.Marshal(accessData)
	assert.Nil(t, err, "%s", err)
	assert.JSONEq(t, string(expectedJSON), string(gotJSON))

	got, err = storage.LoadRefresh(accessData.RefreshToken)
	// We need to convert it to json as pointers inside structs are different
	// and assert library doesn't provide recursive value comparison for structs
	assert.Nil(t, err, "%s", err)
	gotJSON, err = json.Marshal(got)
	assert.Nil(t, err, "%s", err)
	expectedJSON, err = json.Marshal(accessData)
	assert.Nil(t, err, "%s", err)
	assert.JSONEq(t, string(expectedJSON), string(gotJSON))

	err = storage.RemoveAccess(accessData.AccessToken)
	assert.Nil(t, err, "%s", err)

	got, err = storage.LoadAccess(accessData.AccessToken)
	assert.Equal(t, ErrAccessNotFound, err)
	assert.Nil(t, got)
	// RefreshToken should be still there
	got, err = storage.LoadRefresh(accessData.RefreshToken)
	// We need to convert it to json as pointers inside structs are different
	// and assert library doesn't provide recursive value comparison for structs
	assert.Nil(t, err, "%s", err)
	gotJSON, err = json.Marshal(got)
	assert.Nil(t, err, "%s", err)
	expectedJSON, err = json.Marshal(accessData)
	assert.Nil(t, err, "%s", err)
	assert.JSONEq(t, string(expectedJSON), string(gotJSON))

	// let's try with expired token
	accessData.CreatedAt = accessData.CreatedAt.Add(-time.Duration(accessData.ExpiresIn) * time.Second)
	err = storage.SaveAccess(accessData)
	assert.Nil(t, err, "%s", err)

	got, err = storage.LoadAccess(accessData.AccessToken)
	assert.Equal(t, ErrTokenExpired, err)
	assert.Nil(t, got)
	got, err = storage.LoadRefresh(accessData.RefreshToken)
	assert.Equal(t, ErrTokenExpired, err)
	assert.Nil(t, got)
}

func TestRefresh(t *testing.T) {
	t.Parallel()
	storageConfig := CreateStorageConfig("Refresh")
	var err error
	svc := createDynamoDB()
	storage := New(svc, storageConfig)
	err = storage.CreateSchema()
	assert.Nil(t, err, "%s", err)
	defer storage.DropSchema()
	client := &osin.DefaultClient{
		Id:     "1234",
		Secret: "aabbccdd",
	}
	err = storage.CreateClient(client)
	assert.Nil(t, err, "%s", err)
	accessData := &osin.AccessData{
		Client:       client,
		AccessToken:  "1",
		RefreshToken: "r9999",
		ExpiresIn:    3600,
		CreatedAt:    time.Now(),
	}

	got, err := storage.LoadRefresh(accessData.RefreshToken)
	assert.Equal(t, ErrRefreshNotFound, err)
	assert.Nil(t, got)

	err = storage.SaveRefresh(accessData)
	assert.Nil(t, err, "%s", err)

	got, err = storage.LoadRefresh(accessData.RefreshToken)
	// We need to convert it to json as pointers inside structs are different
	// and assert library doesn't provide recursive value comparison for structs
	assert.Nil(t, err, "%s", err)
	gotJSON, err := json.Marshal(got)
	assert.Nil(t, err, "%s", err)
	expectedJSON, err := json.Marshal(accessData)
	assert.Nil(t, err, "%s", err)
	assert.JSONEq(t, string(expectedJSON), string(gotJSON))

	err = storage.RemoveRefresh(accessData.RefreshToken)
	assert.Nil(t, err, "%s", err)

	got, err = storage.LoadRefresh(accessData.RefreshToken)
	assert.Equal(t, ErrRefreshNotFound, err)
	assert.Nil(t, got)

	// let's try with expired token
	accessData.CreatedAt = accessData.CreatedAt.Add(-time.Duration(accessData.ExpiresIn) * time.Second)
	err = storage.SaveRefresh(accessData)
	assert.Nil(t, err, "%s", err)

	got, err = storage.LoadRefresh(accessData.RefreshToken)
	assert.Equal(t, ErrTokenExpired, err)
	assert.Nil(t, got)
}

func TestAuthorize(t *testing.T) {
	t.Parallel()
	storageConfig := CreateStorageConfig("Authorize")
	var err error
	svc := createDynamoDB()
	storage := New(svc, storageConfig)
	err = storage.CreateSchema()
	assert.Nil(t, err, "%s", err)
	defer storage.DropSchema()
	client := &osin.DefaultClient{
		Id:     "1234",
		Secret: "aabbccdd",
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

	got, err := storage.LoadAuthorize(authorizeData.Code)
	assert.Equal(t, ErrAuthorizeNotFound, err)
	assert.Nil(t, got)

	err = storage.SaveAuthorize(authorizeData)
	assert.Nil(t, err, "%s", err)

	got, err = storage.LoadAuthorize(authorizeData.Code)
	// We need to convert it to json as pointers inside structs are different
	// and assert library doesn't provide recursive value comparison for structs
	assert.Nil(t, err, "%s", err)
	gotJSON, err := json.Marshal(got)
	assert.Nil(t, err, "%s", err)
	expectedJSON, err := json.Marshal(authorizeData)
	assert.Nil(t, err, "%s", err)
	assert.JSONEq(t, string(expectedJSON), string(gotJSON))

	err = storage.RemoveAuthorize(authorizeData.Code)
	assert.Nil(t, err, "%s", err)

	got, err = storage.LoadAuthorize(authorizeData.Code)
	assert.Equal(t, ErrAuthorizeNotFound, err)
	assert.Nil(t, got)

	// let's try with expired token
	authorizeData.CreatedAt = authorizeData.CreatedAt.Add(-time.Duration(authorizeData.ExpiresIn) * time.Second)
	err = storage.SaveAuthorize(authorizeData)
	assert.Nil(t, err, "%s", err)

	got, err = storage.LoadAuthorize(authorizeData.Code)
	assert.Equal(t, ErrTokenExpired, err)
	assert.Nil(t, got)
}

type UserDataTest struct {
	Username string
}

func (receiver UserDataTest) ToAttributeValues() map[string]*dynamodb.AttributeValue {
	return map[string]*dynamodb.AttributeValue{
		"username": {
			S: aws.String(receiver.Username),
		},
	}
}
