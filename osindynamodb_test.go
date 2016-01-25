package osindynamodb

import (
	"github.com/RangelReale/osin"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"os"
	"strconv"
	"testing"
)

func createDynamoDB() *dynamodb.DynamoDB {
	os.Clearenv()
	os.Setenv("AWS_ACCESS_KEY_ID", "a")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "b")
	os.Setenv("AWS_SESSION_TOKEN", "")

	return dynamodb.New(session.New(&aws.Config{
		Endpoint: aws.String("http://localhost:4567"),
		Region:   aws.String("us-west-1"),
	}))
}

func TestCreateSchema(t *testing.T) {
	t.Parallel()
	storageConfig := CreateStorageConfig("CreateSchema")
	var err error
	svc := createDynamoDB()
	storage := New(svc, storageConfig)
	err = storage.CreateSchema()
	assert.Nil(t, err, "%s", err)
	defer storage.DropSchema()
}

func TestCreateClient(t *testing.T) {
	t.Parallel()
	storageConfig := CreateStorageConfig("CreateClient")
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
}

// Predictable testing token generation

type TestingAuthorizeTokenGen struct {
	counter int64
}

func (a *TestingAuthorizeTokenGen) GenerateAuthorizeToken(data *osin.AuthorizeData) (ret string, err error) {
	a.counter++
	return strconv.FormatInt(a.counter, 10), nil
}

type TestingAccessTokenGen struct {
	acounter int64
	rcounter int64
}

func (a *TestingAccessTokenGen) GenerateAccessToken(data *osin.AccessData, generaterefresh bool) (accesstoken string, refreshtoken string, err error) {
	a.acounter++
	accesstoken = strconv.FormatInt(a.acounter, 10)

	if generaterefresh {
		a.rcounter++
		refreshtoken = "r" + strconv.FormatInt(a.rcounter, 10)
	}
	return
}
