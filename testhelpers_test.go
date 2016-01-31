package osindynamodb

import (
	"github.com/RangelReale/osin"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"os"
	"strconv"
)

// createDynamoDB instance
func createDynamoDB() *dynamodb.DynamoDB {
	os.Clearenv()
	os.Setenv("AWS_ACCESS_KEY_ID", "a")     // we use local DynamoDB so we just need to pass any key
	os.Setenv("AWS_SECRET_ACCESS_KEY", "b") // we use local DynamoDB so we just need to pass any key

	return dynamodb.New(session.New(&aws.Config{
		Endpoint: aws.String("http://localhost:4567"),
		Region:   aws.String("us-west-1"),
	}))
}

// Predictable testing token generation
// from: https://github.com/RangelReale/osin/blob/cca734bceea0eb44cc87f5e36fd6e2648f5e8580/storage_test.go#L129
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
