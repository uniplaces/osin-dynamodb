# osin-dynamodb

[![Circle CI](https://circleci.com/gh/uniplaces/osin-dynamodb.svg?style=shield)](https://circleci.com/gh/uniplaces/osin-dynamodb)
[![GoDoc](https://godoc.org/github.com/uniplaces/osin-dynamodb?status.svg)](https://godoc.org/github.com/uniplaces/osin-dynamodb)
[![Coverage Status](https://coveralls.io/repos/github/uniplaces/osin-dynamodb/badge.svg?branch=master)](https://coveralls.io/github/uniplaces/osin-dynamodb?branch=master)
[![Report Card](http://goreportcard.com/badge/uniplaces/osin-dynamodb)](http://goreportcard.com/report/uniplaces/osin-dynamodb)

This package implements the storage for [OSIN](https://github.com/RangelReale/osin) with [Amazon DynamoDB](https://aws.amazon.com/dynamodb/) using [aws-sdk-go](https://github.com/aws/aws-sdk-go).

## Installation

Install library with `go get github.com/uniplaces/osin-dynamodb`

or if you use [glide](https://github.com/Masterminds/glide) with `glide get github.com/uniplaces/osin-dynamodb`

## Usage

```go
import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/uniplaces/osin-dynamodb"
	"github.com/RangelReale/osin"
	"os"
)

func main() {
    // This is configuration for local DynamoDB instance used in tests,
    // for details how to configure your real AWS DynamoDB connection check DynamoDB documentation
	os.Clearenv()
	os.Setenv("AWS_ACCESS_KEY_ID", "a")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "b")

	svc := dynamodb.New(session.New(&aws.Config{
		Endpoint: aws.String("http://localhost:4567"),
		Region:   aws.String("us-west-1"),
	}))
	
	// You can use CreateStorageConfig helper to create configuration or you can create it by yourself
	storageConfig := osindynamodb.CreateStorageConfig("oauth_table_prefix_")
	
	// Initialization
    store := osindynamodb.New(svc, storageConfig)
    server := osin.NewServer(osin.NewServerConfig(), store)

    // For further details how to use osin server check osin documentation
}
```