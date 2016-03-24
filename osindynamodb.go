package osindynamodb

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/RangelReale/osin"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var (
	// ErrClientNotFound is returned by GetClient if client was not found
	ErrClientNotFound = errors.New("Client not found")
	// ErrAuthorizeNotFound is returned by LoadAuthorize if authorization code was not found
	ErrAuthorizeNotFound = errors.New("Authorize not found")
	// ErrAccessNotFound is returned by LoadAccess if access token was not found
	ErrAccessNotFound = errors.New("Access not found")
	// ErrRefreshNotFound is returned by LoadRefresh if refresh token was not found
	ErrRefreshNotFound = errors.New("Refresh not found")
	// ErrTokenExpired is returned by LoadAccess, LoadAuthorize or LoadRefresh if token or code expired
	ErrTokenExpired = errors.New("Token expired")
)

// New returns a new DynamoDB storage instance.
func New(db *dynamodb.DynamoDB, config StorageConfig) *Storage {
	return &Storage{
		db:     db,
		config: config,
	}
}

// Storage implements the storage interface for OSIN (https://github.com/RangelReale/osin)
// with Amazon DynamoDB (https://aws.amazon.com/dynamodb/)
// using aws-sdk-go (https://github.com/aws/aws-sdk-go).
type Storage struct {
	db     *dynamodb.DynamoDB
	config StorageConfig
}

// StorageConfig allows to pass configuration to Storage on initialization
type StorageConfig struct {
	// ClientTable is the name of table for clients
	ClientTable string
	// AuthorizeTable is the name of table for authorization codes
	AuthorizeTable string
	// AccessTable is the name of table for access tokens
	AccessTable string
	// RefreshTable is the name of table for refresh tokens
	RefreshTable string
	// CreateUserData is a function that allows you to create struct
	// to which osin.AccessData.UserData will be json.Unmarshaled.
	// Example:
	// struct AppUserData{
	// 	Username string
	// }
	// func() interface{} {
	// 	return &AppUserData{}
	// }
	CreateUserData func() interface{}
}

// UserData is an interface that allows you to store UserData values
// as DynamoDB attributes in AccessTable and RefreshTable
type UserData interface {
	// ToAttributeValues lists user data as attribute values for DynamoDB table
	ToAttributeValues() map[string]*dynamodb.AttributeValue
}

// CreateSchema initiates db with basic schema layout
// This is not a part of interface but can be useful for initiating basic schema and for tests
func (receiver *Storage) CreateSchema() error {
	createParams := []*dynamodb.CreateTableInput{
		{
			TableName: aws.String(receiver.config.AccessTable),
			AttributeDefinitions: []*dynamodb.AttributeDefinition{
				{
					AttributeName: aws.String("token"),
					AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
				},
			},
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String("token"),
					KeyType:       aws.String("HASH"),
				},
			},
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(1),
				WriteCapacityUnits: aws.Int64(1),
			},
		},
		{
			TableName: aws.String(receiver.config.AuthorizeTable),
			AttributeDefinitions: []*dynamodb.AttributeDefinition{
				{
					AttributeName: aws.String("code"),
					AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
				},
			},
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String("code"),
					KeyType:       aws.String("HASH"),
				},
			},
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(1),
				WriteCapacityUnits: aws.Int64(1),
			},
		},
		{
			TableName: aws.String(receiver.config.ClientTable),
			AttributeDefinitions: []*dynamodb.AttributeDefinition{
				{
					AttributeName: aws.String("id"),
					AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
				},
			},
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String("id"),
					KeyType:       aws.String("HASH"),
				},
			},
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(1),
				WriteCapacityUnits: aws.Int64(1),
			},
		},
		{
			TableName: aws.String(receiver.config.RefreshTable),
			AttributeDefinitions: []*dynamodb.AttributeDefinition{
				{
					AttributeName: aws.String("token"),
					AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
				},
			},
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String("token"),
					KeyType:       aws.String("HASH"),
				},
			},
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(1),
				WriteCapacityUnits: aws.Int64(1),
			},
		},
	}

	for i := range createParams {
		if err := createTable(receiver.db, createParams[i]); err != nil {
			return err
		}
	}

	return nil
}

// DropSchema drops all tables
// This is not a part of interface but can be useful in tests
func (receiver *Storage) DropSchema() error {
	tables := []string{
		receiver.config.AccessTable,
		receiver.config.AuthorizeTable,
		receiver.config.RefreshTable,
		receiver.config.ClientTable,
	}
	for i := range tables {
		if err := deleteTable(receiver.db, tables[i]); err != nil {
			return err
		}
	}
	return nil
}

func createTable(db *dynamodb.DynamoDB, createParams *dynamodb.CreateTableInput) error {
	_, err := db.CreateTable(createParams)
	if err != nil {
		return err
	}

	describeParams := &dynamodb.DescribeTableInput{
		TableName: aws.String(*createParams.TableName),
	}
	if err := db.WaitUntilTableExists(describeParams); err != nil {
		return err
	}

	return nil
}

func deleteTable(db *dynamodb.DynamoDB, tableName string) error {
	params := &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	}
	_, err := db.DeleteTable(params)
	if err != nil {
		return err
	}

	describeParams := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}
	if err := db.WaitUntilTableNotExists(describeParams); err != nil {
		return err
	}

	return nil
}

// Clone the storage if needed. Has no effect with this library, it's only to satisfy interface.
func (receiver *Storage) Clone() osin.Storage {
	return receiver
}

// Close the resources the Storage potentially holds. Has no effect with this library, it's only to satisfy interface.
func (receiver *Storage) Close() {
}

// CreateClient adds new client.
// This is not a part of interface and as so, it's never used in osin flow.
// However can be really usefull for applications to add new clients.
func (receiver *Storage) CreateClient(client osin.Client) error {
	data, err := json.Marshal(client)
	if err != nil {
		return err
	}

	params := &dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String(client.GetId()),
			},
			"json": {
				S: aws.String(string(data)),
			},
		},
		TableName: aws.String(receiver.config.ClientTable),
	}

	if _, err := receiver.db.PutItem(params); err != nil {
		return err
	}

	return nil
}

// GetClient loads the client by id (client_id)
func (receiver *Storage) GetClient(id string) (osin.Client, error) {
	var client *osin.DefaultClient

	params := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String(id),
			},
		},
		ProjectionExpression: aws.String("id, json"),
		TableName:            aws.String(receiver.config.ClientTable),
	}

	resp, err := receiver.db.GetItem(params)
	if err != nil {
		return nil, err
	}

	if len(resp.Item) == 0 {
		return nil, ErrClientNotFound
	}

	data := resp.Item["json"].S
	err = json.Unmarshal([]byte(*data), &client)
	if err != nil {
		return nil, err
	}
	return client, nil
}

// RemoveClient revokes or deletes client.
// This is not a part of interface and as so, it's never used in osin flow.
// However can be really usefull for applications to remove or revoke clients.
func (receiver *Storage) RemoveClient(id string) error {
	params := &dynamodb.DeleteItemInput{
		TableName: aws.String(receiver.config.ClientTable),
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String(id),
			},
		},
	}

	_, err := receiver.db.DeleteItem(params)
	if err != nil {
		return err
	}

	return nil
}

// SaveAuthorize saves authorize data.
func (receiver *Storage) SaveAuthorize(authorizeData *osin.AuthorizeData) error {
	data, err := json.Marshal(authorizeData)
	if err != nil {
		return err
	}
	params := &dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			"code": {
				S: aws.String(authorizeData.Code),
			},
			"json": {
				S: aws.String(string(data)),
			},
		},
		TableName: aws.String(receiver.config.AuthorizeTable),
	}

	if _, err := receiver.db.PutItem(params); err != nil {
		return err
	}

	return nil
}

// LoadAuthorize looks up AuthorizeData by a code.
// Client information is loaded together.
// Can return error if expired.
func (receiver *Storage) LoadAuthorize(code string) (authorizeData *osin.AuthorizeData, err error) {
	params := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"code": {
				S: aws.String(code),
			},
		},
		ProjectionExpression: aws.String("json"),
		TableName:            aws.String(receiver.config.AuthorizeTable),
	}

	resp, err := receiver.db.GetItem(params)
	if err != nil {
		return nil, err
	}

	if len(resp.Item) == 0 {
		return nil, ErrAuthorizeNotFound
	}

	authorizeData = &osin.AuthorizeData{}
	authorizeData.Client = &osin.DefaultClient{}
	data := resp.Item["json"].S
	err = json.Unmarshal([]byte(*data), &authorizeData)
	if err != nil {
		return nil, err
	}

	if authorizeData.ExpireAt().Before(time.Now()) {
		return nil, ErrTokenExpired
	}

	return authorizeData, nil
}

// RemoveAuthorize revokes or deletes the authorization code.
func (receiver *Storage) RemoveAuthorize(code string) error {
	params := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"code": {
				S: aws.String(code),
			},
		},
		TableName: aws.String(receiver.config.AuthorizeTable),
	}

	if _, err := receiver.db.DeleteItem(params); err != nil {
		return err
	}

	return nil
}

// SaveAccess writes AccessData.
func (receiver *Storage) SaveAccess(accessData *osin.AccessData) error {
	data, err := json.Marshal(accessData)
	if err != nil {
		return err
	}
	items := map[string]*dynamodb.AttributeValue{
		"token": {
			S: aws.String(accessData.AccessToken),
		},
		"json": {
			S: aws.String(string(data)),
		},
	}

	if userData, ok := accessData.UserData.(UserData); ok {
		for k, v := range userData.ToAttributeValues() {
			items[k] = v
		}
	}
	params := &dynamodb.PutItemInput{
		Item:      items,
		TableName: aws.String(receiver.config.AccessTable),
	}

	if _, err := receiver.db.PutItem(params); err != nil {
		return err
	}

	if accessData.RefreshToken != "" {
		return receiver.SaveRefresh(accessData)
	}

	return nil
}

// LoadAccess retrieves access data by token. Client information is loaded together.
// Can return error if expired.
func (receiver *Storage) LoadAccess(token string) (accessData *osin.AccessData, err error) {
	params := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"token": {
				S: aws.String(token),
			},
		},
		ProjectionExpression: aws.String("json"),
		TableName:            aws.String(receiver.config.AccessTable),
	}

	resp, err := receiver.db.GetItem(params)
	if err != nil {
		return nil, err
	}

	if len(resp.Item) == 0 {
		return nil, ErrAccessNotFound
	}

	accessData = &osin.AccessData{}
	accessData.Client = &osin.DefaultClient{}
	if accessData.AccessData != nil {
		accessData.AccessData.Client = &osin.DefaultClient{}
	}
	if accessData.AuthorizeData != nil {
		accessData.AuthorizeData.Client = &osin.DefaultClient{}
	}
	if receiver.config.CreateUserData != nil {
		accessData.UserData = receiver.config.CreateUserData()
	}
	data := resp.Item["json"].S
	err = json.Unmarshal([]byte(*data), &accessData)
	if err != nil {
		return nil, err
	}
	if accessData.ExpireAt().Before(time.Now()) {
		return nil, ErrTokenExpired
	}
	return accessData, nil
}

// RemoveAccess revokes or deletes an AccessData.
func (receiver *Storage) RemoveAccess(token string) error {
	params := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"token": {
				S: aws.String(token),
			},
		},
		TableName: aws.String(receiver.config.AccessTable),
	}

	if _, err := receiver.db.DeleteItem(params); err != nil {
		return err
	}

	return nil
}

// SaveRefresh writes AccessData for refresh token
// This method is not a part of interface and as so, it's never used in osin flow.
// This method is used internally by SaveAccess(accessData *osin.AccessData)
// and can be usefull for testing
func (receiver *Storage) SaveRefresh(accessData *osin.AccessData) error {
	data, err := json.Marshal(accessData)
	if err != nil {
		return err
	}
	items := map[string]*dynamodb.AttributeValue{
		"token": {
			S: aws.String(accessData.RefreshToken),
		},
		"json": {
			S: aws.String(string(data)),
		},
	}

	if userData, ok := accessData.UserData.(UserData); ok {
		for k, v := range userData.ToAttributeValues() {
			items[k] = v
		}
	}
	params := &dynamodb.PutItemInput{
		Item:      items,
		TableName: aws.String(receiver.config.RefreshTable),
	}

	if _, err := receiver.db.PutItem(params); err != nil {
		return err
	}

	return nil
}

// LoadRefresh retrieves refresh AccessData. Client information is loaded together.
// Can return error if expired.
func (receiver *Storage) LoadRefresh(token string) (accessData *osin.AccessData, err error) {
	params := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"token": {
				S: aws.String(token),
			},
		},
		ProjectionExpression: aws.String("json"),
		TableName:            aws.String(receiver.config.RefreshTable),
	}

	resp, err := receiver.db.GetItem(params)
	if err != nil {
		return nil, err
	}

	if len(resp.Item) == 0 {
		return nil, ErrRefreshNotFound
	}

	accessData = &osin.AccessData{}
	accessData.Client = &osin.DefaultClient{}
	if accessData.AccessData != nil {
		accessData.AccessData.Client = &osin.DefaultClient{}
	}
	if accessData.AuthorizeData != nil {
		accessData.AuthorizeData.Client = &osin.DefaultClient{}
	}
	data := resp.Item["json"].S
	err = json.Unmarshal([]byte(*data), &accessData)
	if err != nil {
		return nil, err
	}
	if accessData.ExpireAt().Before(time.Now()) {
		return nil, ErrTokenExpired
	}
	return accessData, nil
}

// RemoveRefresh revokes or deletes refresh AccessData.
func (receiver *Storage) RemoveRefresh(token string) error {
	params := &dynamodb.DeleteItemInput{
		TableName: aws.String(receiver.config.RefreshTable),
		Key: map[string]*dynamodb.AttributeValue{
			"token": {
				S: aws.String(token),
			},
		},
	}

	_, err := receiver.db.DeleteItem(params)
	if err != nil {
		return err
	}

	return nil
}

// CreateStorageConfig prefixes all table names and returns StorageConfig
func CreateStorageConfig(prefix string) StorageConfig {
	return StorageConfig{
		AccessTable:    prefix + "access",
		ClientTable:    prefix + "client",
		RefreshTable:   prefix + "refresh",
		AuthorizeTable: prefix + "authorize",
	}
}
