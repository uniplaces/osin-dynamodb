package osindynamodb

import (
	"encoding/json"
	"errors"
	"github.com/RangelReale/osin"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"time"
)

var (
	ErrClientNotFound    = errors.New("Client not found")
	ErrAuthorizeNotFound = errors.New("Authorize not found")
	ErrAccessNotFound    = errors.New("Access not found")
	ErrRefreshNotFound   = errors.New("Refresh not found")
	ErrTokenExpired      = errors.New("Token expired")
)

type UserData interface {
	ToAttributeValues() map[string]*dynamodb.AttributeValue
}

type StorageConfig struct {
	ClientTable    string
	AuthorizeTable string
	AccessTable    string
	RefreshTable   string
	CreateUserData func() interface{}
}

type Storage struct {
	db     *dynamodb.DynamoDB
	config StorageConfig
}

func New(db *dynamodb.DynamoDB, config StorageConfig) *Storage {

	return &Storage{
		db:     db,
		config: config,
	}
}

// CreateSchema initiates db with basic schema layout
// This is not a part of interface but can be useful for initiating basic schema and for tests
func (self *Storage) CreateSchema() error {
	createParams := []*dynamodb.CreateTableInput{
		&dynamodb.CreateTableInput{
			TableName: aws.String(self.config.AccessTable),
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
		&dynamodb.CreateTableInput{
			TableName: aws.String(self.config.AuthorizeTable),
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
		&dynamodb.CreateTableInput{
			TableName: aws.String(self.config.ClientTable),
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
		&dynamodb.CreateTableInput{
			TableName: aws.String(self.config.RefreshTable),
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
		if err := createTable(self.db, createParams[i]); err != nil {
			return err
		}
	}

	return nil
}

// DropSchema drops all tables
// This is not a part of interface but can be useful in tests
func (self *Storage) DropSchema() error {
	tables := []string{
		self.config.AccessTable,
		self.config.AuthorizeTable,
		self.config.RefreshTable,
		self.config.ClientTable,
	}
	for i := range tables {
		if err := deleteTable(self.db, tables[i]); err != nil {
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
func (self *Storage) Clone() osin.Storage {
	return self
}

// Close the resources the Storage potentially holds. Has no effect with this library, it's only to satisfy interface.
func (self *Storage) Close() {
}

// CreateClient adds new client.
// This is not a part of interface and as so, it's never used in osin flow.
// However can be really usefull for applications to add new clients.
func (self *Storage) CreateClient(client osin.Client) error {
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
		TableName: aws.String(self.config.ClientTable),
	}

	if _, err := self.db.PutItem(params); err != nil {
		return err
	}

	return nil
}

// GetClient loads the client by id (client_id)
func (self *Storage) GetClient(id string) (osin.Client, error) {
	var client *osin.DefaultClient

	params := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String(id),
			},
		},
		ProjectionExpression: aws.String("id, json"),
		TableName:            aws.String(self.config.ClientTable),
	}

	resp, err := self.db.GetItem(params)
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
func (self *Storage) RemoveClient(id string) error {
	params := &dynamodb.DeleteItemInput{
		TableName: aws.String(self.config.ClientTable),
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String(id),
			},
		},
	}

	_, err := self.db.DeleteItem(params)
	if err != nil {
		return err
	}

	return nil
}

// SaveAuthorize saves authorize data.
func (self *Storage) SaveAuthorize(authorizeData *osin.AuthorizeData) error {
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
		TableName: aws.String(self.config.AuthorizeTable),
	}

	if _, err := self.db.PutItem(params); err != nil {
		return err
	}

	return nil
}

// LoadAuthorize looks up AuthorizeData by a code.
// Client information is loaded together.
// Can return error if expired.
func (self *Storage) LoadAuthorize(code string) (authorizeData *osin.AuthorizeData, err error) {
	params := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"code": {
				S: aws.String(code),
			},
		},
		ProjectionExpression: aws.String("json"),
		TableName:            aws.String(self.config.AuthorizeTable),
	}

	resp, err := self.db.GetItem(params)
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
func (self *Storage) RemoveAuthorize(code string) error {
	params := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"code": {
				S: aws.String(code),
			},
		},
		TableName: aws.String(self.config.AuthorizeTable),
	}

	if _, err := self.db.DeleteItem(params); err != nil {
		return err
	}

	return nil
}

// SaveAccess writes AccessData.
func (self *Storage) SaveAccess(accessData *osin.AccessData) error {
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
		TableName: aws.String(self.config.AccessTable),
	}

	if _, err := self.db.PutItem(params); err != nil {
		return err
	}

	if accessData.RefreshToken != "" {
		return self.SaveRefresh(accessData)
	}

	return nil
}

// LoadAccess retrieves access data by token. Client information is loaded together.
// Can return error if expired.
func (self *Storage) LoadAccess(token string) (accessData *osin.AccessData, err error) {
	params := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"token": {
				S: aws.String(token),
			},
		},
		ProjectionExpression: aws.String("json"),
		TableName:            aws.String(self.config.AccessTable),
	}

	resp, err := self.db.GetItem(params)
	if err != nil {
		return nil, err
	}

	if len(resp.Item) == 0 {
		return nil, ErrAccessNotFound
	}

	accessData = &osin.AccessData{}
	accessData.Client = &osin.DefaultClient{}
	if self.config.CreateUserData != nil {
		accessData.UserData = self.config.CreateUserData()
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
func (self *Storage) RemoveAccess(token string) error {
	params := &dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"token": {
				S: aws.String(token),
			},
		},
		TableName: aws.String(self.config.AccessTable),
	}

	if _, err := self.db.DeleteItem(params); err != nil {
		return err
	}

	return nil
}

// SaveRefresh writes AccessData for refresh token
// This method is not a part of interface and as so, it's never used in osin flow.
// This method is used internally by SaveAccess(accessData *osin.AccessData)
// and can be usefull for testing
func (self *Storage) SaveRefresh(accessData *osin.AccessData) error {
	data, err := json.Marshal(accessData)
	if err != nil {
		return err
	}
	params := &dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			"token": {
				S: aws.String(accessData.RefreshToken),
			},
			"json": {
				S: aws.String(string(data)),
			},
		},
		TableName: aws.String(self.config.RefreshTable),
	}

	if _, err := self.db.PutItem(params); err != nil {
		return err
	}

	return nil
}

// LoadRefresh retrieves refresh AccessData. Client information is loaded together.
// Can return error if expired.
func (self *Storage) LoadRefresh(token string) (accessData *osin.AccessData, err error) {
	params := &dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"token": {
				S: aws.String(token),
			},
		},
		ProjectionExpression: aws.String("json"),
		TableName:            aws.String(self.config.RefreshTable),
	}

	resp, err := self.db.GetItem(params)
	if err != nil {
		return nil, err
	}

	if len(resp.Item) == 0 {
		return nil, ErrRefreshNotFound
	}

	accessData = &osin.AccessData{}
	accessData.Client = &osin.DefaultClient{}
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
func (self *Storage) RemoveRefresh(token string) error {
	params := &dynamodb.DeleteItemInput{
		TableName: aws.String(self.config.RefreshTable),
		Key: map[string]*dynamodb.AttributeValue{
			"token": {
				S: aws.String(token),
			},
		},
	}

	_, err := self.db.DeleteItem(params)
	if err != nil {
		return err
	}

	return nil
}

func CreateStorageConfig(prefix string) StorageConfig {
	return StorageConfig{
		AccessTable:    prefix + "access",
		ClientTable:    prefix + "client",
		RefreshTable:   prefix + "refresh",
		AuthorizeTable: prefix + "authorize",
	}
}
