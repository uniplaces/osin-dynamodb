package osindynamodb

import (
	"encoding/json"
	"fmt"
	"github.com/RangelReale/osin"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"time"
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

func (self *Storage) CreateSchema() error {
	if err := createTableAccess(self.db, self.config.AccessTable); err != nil {
		return err
	}
	if err := createTableAuthorize(self.db, self.config.AuthorizeTable); err != nil {
		return err
	}
	if err := createTableClient(self.db, self.config.ClientTable); err != nil {
		return err
	}
	if err := createTableRefresh(self.db, self.config.RefreshTable); err != nil {
		return err
	}
	return nil
}

func (self *Storage) DropSchema() error {
	if err := deleteTable(self.db, self.config.AccessTable); err != nil {
		return err
	}
	if err := deleteTable(self.db, self.config.AuthorizeTable); err != nil {
		return err
	}
	if err := deleteTable(self.db, self.config.ClientTable); err != nil {
		return err
	}
	if err := deleteTable(self.db, self.config.RefreshTable); err != nil {
		return err
	}
	return nil
}

func createTableAccess(db *dynamodb.DynamoDB, tableName string) error {
	createParams := &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
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
	}
	_, err := db.CreateTable(createParams)
	if err != nil {
		return err
	}

	describeParams := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}
	if err := db.WaitUntilTableExists(describeParams); err != nil {
		return err
	}

	return nil
}

func createTableAuthorize(db *dynamodb.DynamoDB, tableName string) error {
	createParams := &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
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
	}
	_, err := db.CreateTable(createParams)
	if err != nil {
		return err
	}

	describeParams := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}
	if err := db.WaitUntilTableExists(describeParams); err != nil {
		return err
	}

	return nil
}

func createTableClient(db *dynamodb.DynamoDB, tableName string) error {
	createParams := &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
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
	}
	_, err := db.CreateTable(createParams)
	if err != nil {
		return err
	}

	describeParams := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}
	if err := db.WaitUntilTableExists(describeParams); err != nil {
		return err
	}

	return nil
}

func createTableRefresh(db *dynamodb.DynamoDB, tableName string) error {
	createParams := &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
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
	}
	_, err := db.CreateTable(createParams)
	if err != nil {
		return err
	}

	describeParams := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
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

// @todo
// NOT A PART OF INTERFACE
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
		return nil, fmt.Errorf("Client not found")
	}

	data := resp.Item["json"].S
	err = json.Unmarshal([]byte(*data), &client)
	if err != nil {
		return nil, err
	}
	return client, nil
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
		return nil, fmt.Errorf("Authorize not found")
	}

	authorizeData = &osin.AuthorizeData{}
	authorizeData.Client = &osin.DefaultClient{}
	data := resp.Item["json"].S
	err = json.Unmarshal([]byte(*data), &authorizeData)
	if err != nil {
		return nil, err
	}

	if authorizeData.ExpireAt().Before(time.Now()) {
		return nil, fmt.Errorf("Token expired at %s.", authorizeData.ExpireAt().String())
	}

	return authorizeData, nil
}

// RemoveAuthorize revokes or deletes the authorization code.
// @todo
func (self *Storage) RemoveAuthorize(code string) error {
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
		return nil, fmt.Errorf("Access not found")
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
		return nil, fmt.Errorf("Token expired at %s.", accessData.ExpireAt().String())
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

// @todo
// NOT A PART OF INTERFACE
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
		return nil, fmt.Errorf("Refresh not found")
	}

	accessData = &osin.AccessData{}
	accessData.Client = &osin.DefaultClient{}
	data := resp.Item["json"].S
	err = json.Unmarshal([]byte(*data), &accessData)
	if err != nil {
		return nil, err
	}
	if accessData.ExpireAt().Before(time.Now()) {
		return nil, fmt.Errorf("Token expired at %s.", accessData.ExpireAt().String())
	}
	return accessData, nil
}

// RemoveRefresh revokes or deletes refresh AccessData.
func (self *Storage) RemoveRefresh(code string) error {
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
