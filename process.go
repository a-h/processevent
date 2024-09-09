package processevent

import (
	"fmt"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func UnmarshalImage(input map[string]events.DynamoDBAttributeValue, target any) error {
	m := make(map[string]*dynamodb.AttributeValue)
	for k, v := range input {
		av, err := getAttributeValue(v)
		if err != nil {
			return fmt.Errorf("failed to get attribute value for key %s: %w", k, err)
		}
		m[k] = av
	}
	return dynamodbattribute.UnmarshalMap(m, &target)
}

func ptrSlice[T any](v []T) []*T {
	s := make([]*T, len(v))
	for i := range v {
		s[i] = &v[i]
	}
	return s
}

func ptr[T any](v T) *T {
	return &v
}

func listOf(input []events.DynamoDBAttributeValue) (l []*dynamodb.AttributeValue, err error) {
	if input == nil {
		return nil, nil
	}
	l = make([]*dynamodb.AttributeValue, len(input))
	for i, v := range input {
		av, err := getAttributeValue(v)
		if err != nil {
			return nil, fmt.Errorf("failed to get attribute value for index %d: %w", i, err)
		}
		l[i] = av
	}
	return l, nil
}

func mapOf(input map[string]events.DynamoDBAttributeValue) (m map[string]*dynamodb.AttributeValue, err error) {
	if input == nil {
		return nil, nil
	}
	m = make(map[string]*dynamodb.AttributeValue, len(input))
	for k, v := range input {
		av, err := getAttributeValue(v)
		if err != nil {
			return nil, fmt.Errorf("failed to get attribute value for key %s: %w", k, err)
		}
		m[k] = av
	}
	return m, nil
}

func getAttributeValue(av events.DynamoDBAttributeValue) (output *dynamodb.AttributeValue, err error) {
	switch av.DataType() {
	case events.DataTypeBinary:
		return &dynamodb.AttributeValue{B: av.Binary()}, nil
	case events.DataTypeBoolean:
		return &dynamodb.AttributeValue{BOOL: ptr(av.Boolean())}, nil
	case events.DataTypeBinarySet:
		return &dynamodb.AttributeValue{BS: av.BinarySet()}, nil
	case events.DataTypeList:
		list, err := listOf(av.List())
		if err != nil {
			return nil, err
		}
		return &dynamodb.AttributeValue{L: list}, nil
	case events.DataTypeMap:
		m, err := mapOf(av.Map())
		if err != nil {
			return nil, err
		}
		return &dynamodb.AttributeValue{M: m}, nil
	case events.DataTypeNumber:
		return &dynamodb.AttributeValue{N: ptr(av.Number())}, nil
	case events.DataTypeNumberSet:
		return &dynamodb.AttributeValue{NS: ptrSlice(av.NumberSet())}, nil
	case events.DataTypeNull:
		return nil, nil
	case events.DataTypeString:
		return &dynamodb.AttributeValue{S: ptr(av.String())}, nil
	case events.DataTypeStringSet:
		return &dynamodb.AttributeValue{SS: ptrSlice(av.StringSet())}, nil
	}
	return nil, fmt.Errorf("unknown DynamoDBAttributeValue type: %T", av.DataType())
}
