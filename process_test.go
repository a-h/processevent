package processevent

import (
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/google/go-cmp/cmp"
)

type AllTypes struct {
	Binary              []byte         `json:"binary"`
	Boolean             bool           `json:"boolean"`
	BinarySet           [][]byte       `json:"binarySet"`
	List                []string       `json:"list"`
	Map                 map[string]any `json:"map"`
	NumberIntNegative   int            `json:"numberIntNegative"`
	NumberIntZero       int            `json:"numberIntZero"`
	NumberIntPositive   int            `json:"numberIntPositive"`
	NumberFloatNegative float64        `json:"numberFloatNegative"`
	NumberFloatZero     float64        `json:"numberFloatZero"`
	NumberFloatPositive float64        `json:"numberFloatPositive"`
	NumberSet           []string       `json:"numberSet"`
	Null                any            `json:"null"`
	String              string         `json:"string"`
	StringSet           []string       `json:"stringSet"`
}

func TestUnmarshal(t *testing.T) {
	input := events.DynamoDBEvent{
		Records: []events.DynamoDBEventRecord{
			{
				Change: events.DynamoDBStreamRecord{
					NewImage: map[string]events.DynamoDBAttributeValue{
						"binary":  events.NewBinaryAttribute([]byte{0xDE, 0xAD, 0xBE, 0xEF}),
						"boolean": events.NewBooleanAttribute(true),
						"binarySet": events.NewBinarySetAttribute([][]byte{
							{0xDE, 0xAD, 0xBE, 0xEF},
							{0x0D, 0x15, 0xEA, 0x5E},
						}),
						"list": events.NewListAttribute([]events.DynamoDBAttributeValue{
							events.NewStringAttribute("a"),
							events.NewNumberAttribute("1"),
						}),
						"map": events.NewMapAttribute(map[string]events.DynamoDBAttributeValue{
							"innerA": events.NewStringAttribute("value"),
							"innerB": events.NewMapAttribute(map[string]events.DynamoDBAttributeValue{
								"innerInner": events.NewStringAttribute("innerInnerValue"),
							}),
						}),
						"numberIntNegative":   events.NewNumberAttribute("-1"),
						"numberIntZero":       events.NewNumberAttribute("0"),
						"numberIntPositive":   events.NewNumberAttribute("2000"),
						"numberFloatNegative": events.NewNumberAttribute("-0.3"),
						"numberFloatZero":     events.NewNumberAttribute("0.0"),
						"numberFloatPositive": events.NewNumberAttribute("+0.3"),
						"numberSet":           events.NewNumberSetAttribute([]string{"0", "0.5", "1"}),
						"null":                events.NewNullAttribute(),
						"string":              events.NewStringAttribute("string value"),
						"stringSet":           events.NewStringSetAttribute([]string{"A", "B"}),
					},
				},
			},
		},
	}
	expected := AllTypes{
		Binary:  []byte{0xDE, 0xAD, 0xBE, 0xEF},
		Boolean: true,
		BinarySet: [][]byte{
			{0xDE, 0xAD, 0xBE, 0xEF},
			{0x0D, 0x15, 0xEA, 0x5E},
		},
		List: []string{"a", "1"},
		Map: map[string]any{
			"innerA": "value",
			"innerB": map[string]any{
				"innerInner": "innerInnerValue",
			},
		},
		NumberIntNegative:   -1,
		NumberIntZero:       0,
		NumberIntPositive:   2000,
		NumberFloatNegative: -0.3,
		NumberFloatZero:     0.0,
		NumberFloatPositive: 0.3,
		NumberSet:           []string{"0", "0.5", "1"},
		Null:                nil,
		String:              "string value",
		StringSet:           []string{"A", "B"},
	}

	var actual AllTypes
	err := UnmarshalImage(input.Records[0].Change.NewImage, &actual)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if diff := cmp.Diff(expected, actual); diff != "" {
		t.Error(diff)
	}
}
