package main

const (
	ERROR_NONE                       = 0
	ERROR_UNKNOWN_TOPIC_OR_PARTITION = 3
	ERROR_UNSUPPORTED_VERSION        = 35
)

type ApiKeyAndMinMaxVersion struct {
	ApiKey     uint16
	Name       string
	MinVersion uint16
	MaxVersion uint16
}

var ApiKeyAndMinMaxVersions = map[uint16]ApiKeyAndMinMaxVersion{
	0: {
		ApiKey:     0,
		Name:       "Produce",
		MinVersion: 0,
		MaxVersion: 11,
	},
	1: {
		ApiKey:     1,
		Name:       "Fetch",
		MinVersion: 0,
		MaxVersion: 16,
	},
	18: {
		ApiKey:     18,
		Name:       "ApiVersions",
		MinVersion: 0,
		MaxVersion: 4,
	},
	75: {
		ApiKey:     75,
		Name:       "DescribeTopicPartitions",
		MinVersion: 0,
		MaxVersion: 0,
	},
}
