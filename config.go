package gkafka

type Config struct {
	Addr      []string `json:"addr" ftV:"addr"`
	GroupName string   `json:"group_name" ftV:"group_name"`
}
