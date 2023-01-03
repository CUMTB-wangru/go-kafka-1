package gkafka

import (
	"github.com/Shopify/sarama"
	jsoniter "github.com/json-iterator/go"
)

type GProducer struct {
	p sarama.SyncProducer
}

func NewProducer(addrs []string) (*GProducer, error) {
	p, err := sarama.NewSyncProducer(addrs, nil)
	if err != nil {
		return nil, err
	}
	return &GProducer{p: p}, nil
}

func (gp *GProducer) Send(topic string, value []byte) (partition int32, offset int64, err error) {
	msg := &sarama.ProducerMessage{}
	msg.Topic = topic
	msg.Value = sarama.ByteEncoder(value)
	return gp.p.SendMessage(msg)
}

func (gp *GProducer) SendObject(topic string, value interface{}) (partition int32, offset int64, err error) {
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	byteValue, err := json.Marshal(&value)
	if err != nil {
		return
	}
	return gp.Send(topic, byteValue)
}

func (gp *GProducer) Close() error {
	return gp.p.Close()
}
