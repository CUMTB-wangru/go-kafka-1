package gkafka

import (
	"context"
	"errors"
	"fmt"
)

type kafkaType int

const (
	ProducerType kafkaType = 1 << iota
	ConsumerType

	AllType = ProducerType | ConsumerType
)

var (
	km *KafkaManager

	ErrProducerNoInit = errors.New("producer no init")
	ErrConsumerNoInit = errors.New("consumer no init")
)

type KafkaManager struct {
	kafkaType kafkaType
	cfg       *Config
	Producer  *GProducer
	Consumer  *GConsumer
}

func MustInit(conf *Config, kafkaType kafkaType) {
	km = &KafkaManager{
		kafkaType: kafkaType,
		cfg:       conf,
	}
	var err error
	//初始化生产者
	if kafkaType&ProducerType > 0 {
		if km.Producer, err = NewProducer(conf.Addr); err != nil {
			panic(errors.New(fmt.Sprintf("init producer error,%s", err.Error())))
		}
	}

	//初始化消费者
	if kafkaType&ConsumerType > 0 {
		if len(conf.GroupName) == 0 {
			panic(errors.New("mode: ConsumerType, groupName no setting"))
		}
		if km.Consumer, err = NewConsumer(conf.Addr, conf.GroupName); err != nil {
			panic(errors.New(fmt.Sprintf("init consumer error,%s", err.Error())))
		}
	}
}

//Deprecated
func SendMsg(ctx context.Context, topic string, msg interface{}) error {
	_, _, err := km.Producer.SendObject(topic, msg)
	return err
}

func Producer(ctx context.Context, topic string, msg interface{}) error {
	if km.kafkaType&ProducerType == 0 {
		return ErrProducerNoInit
	}
	_, _, err := km.Producer.SendObject(topic, msg)
	return err
}

func ProducerString(ctx context.Context, topic, msg string) error {
	if km.kafkaType&ProducerType == 0 {
		return ErrProducerNoInit
	}
	_, _, err := km.Producer.Send(topic, []byte(msg))
	return err
}

func Consumer(ctx context.Context, topic []string) (<-chan *DataMessage, error) {
	if km.kafkaType&ConsumerType == 0 {
		return nil, ErrConsumerNoInit
	}
	return km.Consumer.newConsumerGroup(ctx, topic)
}

func ConsumerFun(ctx context.Context, topic []string, fun MsgFun) error {
	if km.kafkaType&ConsumerType == 0 {
		return ErrConsumerNoInit
	}
	return km.Consumer.newConsumerGroupFun(ctx, topic, fun)
}
