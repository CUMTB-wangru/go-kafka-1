package gkafka

import (
	"context"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"gitlab.ftsview.com/fotoable-go/glog"
)

type (
	DataMessage struct {
		Key, Value []byte
		Topic      string
		Partition  int32
		Offset     int64
	}

	MsgFun func(*DataMessage) bool
)

var (
	waitSecond    = 1
	maxWaitSecond = 60
)

type GConsumer struct {
	client sarama.ConsumerGroup
}

func NewConsumer(addrs []string, groupName string) (*GConsumer, error) {
	kafkaConf := sarama.NewConfig()
	kafkaConf.Version = sarama.V2_2_1_0
	kafkaConf.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	kafkaConf.Consumer.Offsets.Initial = sarama.OffsetNewest

	client, err := sarama.NewConsumerGroup(addrs, groupName, kafkaConf)
	if err != nil {
		return nil, err
	}
	return &GConsumer{client: client}, nil
}

func (c *GConsumer) newConsumerGroup(ctx context.Context, topic []string) (<-chan *DataMessage, error) {

	msgChan := make(chan *DataMessage)
	consumer := &consumerGroup{
		msgChan: msgChan,
		ready:   make(chan struct{}),
	}
	go func(consumer *consumerGroup) {
		for {
			if err := c.client.Consume(ctx, topic, consumer); err != nil {
				glog.Errorf(ctx, "error from consumer: %s", err.Error())
				waitSecond = waitSecond << 1
				if waitSecond > maxWaitSecond {
					waitSecond = maxWaitSecond
				}
				//等待间隔后尝试重新连接
				time.Sleep(time.Second * time.Duration(waitSecond))
			}
		}
	}(consumer)
	<-consumer.ready
	//等待初始化完成
	return msgChan, nil
}

func (c *GConsumer) newConsumerGroupFun(ctx context.Context, topic []string, fun MsgFun) error {

	consumer := consumerGroup{
		fun:   fun,
		ready: make(chan struct{}),
	}
	go func() {
		for {
			if err := c.client.Consume(ctx, topic, &consumer); err != nil {
				glog.Errorf(ctx, "error from consumer: %s", err.Error())
				waitSecond = waitSecond << 1
				if waitSecond > maxWaitSecond {
					waitSecond = maxWaitSecond
				}
				//等待间隔后尝试重新连接
				time.Sleep(time.Second * time.Duration(waitSecond))
			}
		}
	}()
	<-consumer.ready
	//等待初始化完成
	return nil
}

type consumerGroup struct {
	once    sync.Once
	ready   chan struct{}
	msgChan chan *DataMessage
	fun     MsgFun
}

func (c *consumerGroup) Setup(sarama.ConsumerGroupSession) error {
	c.once.Do(func() {
		close(c.ready)
	})

	//重置等待时间
	waitSecond = 1
	return nil
}

func (c *consumerGroup) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *consumerGroup) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			dataMessage := &DataMessage{
				Key:       message.Key,
				Value:     message.Value,
				Topic:     message.Topic,
				Partition: message.Partition,
				Offset:    message.Offset,
			}
			if c.fun != nil {
				if c.fun(dataMessage) {
					session.MarkMessage(message, "")
				}
			} else {
				c.msgChan <- dataMessage
				session.MarkMessage(message, "")
			}
		case <-session.Context().Done():
			return nil
		}
	}
}
