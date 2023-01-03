package gkafka

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

var (
	topic = "crash-decode-dev"
	conf  = &Config{Addr: []string{"192.168.1.53:9092"}, GroupName: "20201102-0021"}
)

func TestMustInit(t *testing.T) {
	MustInit(conf, AllType)
	ctx := context.Background()
	go func() {
		for {
			Producer(ctx, topic, "ceshi"+time.Now().Format(time.RFC3339))
			time.Sleep(time.Second * 5)
		}
	}()
	go func() {
		msgChan, err := Consumer(ctx, []string{topic})
		if err != nil {
			panic(err)
		}
		for v := range msgChan {
			//fmt.Printf("%+v \n",v)
			fmt.Println(string(v.Value))
		}
	}()
	select {}
}

func TestConsumer(t *testing.T) {
	ctx := context.Background()
	MustInit(conf, ConsumerType)
	msgChan, err := Consumer(ctx, []string{topic})
	if err != nil {
		panic(err)
	}
	for v := range msgChan {
		//fmt.Printf("%+v \n",v)
		fmt.Println(string(v.Value))
	}
}

func TestConsumerFun(t *testing.T) {
	ctx := context.Background()
	MustInit(conf, ConsumerType)
	ConsumerFun(ctx, []string{topic}, message)
	select {}
}

func message(data *DataMessage) bool {
	str := string(data.Value)

	if strings.Index(str, ":50") > 0 {
		fmt.Println(str, true)
		return true
	}
	fmt.Println(str, false)
	return false
}

func TestProducer(t *testing.T) {
	ctx := context.Background()
	MustInit(conf, ProducerType)
	for i := 0; i < 5; i++ {
		Producer(ctx, topic, "ceshi"+time.Now().Format(time.RFC3339))
		time.Sleep(time.Second * 2)
	}
}
