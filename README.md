# gkafka

### 初始化
* func MustInit(conf *Config, kafkaType kafkaType)
> conf: kafka的配置信息

> AllType: 初始化生产者和消费者

> ProducerType: 初始化生产者

> ConsumerType: 初始化消费者
### 生产者
* func Producer(ctx context.Context, topic string, msg interface{}) error
> 通过Topic发送消息，消息可以是字符串，也可以是Map对象或者结构体
### 消费者
* func Consumer(ctx context.Context, topic []string) (<-chan *DataMessage, error)
> 通过chan的方式获取消费的数据
* func ConsumerFun(ctx context.Context, topic []string, fun MsgFun)
> 通过方法的回调方式获取消费的数据