package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"
)

type Producer struct {
	ip          string
	port        string
	producer    sarama.AsyncProducer
	sendmessage chan string
	close       chan bool
	topic       string
}

func NewProducer(ip string, port string, topic string) (*Producer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Version = sarama.V0_11_0_2

	producer, err := sarama.NewAsyncProducer([]string{ip + ":" + port}, config)
	if err != nil {
		fmt.Printf("producer_test create producer error :%s\n", err.Error())
		return nil, err
	}

	return &Producer{
		ip:          ip,
		port:        port,
		producer:    producer,
		sendmessage: make(chan string, 10),
		close:       make(chan bool, 10),
		topic:       topic,
	}, nil
}
func (producer *Producer) Start() {
	// send message
	msg := &sarama.ProducerMessage{
		Topic: producer.topic,
		Key:   sarama.StringEncoder("go_test"),
	}

	value := "this is message"

	defer producer.producer.AsyncClose()

	for {
		select {
		case value = <-producer.sendmessage:
		case <-producer.close:
			return
		}
		msg.Value = sarama.ByteEncoder(value)
		fmt.Printf("input [%s]\n", value)

		// send to chain
		producer.producer.Input() <- msg

		select {
		case suc := <-producer.producer.Successes():
			fmt.Printf("offset: %d,  timestamp: %s", suc.Offset, suc.Timestamp.String())
		case fail := <-producer.producer.Errors():
			fmt.Printf("err: %s\n", fail.Err.Error())
		}
	}

}
func (producer *Producer) Close() {
	producer.close <- true

}
func (producer *Producer) Send(msg string) {
	producer.sendmessage <- msg
}

/************************************************************************/

type Consumer struct {
	ip            string
	port          string
	consumer      sarama.Consumer
	partition     sarama.PartitionConsumer
	recivemessage chan string
	close         chan bool
	topic         string
}

func NewConsumer(ip string, port string, topic string) (*Consumer, error) {
	fmt.Printf("consumer_test")

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version = sarama.V0_11_0_2

	// consumer
	consumer, err := sarama.NewConsumer([]string{ip + ":" + port}, config)
	if err != nil {
		fmt.Printf("consumer_test create consumer error %s\n", err.Error())
		return nil, err
	}

	partition_consumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		fmt.Printf("try create partition_consumer error %s\n", err.Error())
		return nil, err
	}

	return &Consumer{
		ip:            ip,
		port:          port,
		consumer:      consumer,
		partition:     partition_consumer,
		recivemessage: make(chan string, 10),
		topic:         topic,
		close:         make(chan bool),
	}, nil
}

func (consumer *Consumer) StartRead() {
	defer consumer.consumer.Close()
	defer consumer.partition.Close()
	for {
		select {
		case msg := <-consumer.partition.Messages():
			fmt.Printf("msg offset: %d, partition: %d, timestamp: %s, value: %s\n",
				msg.Offset, msg.Partition, msg.Timestamp.String(), string(msg.Value))
			consumer.recivemessage <- string(msg.Value)
		case err := <-consumer.partition.Errors():
			fmt.Printf("err :%s\n", err.Error())
		case <-consumer.close:
			return
		}
	}
}
func (consumer *Consumer) Close() {
	consumer.close <- true
}

func main() {
	fmt.Println("hello world")

	c := true

	/*prodouce*/
	producer, err := NewProducer("47.99.136.146", "1936", "demo")
	if err != nil {
		fmt.Println(err)
		return
	}
	go producer.Start()

	go func() {
		value := "this is message"
		for c {
			fmt.Scanln(&value)
			producer.sendmessage <- value
		}
	}()

	defer producer.Close()

	/*consumer*/
	consumer, err := NewConsumer("47.99.136.146", "1936", "demo")
	if err != nil {
		fmt.Println(err)
		return
	}
	go consumer.StartRead()

	go func() {
		for c {
			select {
			case msg := <-consumer.recivemessage:
				fmt.Println("read msg", msg)
			}
		}
	}()
	defer consumer.Close()

	/****************************/
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGALRM)
	select {
	case <-sigs:
		fmt.Println(" close out ")
		c = false
	}
}
