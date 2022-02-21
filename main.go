package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"syscall"
	"time"

	"crypto/tls"
	"crypto/x509"

	"github.com/Shopify/sarama"
	graphite "github.com/cyberdelia/go-metrics-graphite"
	"github.com/rcrowley/go-metrics"

	"github.com/spf13/viper"
)

var (
	configFolder = flag.String("config", "/etc/mirrormaker", "path to the config directory")
	versionFlag  = flag.Bool("version", false, "print the version of the program")
)
var githash, shorthash, builddate, buildtime string
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

func main() {
	flag.Parse()
	// only provide version information if --version was specified
	if *versionFlag {
		fmt.Printf("runtime: %s\nversion: %s-%s\nbuilt: %s \ncommit: %s\n", runtime.Version(), builddate, shorthash, buildtime, githash)
		os.Exit(0)
	}
	viper.SetConfigName("config")      // name of config file (without extension)
	viper.AddConfigPath(*configFolder) // path to look for the config file in
	viper.AddConfigPath(".")           // optionally look for config in the working directory
	viper.SetDefault("producer.flush.fequency", 1*time.Second)
	viper.SetDefault("producer.flush.bytes", 5388608)
	viper.SetDefault("graphite.interval", 30*time.Second)
	viper.SetDefault("producer.kafka.tls", false)
	viper.SetDefault("producer.kafka.tls-skip-verify", false)
	viper.SetDefault("producer.kafka.tls-cafile", "")
	viper.SetDefault("producer.kafka.tls-certfile", "")
	viper.SetDefault("producer.kafka.tls-keyfile", "")
	viper.SetDefault("producer.kafka.username", "")
	viper.SetDefault("producer.kafka.password", "")

	viper.SetDefault("consumer.kafka.tls", false)
	viper.SetDefault("consumer.kafka.tls-skip-verify", false)
	viper.SetDefault("consumer.kafka.tls-cafile", "")
	viper.SetDefault("consumer.kafka.tls-certfile", "")
	viper.SetDefault("consumer.kafka.tls-keyfile", "")
	viper.SetDefault("consumer.kafka.username", "")
	viper.SetDefault("consumer.kafka.password", "")

	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close()
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}

	// Setup Producer
	producerCFG := setupProducer()
	producerClient, err := sarama.NewClient(viper.GetStringSlice("producer.kafka.nodes"), producerCFG)
	if err != nil {
		log.Fatal(err)
	}

	partitioner := strings.ToLower(viper.GetString("producer.partitioner"))
	if partitioner == "keeppartition" || partitioner == "modulo" {
		producerCFG.Producer.Partitioner = sarama.NewManualPartitioner
	}
	producerTopic := viper.GetString("producer.kafka.topic")
	part, err := producerClient.Partitions(producerTopic)
	if err != nil {
		log.Fatalf("could not get partitions for target topic: %s", err)
	}
	numPartitions := len(part)
	log.Printf("number partitions: %d", numPartitions)
	// connect to producer kafka
	producer, err := sarama.NewAsyncProducerFromClient(producerClient)
	if err != nil {
		log.Fatalf("could not open kafka connection: %s", err)
	}
	// END Setup Producer

	// Setup Consumer
	consumerCFG := setupConsumer()
	consumerClient, err := sarama.NewClient(viper.GetStringSlice("consumer.kafka.nodes"), consumerCFG)
	if err != nil {
		log.Fatal(err)
	}

	signalchannel := make(chan os.Signal, 1)
	signal.Notify(signalchannel, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	// connect to consuming kafka
	ctx, cancel := context.WithCancel(context.Background())
	consumerGroup, err := sarama.NewConsumerGroupFromClient(viper.GetString("consumer.group.id"), consumerClient)
	if err != nil {
		log.Fatalf("could not start consumer group from client: %s", err)
	}
	pfxRegistry := metrics.NewPrefixedRegistry(viper.GetString("consumer.group.id") + ".")
	consumer := Consumer{
		ready:         make(chan bool),
		producer:      producer,
		numPartitions: int32(numPartitions),
		producerTopic: producerTopic,
		partitioner:   partitioner,
		metrics:       pfxRegistry,
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := consumerGroup.Consume(ctx, strings.Split(viper.GetString("consumer.kafka.topic"), ","), &consumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()
	<-consumer.ready

	metrics.NewRegisteredMeter(`messages.processed`, pfxRegistry)
	if viper.GetString("graphite.address") != "" {
		log.Println(`Launched metrics producer socket`)
		addr, err := net.ResolveTCPAddr("tcp", viper.GetString("graphite.address"))
		if err != nil {
			log.Fatalln(err)
		}
		go graphite.Graphite(pfxRegistry, viper.GetDuration("graphite.interval"), viper.GetString("graphite.prefix"), addr)
	}
	log.Println("Connection to Zookeeper and Kafka established.")
	log.Printf("Using partitioner %s\n", partitioner)

runloop:
	for {
		select {
		case <-signalchannel:
			break runloop
		case <-ctx.Done():
			break runloop
		case e := <-consumerGroup.Errors():
			log.Println(e)
			metrics.GetOrRegisterMeter(`consumer.errors`, pfxRegistry).Mark(1)
		case e := <-producer.Errors():
			log.Println(e)
			metrics.GetOrRegisterMeter(`producer.errors`, pfxRegistry).Mark(1)
		}
	}
	c1 := make(chan string, 1)
	go func() {
		if err = consumerGroup.Close(); err != nil {
			log.Println("Error closing the consumer", err)
		}
		if err = consumerClient.Close(); err != nil {
			log.Println("Error closing the consumer", err)
		}
		cancel()
		wg.Wait()
		c1 <- "consumer"
	}()
	go func() {
		if err = producer.Close(); err != nil {
			log.Println("Error closing the producer", err)
		}
		if err = producerClient.Close(); err != nil {
			log.Println("Error closing the producer client", err)
		}
		c1 <- "producer"
	}()
	var closecnt int
	for {
		select {
		case res := <-c1:
			fmt.Printf("Successfully closed %s\n", res)
			closecnt++
			if closecnt == 2 {
				os.Exit(0)
			}
		case <-time.After(5 * time.Minute):
			fmt.Println("could not stop consumer or producer within the defined timeout of 5 minutes")
			os.Exit(1)
		}
	}
}

func getCompressionCodec(comp string) sarama.CompressionCodec {
	switch comp {
	case "snappy":
		return sarama.CompressionSnappy
	case "gzip":
		return sarama.CompressionGZIP
	case "lz4":
		return sarama.CompressionLZ4
	default:
		return sarama.CompressionNone
	}
}

func PartitionMsg(partitioner, topic string, origmsg *sarama.ConsumerMessage, numPartitions int32) (sarama.ProducerMessage, error) {
	if partitioner == "" || topic == "" {
		return sarama.ProducerMessage{}, fmt.Errorf("configuration error, partitioner or topic was not set")
	}
	if len(origmsg.Value) == 0 {
		return sarama.ProducerMessage{}, fmt.Errorf("value is not set")
	}
	if origmsg.Partition < 0 {
		return sarama.ProducerMessage{}, fmt.Errorf("the source message has a negative value for its partition")
	}
	switch partitioner {
	case "hash":
		//by default sarama is using a hash partitioner
		if len(origmsg.Key) == 0 {
			return sarama.ProducerMessage{}, fmt.Errorf("key is not set, we can't use the hash function for this type of messages")
		}
		return sarama.ProducerMessage{Topic: topic, Key: sarama.ByteEncoder(origmsg.Key), Value: sarama.ByteEncoder(origmsg.Value)}, nil
	case "keeppartition":
		//we set the target partition is set to the source partition
		if origmsg.Partition > numPartitions-1 {
			return sarama.ProducerMessage{}, fmt.Errorf("the dest topic has less partitions than the source, this is an invalid configuration and not compatible with keep partition")
		}
		return sarama.ProducerMessage{Topic: topic, Partition: origmsg.Partition, Key: sarama.ByteEncoder(origmsg.Key), Value: sarama.ByteEncoder(origmsg.Value)}, nil
	case "modulo":
		//we will calculate a new target partition using the modulo function.
		targetPartition := origmsg.Partition % numPartitions
		if targetPartition > numPartitions-1 {
			return sarama.ProducerMessage{}, fmt.Errorf("the target partition does not exist on the destination topic")
		}
		return sarama.ProducerMessage{Topic: topic, Partition: targetPartition, Key: sarama.ByteEncoder(origmsg.Key), Value: sarama.ByteEncoder(origmsg.Value)}, nil
	case "random":
		return sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(origmsg.Value)}, nil
	default:
		return sarama.ProducerMessage{}, fmt.Errorf("invalid partitioner defined")
	}
}

func tlsConfig(TLSCertFile, TLSKeyFile, CAFile string, TLSSkipVerify bool) *tls.Config {
	var t *tls.Config

	caCertPool, err := x509.SystemCertPool()
	if err != nil {
		log.Fatal("could not load system cert pool: ", err)
	}
	t = &tls.Config{
		RootCAs:            caCertPool,
		InsecureSkipVerify: TLSSkipVerify,
	}
	if TLSCertFile != "" || TLSKeyFile != "" || CAFile != "" {
		cert, err := tls.LoadX509KeyPair(TLSCertFile, TLSKeyFile)
		if err != nil {
			log.Fatal("kafka TLS load X509 key pair error: ", err)
		}

		caCert, err := ioutil.ReadFile(CAFile)
		if err != nil {
			log.Fatal("kafka TLS CA file error: ", err)
		}

		caCertPool.AppendCertsFromPEM(caCert)

		t.Certificates = []tls.Certificate{cert}
	}

	return t
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready         chan bool
	producer      sarama.AsyncProducer
	numPartitions int32
	producerTopic string
	partitioner   string
	metrics       metrics.Registry
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		msg, err := PartitionMsg(consumer.partitioner, consumer.producerTopic, message, consumer.numPartitions)
		if err != nil {
			log.Println(err)
			return err
		}
		consumer.producer.Input() <- &msg
		metrics.GetOrRegisterMeter(`messages.processed`, consumer.metrics).Mark(1)

		// log.Printf("Message claimed: timestamp = %v, partition = %d, topic = %s, value = %s", message.Timestamp, message.Partition, message.Topic, string(message.Value))
		session.MarkMessage(message, "")
	}
	return nil
}

func setupProducer() *sarama.Config {
	// initialize kafka producer
	producerKafkaVersion, err := sarama.ParseKafkaVersion(viper.GetString("producer.kafka.version"))
	if err != nil {
		log.Printf("could not parse producer kafka version fallback to default version: %s", producerKafkaVersion.String())
	}
	producerCFG := sarama.NewConfig()
	producerCFG.Version = producerKafkaVersion
	producerCFG.ClientID = "mirrormaker"
	producerCFG.Producer.Return.Successes = false
	producerCFG.Producer.Return.Errors = true
	producerCFG.Producer.Compression = getCompressionCodec(viper.GetString("producer.compression"))
	producerCFG.Producer.Retry.Max = 10
	producerCFG.Producer.Flush.Frequency = viper.GetDuration("producer.flush.fequency")
	producerCFG.Producer.Flush.Bytes = viper.GetInt("producer.flush.bytes")
	if viper.GetBool("producer.kafka.tls") {
		producerCFG.Net.TLS.Enable = true
		producerCFG.Net.TLS.Config = tlsConfig(
			viper.GetString("producer.kafka.tls-certfile"),
			viper.GetString("producer.kafka.tls-keyfile"),
			viper.GetString("producer.kafka.tls-cafile"),
			viper.GetBool("producer.kafka.tls-skip-verify"))

		log.Println("Info: enabled kafka tls")
	}
	if viper.GetString("producer.kafka.username") != "" && viper.GetString("producer.kafka.password") != "" {
		producerCFG.Net.SASL.Enable = true
		producerCFG.Net.SASL.User = viper.GetString("producer.kafka.username")
		producerCFG.Net.SASL.Password = viper.GetString("producer.kafka.password")
		producerCFG.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		log.Println("Info: setup kafka sasl")
	}
	return producerCFG
}

func setupConsumer() *sarama.Config {
	// initialize kafka producer
	consumerKafkaVersion, err := sarama.ParseKafkaVersion(viper.GetString("consumer.kafka.version"))
	if err != nil {
		log.Printf("could not parse consumer kafka version fallback to default version: %s", consumerKafkaVersion.String())
	}
	consumerCFG := sarama.NewConfig()
	consumerCFG.Consumer.Offsets.Initial = sarama.OffsetNewest
	// producerCFG.Consumer.Offsets.ResetOffsets = false
	consumerCFG.Consumer.Offsets.CommitInterval = 10 * time.Second
	consumerCFG.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange

	consumerCFG.Consumer.Return.Errors = true // allows to use ConsumerGroup.Errors()
	consumerCFG.Version = consumerKafkaVersion

	consumerCFG.ClientID = "mirrormaker"
	if viper.GetBool("consumer.kafka.tls") {
		consumerCFG.Net.TLS.Enable = true
		consumerCFG.Net.TLS.Config = tlsConfig(
			viper.GetString("consumer.kafka.tls-certfile"),
			viper.GetString("consumer.kafka.tls-keyfile"),
			viper.GetString("consumer.kafka.tls-cafile"),
			viper.GetBool("consumer.kafka.tls-skip-verify"))

		log.Println("Info: enabled kafka tls")
	}
	if viper.GetString("consumer.kafka.username") != "" && viper.GetString("consumer.kafka.password") != "" {
		consumerCFG.Net.SASL.Enable = true
		consumerCFG.Net.SASL.User = viper.GetString("consumer.kafka.username")
		consumerCFG.Net.SASL.Password = viper.GetString("consumer.kafka.password")
		consumerCFG.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		log.Println("Info: setup kafka sasl")
	}
	return consumerCFG
}
