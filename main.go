package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strings"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	graphite "github.com/cyberdelia/go-metrics-graphite"
	"github.com/rcrowley/go-metrics"

	"github.com/spf13/viper"
	"github.com/wvanbergen/kafka/consumergroup"
	kazoo "github.com/wvanbergen/kazoo-go"
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
	viper.SetDefault("producer.flush.fequency.seconds", 1*time.Second)
	viper.SetDefault("producer.flush.bytes", 5388608)
	viper.SetDefault("graphite.interval", 30*time.Second)
	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		panic(fmt.Errorf("fatal error config file: %s \n", err))
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
	prodKafkaVersion, err := sarama.ParseKafkaVersion(viper.GetString("producer.kafka.version"))
	if err != nil {
		log.Println("Warning: Could not parse producer.kafka.version string, fallback to oldest stable version")
	}
	consKafkaVersion, err := sarama.ParseKafkaVersion(viper.GetString("consumer.kafka.version"))
	if err != nil {
		log.Println("Warning: Could not parse consumer.kafka.version string, fallback to oldest stable version")
	}
	// initialize kafka producer
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = false
	cfg.Producer.Return.Errors = true
	cfg.ClientID = "mirrormaker"
	// Set the configured compression codec
	cfg.Producer.Compression = getCompressionCodec(viper.GetString("producer.compression"))
	cfg.Producer.Retry.Max = 10
	cfg.Version = prodKafkaVersion
	client, err := sarama.NewClient(viper.GetStringSlice("producer.kafka.nodes"), cfg)
	if err != nil {
		log.Fatal(err)
	}
	cfg.Producer.Flush.Frequency = viper.GetDuration("producer.flush.fequency")
	cfg.Producer.Flush.Bytes = viper.GetInt("producer.flush.bytes")
	partitioner := strings.ToLower(viper.GetString("producer.partitioner"))
	if partitioner == "keeppartition" || partitioner == "modulo" {
		cfg.Producer.Partitioner = sarama.NewManualPartitioner
	}
	producerTopic := viper.GetString("producer.kafka.topic")
	part, err := client.Partitions(producerTopic)
	if err != nil {
		log.Fatalf("could not get partitions for target topic: %s", err)
	}
	numPartitions := len(part)
	producer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		log.Fatalf("could not open kafka connection: %s", err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	// initialize zookeeper/kafka client configuration
	var zkNodes []string
	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetNewest
	config.Offsets.ResetOffsets = false //for development only
	config.Offsets.ProcessingTimeout = 10 * time.Second
	config.Offsets.CommitInterval = 10 * time.Second
	config.Version = consKafkaVersion
	zkNodes, config.Zookeeper.Chroot = kazoo.ParseConnectionString(viper.GetString("consumer.zookeeper.connect"))

	// connect to zookeeper/kafka
	consumer, err := consumergroup.JoinConsumerGroup(viper.GetString("consumer.group.id"), []string{viper.GetString("consumer.topic")}, zkNodes, config)
	if err != nil {
		log.Fatalln(err)
	}

	pfxRegistry := metrics.NewPrefixedRegistry(viper.GetString("consumer.group.id") + ".")
	metrics.NewRegisteredMeter(`messages.processed`,
		pfxRegistry)
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
		case <-c:
			// SIGINT/SIGTERM
			break runloop
		case e := <-consumer.Errors():
			log.Println(e)
			metrics.GetOrRegisterMeter(`consumer.errors`, pfxRegistry).Mark(1)
		case er := <-producer.Errors():
			log.Println(er)
			metrics.GetOrRegisterMeter(`producer.errors`, pfxRegistry).Mark(1)
		case message := <-consumer.Messages():
			msg, err := PartitionMsg(partitioner, producerTopic, message, int32(numPartitions))
			if err != nil {
				log.Println(err)
				break runloop
			}
			producer.Input() <- &msg
			metrics.GetOrRegisterMeter(`messages.processed`, pfxRegistry).Mark(1)
			consumer.CommitUpto(message)
		}
	}
	c1 := make(chan string, 1)
	go func() {
		if err = consumer.Close(); err != nil {
			log.Println("Error closing the consumer", err)
		}
		c1 <- "consumer"
	}()
	go func() {
		if err = producer.Close(); err != nil {
			log.Println("Error closing the producer", err)
		}
		client.Close()
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
		return sarama.ProducerMessage{}, fmt.Errorf("configuration error, partitioner or topic was not set.")
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
			return sarama.ProducerMessage{}, fmt.Errorf("the dest topic has less partitions than the source, this is an invalid configuration and not compatible with keep partition.")
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
