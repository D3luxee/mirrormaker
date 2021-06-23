package main

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
)

var goodmsgs []sarama.ConsumerMessage = []sarama.ConsumerMessage{{
	Partition: 0,
	Value:     []byte("Terrible Test"),
	Key:       []byte("Terrible Test"),
},
	{
		Partition: 0,
		Value:     []byte("Terrible Test"),
		Key:       []byte("Terrible Test"),
	}, {
		Partition: 1,
		Value:     []byte("Terrible Test"),
		Key:       []byte("Terrible Test"),
	}, {
		Partition: 2,
		Value:     []byte("Terrible Test"),
		Key:       []byte("Terrible Test"),
	}, {
		Partition: 3,
		Value:     []byte("Terrible Test"),
		Key:       []byte("Terrible Test"),
	}, {
		Partition: 4,
		Value:     []byte("Terrible Test"),
		Key:       []byte("Terrible Test"),
	}, {
		Partition: 5,
		Value:     []byte("Terrible Test"),
		Key:       []byte("Terrible Test"),
	}, {
		Partition: 6,
		Value:     []byte("Terrible Test"),
		Key:       []byte("Terrible Test"),
	}, {
		Partition: 7,
		Value:     []byte("Terrible Test"),
		Key:       []byte("Terrible Test"),
	}, {
		Partition: 8,
		Value:     []byte("Terrible Test"),
		Key:       []byte("Terrible Test"),
	}, {
		Partition: 9,
		Value:     []byte("Terrible Test"),
		Key:       []byte("Terrible Test"),
	}, {
		Partition: 10,
		Value:     []byte("Terrible Test"),
		Key:       []byte("Terrible Test"),
	}, {
		Partition: 11,
		Value:     []byte("Terrible Test"),
		Key:       []byte("Terrible Test"),
	}, {
		Partition: 12,
		Value:     []byte("Terrible Test"),
		Key:       []byte("Terrible Test"),
	}, {
		Partition: 13,
		Value:     []byte("Terrible Test"),
		Key:       []byte("Terrible Test"),
	}, {
		Partition: 14,
		Value:     []byte("Terrible Test"),
		Key:       []byte("Terrible Test"),
	}, {
		Partition: 15,
		Value:     []byte("Terrible Test"),
		Key:       []byte("Terrible Test"),
	}, {
		Partition: 16,
		Value:     []byte("Terrible Test"),
		Key:       []byte("Terrible Test"),
	}, {
		Partition: 17,
		Value:     []byte("Terrible Test"),
		Key:       []byte("Terrible Test"),
	}}

func TestPartitionMsgModulo(t *testing.T) {
	var numPartitionsLess int32 = 8
	var numPartitionsMore int32 = 32
	for _, b := range goodmsgs {
		//check good messages and the expected outcome
		c, err := PartitionMsg("modulo", "empty", &b, numPartitionsLess)
		assert.NoError(t, err, "Unexpected error %v", err)
		assert.LessOrEqual(t, c.Partition, numPartitionsLess-1, "The outgoing partition is not available, source partition: %d target partition: %d we only have partition 0 to %d", b.Partition, c.Partition, numPartitionsLess-1)
		assert.NotEmpty(t, c.Key, "Key is empty after partitioning")
		assert.NotEmpty(t, c.Value, "Value is emptry afer partitioning")
		c, err = PartitionMsg("modulo", "empty", &b, numPartitionsMore)
		assert.NoError(t, err, "Unexpected error %v", err)
		assert.LessOrEqual(t, c.Partition, numPartitionsMore-1, "The outgoing partition is not available, source partition: %d target partition: %d we only have partition 0 to %d", b.Partition, c.Partition, numPartitionsMore-1)
	}
	//check bad messages and error handling
	msg := sarama.ConsumerMessage{
		Partition: 8,
	}
	_, err := PartitionMsg("modulo", "", &msg, numPartitionsLess)
	assert.Error(t, err, "No error occured on unset topic")
	_, err = PartitionMsg("", "empty", &msg, numPartitionsLess)
	assert.Error(t, err, "No error occured on unset partitioning type")
	_, err = PartitionMsg("modulo", "empty", &msg, numPartitionsLess)
	assert.Error(t, err, "No error occured on a message without value")
	msg = sarama.ConsumerMessage{
		Partition: -8,
		Value:     []byte("Terrible Test"),
	}
	_, err = PartitionMsg("modulo", "empty", &msg, numPartitionsLess)
	assert.Error(t, err, "No error occured on a negative source partition")
}

func TestPartitionMsgHash(t *testing.T) {
	var numPartitionsLess int32 = 8
	for _, b := range goodmsgs {
		//check good messages and the expected outcome
		c, err := PartitionMsg("hash", "empty", &b, numPartitionsLess)
		assert.NoError(t, err, "Unexpected error %v", err)
		assert.NotEmpty(t, c.Key, "Key is empty after partitioning")
		assert.NotEmpty(t, c.Value, "Value is emptry afer partitioning")
	}
	//check bad messages and error handling
	msg := sarama.ConsumerMessage{
		Partition: 8,
	}
	_, err := PartitionMsg("hash", "", &msg, numPartitionsLess)
	assert.Error(t, err, "No error occured on unset topic")
	_, err = PartitionMsg("", "empty", &msg, numPartitionsLess)
	assert.Error(t, err, "No error occured on unset partitioning type")
	_, err = PartitionMsg("hash", "empty", &msg, numPartitionsLess)
	assert.Error(t, err, "No error occured on a message without value")
	msg = sarama.ConsumerMessage{
		Partition: -8,
		Value:     []byte("Terrible Test"),
	}
	_, err = PartitionMsg("hash", "empty", &msg, numPartitionsLess)
	assert.Error(t, err, "No error occured on a negative source partition")
}

func TestPartitionMsgKeepPartition(t *testing.T) {
	var numPartitionsSame int32 = 18
	for _, b := range goodmsgs {
		//check good messages and the expected outcome
		c, err := PartitionMsg("keeppartition", "empty", &b, numPartitionsSame)
		assert.Equal(t, b.Partition, c.Partition, "The source partition %d does not equal to the destination partition %d", b.Partition, c.Partition)
		assert.NoError(t, err, "Unexpected error %v", err)
		assert.NotEmpty(t, c.Key, "Key is empty after partitioning")
		assert.NotEmpty(t, c.Value, "Value is emptry afer partitioning")
	}
	//check bad messages and error handling
	msg := sarama.ConsumerMessage{
		Partition: 8,
	}
	_, err := PartitionMsg("keeppartition", "", &msg, numPartitionsSame)
	assert.Error(t, err, "No error occured on unset topic")
	_, err = PartitionMsg("", "empty", &msg, numPartitionsSame)
	assert.Error(t, err, "No error occured on unset partitioning type")
	_, err = PartitionMsg("keeppartition", "empty", &msg, numPartitionsSame)
	assert.Error(t, err, "No error occured on a message without value")
	msg = sarama.ConsumerMessage{
		Partition: -8,
		Value:     []byte("Terrible Test"),
	}
	_, err = PartitionMsg("keeppartition", "empty", &msg, numPartitionsSame)
	assert.Error(t, err, "No error occured on a negative source partition")
	//source topic got more partitions then the destination must fail
	msg = sarama.ConsumerMessage{
		Partition: 30,
		Value:     []byte("Terrible Test"),
	}
	_, err = PartitionMsg("keeppartition", "empty", &msg, numPartitionsSame)
	assert.Error(t, err, "No error occured if the source partition does not exist on the target topic")
}
