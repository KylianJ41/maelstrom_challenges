package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type KafkaNode struct {
	node             *maelstrom.Node
	logs             map[string]map[int]interface{}
	offsets          map[string]int
	committedOffsets map[string]int
	mu               sync.RWMutex
}

func NewKafkaNode() *KafkaNode {
	return &KafkaNode{
		node:             maelstrom.NewNode(),
		logs:             make(map[string]map[int]interface{}),
		offsets:          make(map[string]int),
		committedOffsets: make(map[string]int),
	}
}

func (kn *KafkaNode) handleSend(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	key := body["key"].(string)
	message := body["msg"]

	kn.mu.Lock()
	defer kn.mu.Unlock()

	if _, exists := kn.logs[key]; !exists {
		kn.logs[key] = make(map[int]interface{})
		kn.offsets[key] = 0
	}

	offset := kn.offsets[key]
	kn.logs[key][offset] = message
	kn.offsets[key]++

	return kn.node.Reply(msg, map[string]any{
		"type":   "send_ok",
		"offset": offset,
	})
}

func (kn *KafkaNode) handlePoll(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	offsets := body["offsets"].(map[string]interface{})

	kn.mu.RLock()
	defer kn.mu.RUnlock()

	result := make(map[string][][2]interface{})

	for key, offsetInterface := range offsets {
		offset := int(offsetInterface.(float64))
		if log, exists := kn.logs[key]; exists {
			messages := [][2]interface{}{}
			for i := offset; i < kn.offsets[key]; i++ {
				if msg, exists := log[i]; exists {
					messages = append(messages, [2]interface{}{i, msg})
				}
			}
			if len(messages) > 0 {
				result[key] = messages
			}
		}
	}

	return kn.node.Reply(msg, map[string]any{
		"type": "poll_ok",
		"msgs": result,
	})
}

func (kn *KafkaNode) handleCommitOffsets(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	offsets := body["offsets"].(map[string]interface{})

	kn.mu.Lock()
	defer kn.mu.Unlock()

	for key, offsetInterface := range offsets {
		offset := int(offsetInterface.(float64))
		if currentOffset, exists := kn.committedOffsets[key]; !exists || offset > currentOffset {
			kn.committedOffsets[key] = offset
		}
	}

	return kn.node.Reply(msg, map[string]string{
		"type": "commit_offsets_ok",
	})
}

func (kn *KafkaNode) handleListCommittedOffsets(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	keys := body["keys"].([]interface{})

	kn.mu.RLock()
	defer kn.mu.RUnlock()

	offsets := make(map[string]int)
	for _, key := range keys {
		if offset, exists := kn.committedOffsets[key.(string)]; exists {
			offsets[key.(string)] = offset
		}
	}

	return kn.node.Reply(msg, map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": offsets,
	})
}

func main() {
	kn := NewKafkaNode()

	kn.node.Handle("send", kn.handleSend)
	kn.node.Handle("poll", kn.handlePoll)
	kn.node.Handle("commit_offsets", kn.handleCommitOffsets)
	kn.node.Handle("list_committed_offsets", kn.handleListCommittedOffsets)

	if err := kn.node.Run(); err != nil {
		log.Fatal(err)
	}
}
