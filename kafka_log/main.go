package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type KafkaNode struct {
	node *maelstrom.Node
	kv   *maelstrom.KV
	mu   sync.RWMutex
}

func NewKafkaNode() *KafkaNode {
	node := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(node)
	return &KafkaNode{
		node: node,
		kv:   kv,
	}
}

func (kn *KafkaNode) handleSend(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	key := body["key"].(string)
	message := body["msg"]

	offsetKey := fmt.Sprintf("%s_offset", key)
	logKey := fmt.Sprintf("%s_log", key)

	kn.mu.Lock()
	defer kn.mu.Unlock()

	ctx := context.TODO()

	var offset int
	err := kn.kv.ReadInto(ctx, offsetKey, &offset)
	if err != nil {
		// Key doesn't exist, init with 0
		if err := kn.kv.Write(ctx, offsetKey, 0); err != nil {
			return err
		}
		offset = 0
	}

	// Read existing log
	var log map[int]interface{}
	err = kn.kv.ReadInto(ctx, logKey, &log)
	if err != nil {
		// Log doesn't exist, create a new one
		log = make(map[int]interface{})
	}

	// append message to log
	log[offset] = message
	if err := kn.kv.Write(ctx, logKey, log); err != nil {
		return err
	}

	// increment offset
	if err := kn.kv.Write(ctx, offsetKey, offset+1); err != nil {
		return err
	}

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

	result := make(map[string][][2]interface{})

	for key, offsetInterface := range offsets {
		requestedOffset := int(offsetInterface.(float64))

		offsetKey := fmt.Sprintf("%s_offset", key)
		logKey := fmt.Sprintf("%s_log", key)

		ctx := context.TODO()

		var currentOffset int
		err := kn.kv.ReadInto(ctx, offsetKey, &currentOffset)
		if err != nil {
			// If we can't read the offset, skip this key
			continue
		}

		var log map[int]interface{}
		err = kn.kv.ReadInto(ctx, logKey, &log)
		if err != nil {
			// If we can't read the log, skip this key
			continue
		}

		messages := [][2]interface{}{}
		for i := requestedOffset; i < currentOffset; i++ {
			if msg, exists := log[i]; exists {
				messages = append(messages, [2]interface{}{i, msg})
			}
		}

		if len(messages) > 0 {
			result[key] = messages
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

	for key, offsetInterface := range offsets {
		newOffset := int(offsetInterface.(float64))
		commitKey := fmt.Sprintf("%s_commited", key)

		for {
			ctx := context.TODO()

			var currentOffset int
			err := kn.kv.ReadInto(ctx, commitKey, &currentOffset)
			if err != nil {
				// Key doesn't exist, try to create it
				err := kn.kv.Write(ctx, commitKey, newOffset)
				if err != nil {
					// If write failed, it might be due to concurrent creation
					// We'll retry the whole process
					continue
				}
				break
			}

			if newOffset <= currentOffset {
				// New offset is not greater than the current one, ignore
				break
			}

			err = kn.kv.CompareAndSwap(ctx, commitKey, currentOffset, newOffset, false)
			if err == nil {
				// Successfully updated
				break
			}

			// Check if it's a precondition failed error (i.e., concurrent modification)
			if err.Error() == "precondition failed" {
				// Retry the whole process
				continue
			}

			// Some other error occurred
			return err
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

	offsets := make(map[string]int)
	for _, keyInterface := range keys {
		key := keyInterface.(string)
		commitKey := fmt.Sprintf("%s_commited", key)

		ctx := context.TODO()

		var offset int
		err := kn.kv.ReadInto(ctx, commitKey, &offset)
		if err == nil {
			offsets[key] = offset
		}
		// If err != nil, it means the key doesn't exist or there was a read error.
		// In this case, we simply omit this key from the response, as per the original logic.
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
