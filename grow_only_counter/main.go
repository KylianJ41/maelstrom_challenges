package main

import (
	"context"
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	counterKey := "counter"

	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		delta := int(body["delta"].(float64))

		for {
			ctx := context.Background()
			value, err := kv.ReadInt(ctx, counterKey)
			if err != nil {
				value = 0
			}

			newValue := value + delta
			err = kv.CompareAndSwap(ctx, counterKey, value, newValue, true)
			if err == nil {
				break
			}
		}

		return n.Reply(msg, map[string]string{"type": "add_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		ctx := context.Background()
		value, err := kv.ReadInt(ctx, counterKey)
		if err != nil {
			value = 0
		}

		return n.Reply(msg, map[string]any{
			"type":  "read_ok",
			"value": value,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
