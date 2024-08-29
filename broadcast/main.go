package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var messages = make(map[int]bool)

	n.Handle("broadcast", func(msg maelstrom.Message) error {

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		message, ok := body["message"].(float64)
		if !ok {
			return fmt.Errorf("invalid msg format")
		}
		messages[int(message)] = true

		ans := map[string]string{
			"type": "broadcast_ok",
		}

		return n.Reply(msg, ans)
	})

	n.Handle("read", func(msg maelstrom.Message) error {

		messageList := make([]int, 0, len(messages))
		for k := range messages {
			messageList = append(messageList, k)
		}

		ans := map[string]any{
			"type":     "read_ok",
			"messages": messageList,
		}

		return n.Reply(msg, ans)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		return n.Reply(msg, map[string]any{
			"type": "topology_ok",
		})
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
