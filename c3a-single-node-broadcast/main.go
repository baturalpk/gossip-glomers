package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	valuesStoreInMemory = make([]int, 0, 5)
	muVal               sync.Mutex

	neighbors []string
	muTop     sync.Mutex
)

func registerHandlers(n *maelstrom.Node) {
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var reqBody struct {
			Message int `json:"message"`
		}
		if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
			return err
		}

		muVal.Lock()
		valuesStoreInMemory = append(valuesStoreInMemory, reqBody.Message)
		muVal.Unlock()

		return n.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		muVal.Lock()
		readValues := make([]int, len(valuesStoreInMemory))
		copy(readValues, valuesStoreInMemory)
		muVal.Unlock()

		return n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": readValues,
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var reqBody struct {
			Topology map[string][]string `json:"topology"`
		}
		if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
			return err
		}

		self := n.ID()

		muTop.Lock()
		neighbors = reqBody.Topology[self]
		muTop.Unlock()

		return n.Reply(msg, map[string]any{
			"type": "topology_ok",
		})
	})
}

func main() {
	node := maelstrom.NewNode()

	registerHandlers(node)

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
