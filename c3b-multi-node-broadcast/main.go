package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	valuesSetInMemory = map[int]struct{}{}
	muVal             sync.Mutex

	neighbors []string
	muTop     sync.Mutex
)

type MyNode struct {
	node *maelstrom.Node
}

func (my *MyNode) runBroadcastDispatcher() {
	muVal.Lock()
	readValuesCached := make([]int, len(valuesSetInMemory))
	for el := range valuesSetInMemory {
		readValuesCached = append(readValuesCached, el)
	}
	muVal.Unlock()

	for _, dest := range neighbors {
		for el := range readValuesCached {
			err := my.node.Send(dest, map[string]any{
				"type":    "broadcast",
				"message": el,
			})
			if err != nil {
				log.Printf("error: runBroadcastDispatcher: sendTo(%s): %s\n", dest, err.Error())
			}
		}
	}
}

func (my *MyNode) registerHandlers() {
	my.node.Handle("broadcast", func(msg maelstrom.Message) error {
		var reqBody struct {
			Message int `json:"message"`
		}
		if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
			return err
		}

		muVal.Lock()
		valuesSetInMemory[reqBody.Message] = struct{}{}
		muVal.Unlock()

		return my.node.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})
	})

	my.node.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		// ignore
		return nil
	})

	my.node.Handle("read", func(msg maelstrom.Message) error {
		muVal.Lock()
		readValues := make([]int, len(valuesSetInMemory))
		for el := range valuesSetInMemory {
			readValues = append(readValues, el)
		}
		muVal.Unlock()

		return my.node.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": readValues,
		})
	})

	my.node.Handle("topology", func(msg maelstrom.Message) error {
		var reqBody struct {
			Topology map[string][]string `json:"topology"`
		}
		if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
			return err
		}

		self := my.node.ID()

		muTop.Lock()
		neighbors = reqBody.Topology[self]
		muTop.Unlock()

		return my.node.Reply(msg, map[string]any{
			"type": "topology_ok",
		})
	})
}

func (my *MyNode) Run() error {
	return my.node.Run()
}

func main() {
	myNode := MyNode{
		node: maelstrom.NewNode(),
	}
	myNode.registerHandlers()

	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for range ticker.C {
			myNode.runBroadcastDispatcher()
		}
	}()

	if err := myNode.Run(); err != nil {
		ticker.Stop()
		log.Fatal(err)
	}
	ticker.Stop()
}
