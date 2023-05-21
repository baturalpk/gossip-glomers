package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func genRandomHex(size int) (string, error) {
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func main() {
	node := maelstrom.NewNode()

	node.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Generate unique ID
		unixEpochHex := strconv.FormatInt(time.Now().UnixNano(), 16)
		nodeID := node.ID()
		randHex, err := genRandomHex(6)
		if err != nil {
			return err
		}
		uid := fmt.Sprintf("%s-%s%s", unixEpochHex, randHex, nodeID)

		body["type"] = "generate_ok"
		body["id"] = uid
		return node.Reply(msg, body)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
