package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	localSet := make(map[int]bool)

	var minNum, maxNum int
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Handle init message, to initialize min and max range for each node
	n.Handle("init", func(msg maelstrom.Message) error {
		switch n.ID() {
		case "n0":
			minNum = 0
			maxNum = 33_333
		case "n1":
			minNum = 33_334
			maxNum = 66_666
		case "n2":
			minNum = 66_667
			maxNum = 100_000
		}

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "init_ok"
		return n.Reply(msg, body)
	})

	// handle generate id request, each node covers certain range, thus no peer communication required
	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		newID := r.Intn(maxNum-minNum+1) + minNum
		for localSet[newID] {
			newID = r.Intn(maxNum-minNum+1) + minNum
		}
		localSet[newID] = true

		body["id"] = newID
		body["type"] = "generate_ok"
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
