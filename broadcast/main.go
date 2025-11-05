package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var messages []float64
	var neighbors []string

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		newMessage := body["message"].(float64)
		body["type"] = "broadcast_ok"

		messages = append(messages, newMessage)
		delete(body, "message")

		propMsgBody := make(map[string]any)
		propMsgBody["src"] = n.ID()
		propMsgBody["message"] = newMessage
		propMsgBody["type"] = "propagate"
		for _, nei := range neighbors {
			n.Send(nei, propMsgBody)
		}

		return n.Reply(msg, body)
	})

	n.Handle("propagate", func(msg maelstrom.Message) error {
		body, err := UnmarshalReqBody(msg)
		if err != nil {
			return err
		}

		sender := body["src"]
		newMessage := body["message"].(float64)
		messages = append(messages, newMessage)

		propMsg := make(map[string]any)
		propMsg["src"] = n.ID()
		propMsg["message"] = newMessage
		propMsg["type"] = "propagate"

		for _, nei := range neighbors {
			if nei != sender {
				n.Send(nei, propMsg)
			}
		}

		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		body, err := UnmarshalReqBody(msg)
		if err != nil {
			return err
		}

		body["type"] = "read_ok"
		body["messages"] = messages

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		type TopologyBody struct {
			Type     string              `json:"type"`
			Topology map[string][]string `json:"topology"`
			Msg_id   int                 `json:"msg_id`
		}
		var body TopologyBody
		err := json.Unmarshal(msg.Body, &body)
		if err != nil {
			return err
		}

		nei := body.Topology[n.ID()]
		neighbors = append(neighbors, nei...)

		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func UnmarshalReqBody(msg maelstrom.Message) (map[string]any, error) {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return nil, err
	}

	return body, nil
}
