package main

import (
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/gorilla/websocket"
)

// SubscribeRequest defines the JSON structure for a subscription.
type SubscribeRequest struct {
	Method string `json:"method"`
	Params struct {
		Channel  string   `json:"channel"`
		Symbol   []string `json:"symbol"`
		Depth    int      `json:"depth"`
		Snapshot bool     `json:"snapshot"`
	} `json:"params"`
}

func main() {
	// Kraken V2 WebSocket endpoint.
	wsURL := "wss://ws.kraken.com/v2"

	// Open a WebSocket connection.
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		log.Fatalf("Error connecting to Kraken WebSocket: %v", err)
	}
	defer conn.Close()

	// Open (or create) a file for appending JSON lines.
	file, err := os.OpenFile("orderbook.jsonl", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer file.Close()

	// Create a buffered channel to store update messages.
	updatesChan := make(chan []byte, 100)

	// Start a goroutine to write messages from the channel to the file.
	go func() {
		for msg := range updatesChan {
			// Write the raw JSON message followed by a newline.
			if _, err := file.Write(append(msg, '\n')); err != nil {
				log.Printf("Error writing to file: %v", err)
			}
		}
	}()

	// Build the subscription message for TRX/USD with a depth of 10.
	subReq := SubscribeRequest{Method: "subscribe"}
	subReq.Params.Channel = "book"
	subReq.Params.Symbol = []string{"TRX/USD"}
	subReq.Params.Depth = 10
	subReq.Params.Snapshot = true

	msg, err := json.Marshal(subReq)
	if err != nil {
		log.Fatalf("Error marshalling subscription request: %v", err)
	}

	// Send the subscription message.
	if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
		log.Fatalf("Error sending subscription request: %v", err)
	}
	log.Printf("Subscription sent: %s", msg)

	// Optionally, set a read deadline.
	conn.SetReadDeadline(time.Now().Add(60 * 60 * time.Second))

	// Read messages from the WebSocket.
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Fatalf("Error reading message: %v", err)
		}
		log.Printf("Received message: %s", message)

		// Send the message to the buffered channel.
		updatesChan <- message
	}
}
