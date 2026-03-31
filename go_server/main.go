package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

type KVRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func invalidKey(key string) bool {
	return key == "" || strings.ContainsAny(key, " \t\n\r")
}

func kvHandler(w http.ResponseWriter, r *http.Request) {
	var cmd string
	switch r.Method {
	case http.MethodPost:
		var req KVRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid json", http.StatusBadRequest)
			return
		}
		if invalidKey(req.Key) {
			http.Error(w, "Key is invalid (empty or contains whitespace)", http.StatusBadRequest)
			return
		}
		cmd = fmt.Sprintf("PUT %s %s", req.Key, req.Value)
	case http.MethodGet:
		key := r.URL.Query().Get("key")
		if invalidKey(key) {
			http.Error(w, "Key query parameter is invalid (empty or contains whitespace)", http.StatusBadRequest)
			return
		}
		cmd = fmt.Sprintf("GET %s", key)
	case http.MethodDelete:
		key := r.URL.Query().Get("key")
		if invalidKey(key) {
			http.Error(w, "Key query parameter is invalid (empty or contains whitespace)", http.StatusBadRequest)
			return
		}
		cmd = fmt.Sprintf("DEL %s", key)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	resp, err := sendCommand(cmd)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp = strings.TrimSpace(resp)

	fmt.Fprintln(w, resp)
}

func main() {
	http.HandleFunc("/kv", kvHandler)
	port := 8080
	fmt.Printf("Go API server is running on port %d\n", port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil); err != nil {
		fmt.Printf("Go API server exited with error: %v\n", err)
	}
}
