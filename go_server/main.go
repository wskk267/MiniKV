package main

import (
	"fmt"
	"net/http"
	"encoding/json"
	"strings"
)

type KVRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
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
		if req.Key == "" {
			http.Error(w, "Key is required", http.StatusBadRequest)
			return
		}
		cmd = fmt.Sprintf("PUT %s %s", req.Key, req.Value)
	case http.MethodGet:
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "Key query parameter is required", http.StatusBadRequest)
			return
		}
		cmd = fmt.Sprintf("GET %s", key)
	case http.MethodDelete:
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "Key query parameter is required", http.StatusBadRequest)
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
	resp=strings.TrimSpace(resp)
	if resp == "NOT_FOUND" {
		http.Error(w, resp, http.StatusNotFound)
		return
	}

	fmt.Fprintln(w, resp)
}

func main() {
	http.HandleFunc("/kv", kvHandler)
	port := 8080
	fmt.Printf("Go API server is running on port %d\n", port)
	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)	
}
