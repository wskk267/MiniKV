package main

import (
	"fmt"
	"net/http"
)

func kvHandler(w http.ResponseWriter, r *http.Request) {
	resp, err := sendCommand("PING")
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	fmt.Fprintf(w, "Response from server: %s", resp)
}

func main() {
	http.HandleFunc("/kv", kvHandler)
	port := 8080
	fmt.Printf("Go API server is running on port %d\n", port)
	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)	
}
