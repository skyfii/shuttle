package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"strings"
)

// marshal whatever we've got with out default indentation
// swallowing errors.
func marshal(i interface{}) []byte {
	jsonBytes, err := json.MarshalIndent(i, "", "  ")
	if err != nil {
		log.Println("ERROR: Troble encoding json-", err)
	}
	return append(jsonBytes, '\n')
}

// random 64bit ID
func genId() string {
	b := make([]byte, 8)
	rand.Read(b)
	return fmt.Sprintf("%x", b)
}

// remove empty strings from a []string
func filterEmpty(a []string) []string {
	removed := 0
	for i := 0; i < len(a); i++ {
		if removed > 0 {
			a[i-removed] = a[i]
		}
		if len(strings.TrimSpace(a[i])) == 0 {
			removed++
		}

	}
	return a[:len(a)-removed]
}
