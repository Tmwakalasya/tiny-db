package main

import (
	"fmt"
	"os"

	"github.com/Tmwakalasya/tiny-db"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "demo:", err)
		os.Exit(1)
	}
}

func run() error {
	db := tinydb.New()
	key := "api:weather:boston"

	if err := db.Put(key, `{"temperature_c":22,"conditions":"sunny"}`); err != nil {
		return err
	}
	value, found := db.Get(key)
	fmt.Printf("After Put:    found=%t value=%s\n", found, value)

	if err := db.Put(key, `{"temperature_c":19,"conditions":"cloudy"}`); err != nil {
		return err
	}
	value, found = db.Get(key)
	fmt.Printf("After update: found=%t value=%s\n", found, value)

	deleted, err := db.Delete(key)
	if err != nil {
		return err
	}
	_, found = db.Get(key)
	fmt.Printf("After Delete: deleted=%t found=%t\n", deleted, found)

	fmt.Println("New() stores data in memory; it is lost when the process exits.")
	fmt.Println("For persistence: go run ./cmd/persist -path ./tinydb.log")
	fmt.Println("To measure performance: go run ./cmd/metrics")
	return db.Close()
}
