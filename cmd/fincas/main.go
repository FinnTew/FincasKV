package main

import (
	"github.com/FinnTew/FincasKV/internal/database"
	"github.com/FinnTew/FincasKV/internal/storage"
	"log"
)

func main() {
	db := database.NewFincasDB(
		storage.WithDataDir("./fincas"),
	)
	defer db.Close()

	err := db.Set("key", "value")
	if err != nil {
		log.Fatal(err)
	}
	val, err := db.Get("key")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(val)
}
