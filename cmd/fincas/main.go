package main

import (
	"github.com/FinnTew/FincasKV/internal/config"
	"github.com/FinnTew/FincasKV/internal/database"
	"log"
)

func main() {
	err := config.Init("./conf.yaml")
	if err != nil {
		log.Fatal(err)
	}

	db := database.NewFincasDB()
	err = db.Set("hello", "world")
	if err != nil {
		log.Fatal(err)
	}
	val, err := db.Get("hello")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(val)
}
