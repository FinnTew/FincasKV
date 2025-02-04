package main

import (
	"github.com/FinnTew/FincasKV/internal/config"
	"github.com/FinnTew/FincasKV/internal/database"
	"github.com/FinnTew/FincasKV/internal/network/server"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	err := config.Init("./conf.yaml")
	if err != nil {
		log.Fatal(err)
	}

	db := database.NewFincasDB()
	defer db.Close()

	srv, err := server.New(db)
	if err != nil {
		log.Fatal(err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := srv.Start(); err != nil {
			log.Fatal(err)
		}
	}()

	<-sigCh
	log.Println("Shutting down...")

	if err := srv.Stop(); err != nil {
		log.Printf("Error shutting down: %v", err)
	}
}
