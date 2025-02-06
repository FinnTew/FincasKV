package main

import (
	"flag"
	"fmt"
	"github.com/FinnTew/FincasKV/config"
	"github.com/FinnTew/FincasKV/database"
	"github.com/FinnTew/FincasKV/network/server"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var (
	Version   string
	BuildTime string
	CommitID  string
)

func printASCIILogo() {
	filePath := "./ascii_logo.txt"
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	logo, err := io.ReadAll(file)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(logo))
}

func main() {
	confPath := flag.String("conf", "./conf.yaml", "path to config file")
	port := flag.Int("port", 8911, "port to listen on")
	dataDir := flag.String("dir", "./fincas", "path to data")
	flag.Parse()

	if _, err := os.Stat(*confPath); os.IsNotExist(err) {
		log.Fatal("config file does not exist")
	}

	err := config.Init(*confPath)
	if err != nil {
		log.Fatal(err)
	}

	db := database.NewFincasDB(*dataDir)
	defer db.Close()

	addr := fmt.Sprintf(":%d", *port)
	srv, err := server.New(db, &addr)
	if err != nil {
		log.Fatal(err)
	}

	printASCIILogo()
	fmt.Println("FincasKV Version: ", Version)
	fmt.Println("Build Time: ", BuildTime)
	fmt.Println("Commit ID: ", CommitID)

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
