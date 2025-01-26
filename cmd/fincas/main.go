package main

import (
	"fmt"
	"github.com/FinnTew/FincasKV/internal/storage"
	"log"
)

func main() {
	// 创建数据库实例
	db, err := storage.Open(
		storage.WithDataDir("./data"),
		storage.WithMaxFileSize(1<<30),  // 1GB
		storage.WithMemCacheSize(1<<20), // 1MB
	)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// 写入数据
	if err := db.Put("key1", []byte("value1")); err != nil {
		log.Fatal(err)
	}

	// 读取数据
	value, err := db.Get("key1")
	if err != nil {
		log.Fatal(err)
	} else {
		fmt.Println(string(value))
	}

	// 删除数据
	if err := db.Del("key1"); err != nil {
		log.Fatal(err)
	}

	// 合并文件
	if err := db.Merge(); err != nil {
		log.Fatal(err)
	}
}
