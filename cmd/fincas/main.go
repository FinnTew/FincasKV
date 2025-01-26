package main

import (
	"fmt"
	"github.com/FinnTew/FincasKV/internal/storage"
	"log"
	"strconv"
)

func main() {
	// 创建数据库实例
	db, err := storage.Open(
		storage.WithDataDir("./data"),
		storage.WithMaxFileSize(1<<30),  // 1GB
		storage.WithMemCacheSize(1<<20), // 1MB
		storage.WithMemIndexDS(storage.BTree),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// 写入数据
	for i := 1; i <= 1000; i++ {
		if err := db.Put("key"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i))); err != nil {
			log.Fatal(err)
		}
	}

	// 读取数据
	for i := 1; i <= 1000; i++ {
		value, err := db.Get("key" + strconv.Itoa(i))
		if err != nil {
			log.Fatal(err)
		} else {
			fmt.Println(string(value))
		}
	}

	// 删除数据
	for i := 1; i <= 1000; i += 2 {
		if err := db.Del("key" + strconv.Itoa(i)); err != nil {
			log.Fatal(err)
		}
	}

	// 合并文件
	if err := db.Merge(); err != nil {
		log.Fatal(err)
	}

	for i := 1; i <= 1000; i++ {
		value, err := db.Get("key" + strconv.Itoa(i))
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println(string(value))
		}
	}

	err = db.Close()
	if err != nil {
		log.Fatal(err)
	}
}
