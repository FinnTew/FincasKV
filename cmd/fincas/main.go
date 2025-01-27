package main

import (
	"fmt"
	"github.com/FinnTew/FincasKV/internal/database/base"
	"time"

	"github.com/FinnTew/FincasKV/internal/storage"
)

func main() {
	opts := base.DefaultBaseDBOptions()
	opts.ExpireCheckInterval = 30 * time.Second
	opts.FlushTTLOnChange = false
	opts.TTLMetadataFile = "my_ttl_data.txt"

	db, err := base.NewDB(opts,
		storage.WithDataDir("./data"),
		storage.WithMaxFileSize(32*1024))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Put("hello", "world")
	if err != nil {
		panic(err)
	}
	val, err := db.Get("hello")
	if err != nil {
		panic(err)
	}
	fmt.Println("hello =>", val)

	err = db.Expire("hello", 3*time.Second)
	if err != nil {
		panic(err)
	}
	time.Sleep(4 * time.Second) // 等过期

	exists, err := db.Exists("hello")
	if err != nil {
		panic(err)
	}
	fmt.Println("exists(hello) after TTL:", exists) // false

	_ = db.Put("test1", "foo")
	_ = db.Put("test2", "bar")
	keys, _ := db.Keys("test*")
	fmt.Println("keys(test*):", keys) // [test1 test2] 顺序未定

	t, _ := db.Type("test1")
	fmt.Println("Type of test1:", t) // string
	t, _ = db.Type("non-exists")
	fmt.Println("Type of non-exists:", t) // none

	_ = db.Expire("test1", 10*time.Second)
	db.Persist("test1")
}
