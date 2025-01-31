package main

import (
	"fmt"
	"github.com/FinnTew/FincasKV/internal/database/base"
	"github.com/FinnTew/FincasKV/internal/storage"
	"log"
	"time"
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

	batch := db.NewWriteBatch(nil)
	err = batch.Put("key1", "value1")
	if err != nil {
		log.Fatal(err)
	}
	err = batch.Put("key2", "value2")
	if err != nil {
		log.Fatal(err)
	}
	err = batch.Expire("key1", time.Hour)
	if err != nil {
		log.Fatal(err)
	}
	err = batch.Commit()
	if err != nil {
		log.Fatal(err)
	}
	val, err = db.Get("key1")
	if err != nil {
		panic(err)
	}
	fmt.Println("key1:", val)

	val, err = db.Get("key2")
	if err != nil {
		panic(err)
	}
	fmt.Println("key2:", val)
}

//func main() {
//	r := redis.RString{}
//	r.MSet(map[string]string{
//		"hello": "world",
//		"foo":   "bar",
//	})
//	res, err := r.MGet("hello", "foo")
//	if err != nil {
//		log.Fatal(err)
//	}
//	for k, v := range res {
//		length, err := r.StrLen(k)
//		if err != nil {
//			log.Fatal(err)
//		}
//		fmt.Printf("%s => %s (len: %d)\n", k, v, length)
//	}
//
//	r.Set("val", "1")
//	v, err := r.Get("val")
//	if err != nil {
//		log.Fatal(err)
//	}
//	log.Println(v)
//	r.Incr("val")
//	v, err = r.Get("val")
//	if err != nil {
//		log.Fatal(err)
//	}
//	log.Println(v)
//	r.IncrBy("val", 2)
//	v, err = r.Get("val")
//	if err != nil {
//		log.Fatal(err)
//	}
//	log.Println(v)
//	r.Decr("val")
//	v, err = r.Get("val")
//	if err != nil {
//		log.Fatal(err)
//	}
//	log.Println(v)
//	r.DecrBy("val", 2)
//	v, err = r.Get("val")
//	if err != nil {
//		log.Fatal(err)
//	}
//	log.Println(v)
//}
