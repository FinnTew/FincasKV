package main

import (
	"github.com/FinnTew/FincasKV/internal/database/redis"
	"log"
)

//func main() {
//	opts := base.DefaultBaseDBOptions()
//	opts.ExpireCheckInterval = 30 * time.Second
//	opts.FlushTTLOnChange = false
//	opts.TTLMetadataFile = "my_ttl_data.txt"
//
//	db, err := base.NewDB(opts,
//		storage.WithDataDir("./data"),
//		storage.WithMaxFileSize(32*1024))
//	if err != nil {
//		panic(err)
//	}
//	defer db.Close()
//
//	err = db.Put("hello", "world")
//	if err != nil {
//		panic(err)
//	}
//	val, err := db.Get("hello")
//	if err != nil {
//		panic(err)
//	}
//	fmt.Println("hello =>", val)
//
//	err = db.Expire("hello", 3*time.Second)
//	if err != nil {
//		panic(err)
//	}
//	time.Sleep(4 * time.Second) // 等过期
//
//	exists, err := db.Exists("hello")
//	if err != nil {
//		panic(err)
//	}
//	fmt.Println("exists(hello) after TTL:", exists) // false
//
//	_ = db.Put("test1", "foo")
//	_ = db.Put("test2", "bar")
//	keys, _ := db.Keys("test*")
//	fmt.Println("keys(test*):", keys) // [test1 test2] 顺序未定
//
//	t, _ := db.Type("test1")
//	fmt.Println("Type of test1:", t) // string
//	t, _ = db.Type("non-exists")
//	fmt.Println("Type of non-exists:", t) // none
//
//	_ = db.Expire("test1", 10*time.Second)
//	db.Persist("test1")
//}

func main() {
	r := redis.RString{}
	r.MSet(map[string]string{
		"hello": "world",
		"foo":   "bar",
	})
	v, err := r.Get("hello")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(v)

	v, err = r.Get("foo")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(v)

	r.Set("val", "1")
	v, err = r.Get("val")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(v)
	r.Incr("val")
	v, err = r.Get("val")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(v)
	r.IncrBy("val", 2)
	v, err = r.Get("val")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(v)
	r.Decr("val")
	v, err = r.Get("val")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(v)
	r.DecrBy("val", 2)
	v, err = r.Get("val")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(v)
}
