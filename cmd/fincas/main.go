package main

import (
	"github.com/FinnTew/FincasKV/internal/database/redis"
	"log"
)

func main() {
	zs := redis.NewRZSet()
	res, err := zs.ZAdd("test", redis.ZMember{
		Member: "a",
		Score:  1.1,
	}, redis.ZMember{
		Member: "b",
		Score:  1.2,
	}, redis.ZMember{
		Member: "c",
		Score:  1.3,
	}, redis.ZMember{
		Member: "d",
		Score:  1.4,
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Println(res)

	members, err := zs.ZRangeByScoreWithScores("test", 0, 2)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(members)

	members, err = zs.ZRangeWithScores("test", 0, 2)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(members)

	rk, err := zs.ZRank("test", "a")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(rk)
}
