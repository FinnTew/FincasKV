package redis

import (
	base2 "github.com/FinnTew/FincasKV/database/base"
	"github.com/FinnTew/FincasKV/storage"
	"log"
)

type DBWrapper struct {
	db *base2.DB
}

func NewBDWrapper(dbOpts *base2.BaseDBOptions, bcOpts ...storage.Option) *DBWrapper {
	db, err := base2.NewDB(dbOpts, bcOpts...)
	if err != nil {
		log.Fatal(err)
	}
	return &DBWrapper{db: db}
}

func (db *DBWrapper) GetDB() *base2.DB {
	return db.db
}
