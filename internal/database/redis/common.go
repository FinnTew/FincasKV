package redis

import (
	"github.com/FinnTew/FincasKV/internal/database/base"
	"github.com/FinnTew/FincasKV/internal/storage"
	"log"
)

type DBWrapper struct {
	db *base.DB
}

func NewBDWrapper(dbOpts *base.BaseDBOptions, bcOpts ...storage.Option) *DBWrapper {
	db, err := base.NewDB(dbOpts, bcOpts...)
	if err != nil {
		log.Fatal(err)
	}
	return &DBWrapper{db: db}
}

func (db *DBWrapper) GetDB() *base.DB {
	return db.db
}
