package command

import (
	"fmt"
	"github.com/FinnTew/FincasKV/database"
	"strconv"
	"strings"
	"time"
)

type ListCmd struct {
	BaseCmd
}

func (c *ListCmd) Apply(db *database.FincasDB) error {
	switch c.GetMethod() {
	case MethodLPush:
		if len(c.Args) < 2 {
			return ErrArgsCount
		}
		key := string(c.Args[0])
		var vals []string
		for _, v := range c.Args[1:] {
			vals = append(vals, string(v))
		}
		_, err := db.LPush(key, vals...)
		return err
	case MethodRPush:
		if len(c.Args) < 2 {
			return ErrArgsCount
		}
		key := string(c.Args[0])
		var vals []string
		for _, v := range c.Args[1:] {
			vals = append(vals, string(v))
		}
		_, err := db.RPush(key, vals...)
		return err
	case MethodLPop:
		if len(c.Args) != 1 {
			return ErrArgsCount
		}
		_, err := db.LPop(string(c.Args[0]))
		return err
	case MethodRPop:
		if len(c.Args) != 1 {
			return ErrArgsCount
		}
		_, err := db.RPop(string(c.Args[0]))
		return err
	case MethodLTrim:
		if len(c.Args) != 3 {
			return ErrArgsCount
		}
		start, err := strconv.ParseInt(string(c.Args[1]), 10, 64)
		if err != nil {
			return err
		}
		end, err := strconv.ParseInt(string(c.Args[2]), 10, 64)
		if err != nil {
			return err
		}
		return db.LTrim(string(c.Args[0]), int(start), int(end))
	case MethodBLPop:
		if len(c.Args) < 2 {
			return ErrArgsCount
		}
		timeout, err := strconv.ParseInt(string(c.Args[len(c.Args)-1]), 10, 64)
		if err != nil {
			return err
		}
		var keys []string
		for _, arg := range c.Args[:len(c.Args)-1] {
			keys = append(keys, string(arg))
		}
		_, err = db.BLPop(time.Duration(timeout), keys...)
		return err
	case MethodBRPop:
		if len(c.Args) != 1 {
			return ErrArgsCount
		}
		timeout, err := strconv.ParseInt(string(c.Args[len(c.Args)-1]), 10, 64)
		if err != nil {
			return err
		}
		var keys []string
		for _, arg := range c.Args[:len(c.Args)-1] {
			keys = append(keys, string(arg))
		}
		_, err = db.BRPop(time.Duration(timeout), keys...)
		return err
	case MethodLInsert:
		if len(c.Args) != 4 {
			return ErrArgsCount
		}
		key := string(c.Args[0])
		before := string(c.Args[1])
		pivot := string(c.Args[2])
		elem := string(c.Args[3])
		var err error
		switch strings.ToUpper(before) {
		case "BEFORE":
			_, err = db.LInsertBefore(key, pivot, elem)
		case "AFTER":
			_, err = db.LInsertAfter(key, pivot, elem)
		default:
			return ErrSyntax
		}
		return err
	default:
		return fmt.Errorf("unsoprted method in list command")
	}
}
