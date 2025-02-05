package command

import (
	"fmt"
	"github.com/FinnTew/FincasKV/internal/database"
	"strconv"
)

type StringCmd struct {
	BaseCmd
}

func (c *StringCmd) Apply(db *database.FincasDB) error {
	switch c.GetMethod() {
	case MethodSet:
		if len(c.Args) != 2 {
			return ErrArgsCount
		}
		return db.Set(string(c.Args[0]), string(c.Args[1]))
	case MethodDel:
		if len(c.Args) != 1 {
			return ErrArgsCount
		}
		return db.Del(string(c.Args[0]))
	case MethodIncr:
		if len(c.Args) != 1 {
			return ErrArgsCount
		}
		_, err := db.Incr(string(c.Args[0]))
		return err
	case MethodIncrBy:
		if len(c.Args) != 2 {
			return ErrArgsCount
		}
		val, err := strconv.ParseInt(string(c.Args[1]), 10, 64)
		if err != nil {
			return err
		}
		_, err = db.IncrBy(string(c.Args[0]), val)
		return err
	case MethodDecr:
		if len(c.Args) != 1 {
			return ErrArgsCount
		}
		_, err := db.Decr(string(c.Args[0]))
		return err
	case MethodDecrBy:
		if len(c.Args) != 2 {
			return ErrArgsCount
		}
		val, err := strconv.ParseInt(string(c.Args[1]), 10, 64)
		if err != nil {
			return err
		}
		_, err = db.DecrBy(string(c.Args[0]), val)
		return err
	case MethodAppend:
		if len(c.Args) != 2 {
			return ErrArgsCount
		}
		_, err := db.Append(string(c.Args[0]), string(c.Args[1]))
		return err
	case MethodGetSet:
		if len(c.Args) != 2 {
			return ErrArgsCount
		}
		_, err := db.GetSet(string(c.Args[0]), string(c.Args[1]))
		return err
	case MethodSetNX:
		if len(c.Args) != 2 {
			return ErrArgsCount
		}
		_, err := db.SetNX(string(c.Args[0]), string(c.Args[1]))
		return err
	case MethodMSet:
		if len(c.Args) < 2 {
			return ErrArgsCount
		}
		if len(c.Args)%2 != 0 {
			return ErrSyntax
		}
		kvPairs := make(map[string]string, len(c.Args)/2)
		for i := 0; i < len(c.Args); i += 2 {
			kvPairs[string(c.Args[i])] = string(c.Args[i+1])
		}
		return db.MSet(kvPairs)
	default:
		return fmt.Errorf("unsoprted method in string command")
	}
}
