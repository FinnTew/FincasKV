package command

import (
	"fmt"
	"github.com/FinnTew/FincasKV/database"
	"strconv"
)

type HashCmd struct {
	BaseCmd
}

func (c *HashCmd) Apply(db *database.FincasDB) error {
	switch c.GetMethod() {
	case MethodHSet:
		if len(c.Args) != 3 {
			return ErrArgsCount
		}
		return db.HSet(string(c.Args[0]), string(c.Args[1]), string(c.Args[2]))
	case MethodHMSet:
		if len(c.Args) < 3 {
			return ErrArgsCount
		}
		if (len(c.Args)-1)%2 != 0 {
			return ErrSyntax
		}
		kvPairs := make(map[string]string, (len(c.Args)-1)/2)
		for i := 1; i < len(c.Args); i += 2 {
			kvPairs[string(c.Args[i])] = string(c.Args[i+1])
		}
		return db.HMSet(string(c.Args[0]), kvPairs)
	case MethodHDel:
		if len(c.Args) < 2 {
			return ErrArgsCount
		}
		fields := make([]string, 0, len(c.Args))
		for _, arg := range c.Args {
			fields = append(fields, string(arg))
		}
		_, err := db.HDel(fields[0], fields[1:]...)
		return err
	case MethodHIncrBy:
		if len(c.Args) != 3 {
			return ErrArgsCount
		}
		val, err := strconv.ParseInt(string(c.Args[2]), 10, 64)
		if err != nil {
			return err
		}
		_, err = db.HIncrBy(string(c.Args[0]), string(c.Args[1]), val)
		return err
	case MethodHIncrByFloat:
		if len(c.Args) != 3 {
			return ErrArgsCount
		}
		val, err := strconv.ParseFloat(string(c.Args[2]), 64)
		if err != nil {
			return err
		}
		_, err = db.HIncrByFloat(string(c.Args[0]), string(c.Args[1]), val)
		return err
	case MethodHSetNX:
		if len(c.Args) != 3 {
			return ErrArgsCount
		}
		_, err := db.HSetNX(string(c.Args[0]), string(c.Args[1]), string(c.Args[2]))
		return err
	default:
		return fmt.Errorf("unsoprted method in hash command")
	}
}
