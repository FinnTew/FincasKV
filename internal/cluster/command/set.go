package command

import (
	"fmt"
	"github.com/FinnTew/FincasKV/internal/database"
	"strconv"
)

type SetCmd struct {
	BaseCmd
}

func (c *SetCmd) Apply(db *database.FincasDB) error {
	switch c.GetMethod() {
	case MethodSAdd:
		if len(c.Args) < 2 {
			return ErrArgsCount
		}
		key := string(c.Args[0])
		var members []string
		for _, v := range c.Args[1:] {
			members = append(members, string(v))
		}
		_, err := db.SAdd(key, members...)
		return err
	case MethodSRem:
		if len(c.Args) < 2 {
			return ErrArgsCount
		}
		key := string(c.Args[0])
		var members []string
		for _, v := range c.Args[1:] {
			members = append(members, string(v))
		}
		_, err := db.SRem(key, members...)
		return err
	case MethodSPop:
		switch len(c.Args) {
		case 1:
			_, err := db.SPop(string(c.Args[0]))
			return err
		case 2:
			count, err := strconv.ParseInt(string(c.Args[1]), 10, 64)
			if err != nil {
				return err
			}
			_, err = db.SPopN(string(c.Args[0]), int(count))
			return err
		default:
			return ErrArgsCount
		}
	case MethodSMove:
		if len(c.Args) != 3 {
			return ErrArgsCount
		}
		_, err := db.SMove(string(c.Args[0]), string(c.Args[1]), string(c.Args[2]))
		return err
	default:
		return fmt.Errorf("unsoprted method in set command")
	}
}
