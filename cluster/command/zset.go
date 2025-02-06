package command

import (
	"fmt"
	"github.com/FinnTew/FincasKV/database"
	"github.com/FinnTew/FincasKV/database/redis"
	"strconv"
	"strings"
)

type ZSetCmd struct {
	BaseCmd
}

func (c *ZSetCmd) Apply(db *database.FincasDB) error {
	switch c.GetMethod() {
	case MethodZAdd:
		if len(c.Args) < 3 {
			return ErrArgsCount
		}
		key := string(c.Args[0])
		var members []redis.ZMember
		for i := 1; i < len(c.Args); i += 2 {
			score, err := strconv.ParseFloat(string(c.Args[i]), 64)
			if err != nil {
				return err
			}
			members = append(members, redis.ZMember{
				Score:  score,
				Member: string(c.Args[i+1]),
			})
		}

		_, err := db.ZAdd(key, members...)
		return err
	case MethodZRem:
		if len(c.Args) < 2 {
			return ErrArgsCount
		}
		key := string(c.Args[0])
		var members []string
		for _, arg := range c.Args[1:] {
			members = append(members, string(arg))
		}
		_, err := db.ZRem(key, strings.Join(members, ","))
		return err
	case MethodZIncrBy:
		if len(c.Args) != 3 {
			return ErrArgsCount
		}

		key := string(c.Args[0])
		increment, err := strconv.ParseFloat(string(c.Args[1]), 64)
		if err != nil {
			return err
		}
		member := string(c.Args[2])
		_, err = db.ZIncrBy(key, member, increment)
		return err
	case MethodZRemRangeByRank:
		if len(c.Args) != 3 {
			return ErrArgsCount
		}
		key := string(c.Args[0])
		start, err := strconv.ParseInt(string(c.Args[1]), 10, 64)
		if err != nil {
			return err
		}
		stop, err := strconv.ParseInt(string(c.Args[2]), 10, 64)
		if err != nil {
			return err
		}

		_, err = db.ZRemRangeByRank(key, int(start), int(stop))
		return err
	case MethodZRemRangeByScore:
		if len(c.Args) != 3 {
			return ErrArgsCount
		}
		key := string(c.Args[0])
		minScore, err := strconv.ParseFloat(string(c.Args[1]), 64)
		if err != nil {
			return err
		}
		maxScore, err := strconv.ParseFloat(string(c.Args[2]), 64)
		if err != nil {
			return err
		}

		_, err = db.ZRemRangeByScore(key, minScore, maxScore)
		return err
	default:
		return fmt.Errorf("unsoprted method in zset command")
	}
}
