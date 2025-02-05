package command

import (
	"encoding/json"
	"fmt"
	"github.com/FinnTew/FincasKV/internal/database"
)

type CmdTyp uint8

const (
	CmdString CmdTyp = iota
	CmdList
	CmdHash
	CmdSet
	CmdZSet
)

type MethodTyp uint8

const (
	// String method
	MethodSet MethodTyp = iota
	MethodDel
	MethodIncr
	MethodIncrBy
	MethodDecr
	MethodDecrBy
	MethodAppend
	MethodGetSet
	MethodSetNX
	MethodMSet
	// Hash method
	MethodHSet
	MethodHMSet
	MethodHDel
	MethodHIncrBy
	MethodHIncrByFloat
	MethodHSetNX
	// List method
	MethodLPush
	MethodRPush
	MethodLPop
	MethodRPop
	MethodLTrim
	MethodBLPop
	MethodBRPop
	MethodLInsert
	// Set method
	MethodSAdd
	MethodSRem
	MethodSPop
	MethodSMove
	// ZSet method
	MethodZAdd
	MethodZRem
	MethodZIncrBy
	MethodZRemRangeByRank
	MethodZRemRangeByScore
)

var (
	ErrArgsCount = fmt.Errorf("args count error")
	ErrSyntax    = fmt.Errorf("syntax error")
)

type Command interface {
	// GetType 获取类型
	GetType() CmdTyp

	// GetMethod 获取方法
	GetMethod() MethodTyp

	// Apply 应用到状态机
	Apply(db *database.FincasDB) error

	// Encode 编码命令
	Encode() ([]byte, error)
}

type BaseCmd struct {
	Typ    CmdTyp    `json:"type"`
	Method MethodTyp `json:"method"`
	Args   [][]byte  `json:"args"`
}

func (c *BaseCmd) GetType() CmdTyp {
	return c.Typ
}

func (c *BaseCmd) GetMethod() MethodTyp {
	return c.Method
}

func (c *BaseCmd) Encode() ([]byte, error) {
	return json.Marshal(c)
}

func NewCommand(typ CmdTyp, method MethodTyp, args [][]byte) Command {
	switch typ {
	case CmdString:
		return &StringCmd{
			BaseCmd: BaseCmd{
				Typ:    typ,
				Method: method,
				Args:   args,
			},
		}
	case CmdList:
		return &ListCmd{
			BaseCmd: BaseCmd{
				Typ:    typ,
				Method: method,
				Args:   args,
			},
		}
	case CmdHash:
		return &HashCmd{
			BaseCmd: BaseCmd{
				Typ:    typ,
				Method: method,
				Args:   args,
			},
		}
	case CmdSet:
		return &SetCmd{
			BaseCmd: BaseCmd{
				Typ:    typ,
				Method: method,
				Args:   args,
			},
		}
	case CmdZSet:
		return &ZSetCmd{
			BaseCmd: BaseCmd{
				Typ:    typ,
				Method: method,
				Args:   args,
			},
		}
	default:
		return nil
	}
}
