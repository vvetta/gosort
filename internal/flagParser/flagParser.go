package flagParser

import (
	"strconv"

	"github.com/vvetta/gosort/internal/domain"
)

type FlagParser struct {
	cmdArgs []string
}

func NewFlagParser(cmdArgs []string) *FlagParser {
	return &FlagParser{cmdArgs: cmdArgs}
}

func (f *FlagParser) Parse() (sortFlag domain.SortFlags, err error) {
	defer func () {
		if r := recover(); r != nil {
			err = domain.ErrWithKFlag	
		}
	}()

	var sortFlags domain.SortFlags
	
	kValueIdx := -1
	// - (45) k(107) r(114) n(110) u(117)
	for i, flag := range f.cmdArgs {
		if i == kValueIdx {continue}
		if len(flag) > 2 && flag[0] == 45 {
			for j := 1; j < len(flag); j++ {
				switch flag[j] {
				case 114:
					sortFlags.R = true
				case 110: 
					sortFlags.N = true
				case 117:
					sortFlags.U = true
				default:
					continue
				}
			}
		} else if len(flag) == 2 && flag[0] == 45 && flag[1] == 107 {
			var err error
			sortFlags.K, err = strconv.Atoi(f.cmdArgs[i + 1])
			if err != nil {
				return domain.SortFlags{}, domain.ErrWithKFlag	
			}
			kValueIdx = i + 1
		} else if len(flag) == 2 && flag[0] == 45 {
			switch flag[1] {
			case 114:
				sortFlags.R = true
			case 110:
				sortFlags.N = true
			case 117:
				sortFlags.U = true
			default:
				continue
			}
		} else if len(flag) >= 1 && flag[0] != 45 {
			sortFlags.Filename = flag
		}
	}

	if sortFlags.K < 0 {
		err = domain.ErrKFlagLessZero	
		sortFlags = domain.SortFlags{}	
		return
	}

	return sortFlags, nil
}
