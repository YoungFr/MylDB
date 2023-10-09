package main

import (
	"fmt"
	"os"
)

// 以 '.' 字符开头的命令称为元命令
type MetaCommandResult int

const (
	META_COMMAND_SUCCESS MetaCommandResult = iota
	META_COMMAND_UNKNOWN
)

func execMetaCommand(cmd string, table *Table) MetaCommandResult {
	if cmd == ".exit" {
		closeDB(table)
		fmt.Println("Bye.")
		os.Exit(0)
	}
	return META_COMMAND_UNKNOWN
}
