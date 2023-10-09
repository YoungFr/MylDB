package main

import (
	"bufio"
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "must supply a database file\n")
		os.Exit(1)
	}
	if len(os.Args) > 2 {
		fmt.Fprintf(os.Stderr, "only one database can be opened\n")
		os.Exit(1)
	}
	table := openDB(os.Args[1])
	var cmd string
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 0), 2*1024*1024)
	for {
		// 1. 打印命令提示符
		fmt.Print("MylDB > ")
		// 2. 读取输入
		if !sc.Scan() {
			if sc.Err() == nil {
				closeDB(table)
				os.Exit(0)
			}
			fmt.Fprintf(os.Stderr, "reading input error: %s\n", sc.Err().Error())
			os.Exit(1)
		}
		cmd = sc.Text()
		if cmd == "" {
			continue
		}
		// 3. 执行元命令
		if cmd[0] == '.' {
			switch execMetaCommand(cmd, table) {
			case META_COMMAND_SUCCESS:
				continue
			case META_COMMAND_UNKNOWN:
				fmt.Fprintf(os.Stderr, "unknown meta command: %s\n", cmd)
				continue
			}
		}
		// 4. 准备 SQL 语句
		var stmt Stmt
		switch prepareStmt(cmd, &stmt) {
		case PREPARE_STMT_SUCCESS:
			// break
		case PREPARE_STMT_UNKNOWN, PREPARE_SYNTAX_ERROR:
			continue
		}
		// 5. 执行 SQL 语句
		switch execStmt(&stmt, table) {
		case EXEC_SUCCESS:
			fmt.Printf("Executed.\n")
		case EXEC_TABLE_FULL:
			fmt.Fprintf(os.Stderr, "table is full\n")
		}
	}
}
