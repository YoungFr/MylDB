package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type StmtType int

const (
	STMT_INSERT StmtType = iota
	STMT_SELECT
)

type Stmt struct {
	tp  StmtType
	row Row
}

type PrepareStmtResult int

const (
	PREPARE_STMT_SUCCESS PrepareStmtResult = iota
	PREPARE_STMT_UNKNOWN
	PREPARE_SYNTAX_ERROR
)

// 根据输入的命令准备好要执行的语句
func prepareStmt(cmd string, stmt *Stmt) PrepareStmtResult {
	fields := strings.Fields(cmd)
	switch strings.ToLower(fields[0]) {
	case "insert":
		// "INSERT [id] [username] [email]"
		stmt.tp = STMT_INSERT
		if len(fields) != 4 {
			fmt.Fprintf(os.Stderr, "mismatched item numbers to be inserted: expected 3 but got %d\n", len(fields)-1)
			return PREPARE_SYNTAX_ERROR
		}
		id, err := strconv.Atoi(fields[1])
		if err != nil {
			fmt.Fprintf(os.Stderr, "parsing id error: %s\n", err.Error())
			return PREPARE_SYNTAX_ERROR
		}
		if id < 0 {
			fmt.Fprintf(os.Stderr, "id must be larger or equal than zero\n")
			return PREPARE_SYNTAX_ERROR
		}
		stmt.row.id = int32(id)
		if len(fields[2]) > USERNAME_SIZE {
			fmt.Fprintf(os.Stderr, "username is too long: expected varchar(%d) but got varchar(%d)\n", USERNAME_SIZE, len(fields[2]))
			return PREPARE_SYNTAX_ERROR
		}
		var username [USERNAME_SIZE]byte
		for i := 0; i < len(fields[2]); i++ {
			username[i] = fields[2][i]
		}
		stmt.row.username = username
		if len(fields[3]) > EMAIL_SIZE {
			fmt.Fprintf(os.Stderr, "email is too long: expected varchar(%d) but got varchar(%d)\n", EMAIL_SIZE, len(fields[3]))
			return PREPARE_SYNTAX_ERROR
		}
		var email [EMAIL_SIZE]byte
		for i := 0; i < len(fields[3]); i++ {
			email[i] = fields[3][i]
		}
		stmt.row.email = email
		return PREPARE_STMT_SUCCESS
	case "select":
		// "SELECT"
		stmt.tp = STMT_SELECT
		return PREPARE_STMT_SUCCESS
	default:
		fmt.Fprintf(os.Stderr, "unknown statement type: %s\n", strings.ToLower(fields[0]))
		return PREPARE_STMT_UNKNOWN
	}
}
