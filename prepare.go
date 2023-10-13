package main

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
)

type PrepareStmtResult int

const (
	PREPARE_STMT_SUCCESS PrepareStmtResult = iota
	PREPARE_STMT_UNKNOWN
	PREPARE_SYNTAX_ERROR
)

func prepareStmt(cmd string, stmt *Stmt) PrepareStmtResult {
	fields := strings.Fields(cmd)
	switch strings.ToLower(fields[0]) {
	case "insert":
		stmt.tp = STMT_INSERT
		if len(fields) != 4 {
			fmt.Fprintf(os.Stderr, "mismatched number of fields to be inserted: expected 3 but got %d\n", len(fields)-1)
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
		if id > math.MaxInt32 {
			fmt.Fprintf(os.Stderr, "id is too large: %d > %d", id, math.MaxInt32)
			os.Exit(1)
		}
		stmt.row.id = int32(id)
		if len(fields[2]) > USERNAME_SIZE {
			fmt.Fprintf(os.Stderr, "username is too long: expected char(%d) but got char(%d)\n", USERNAME_SIZE, len(fields[2]))
			return PREPARE_SYNTAX_ERROR
		}
		var username [USERNAME_SIZE]byte
		for i := 0; i < len(fields[2]); i++ {
			username[i] = fields[2][i]
		}
		stmt.row.username = username
		if len(fields[3]) > EMAIL_SIZE {
			fmt.Fprintf(os.Stderr, "email is too long: expected char(%d) but got char(%d)\n", EMAIL_SIZE, len(fields[3]))
			return PREPARE_SYNTAX_ERROR
		}
		var email [EMAIL_SIZE]byte
		for i := 0; i < len(fields[3]); i++ {
			email[i] = fields[3][i]
		}
		stmt.row.email = email
		return PREPARE_STMT_SUCCESS
	case "select":
		stmt.tp = STMT_SELECT
		return PREPARE_STMT_SUCCESS
	default:
		fmt.Fprintf(os.Stderr, "unknown statement type: %s\n", strings.ToLower(fields[0]))
		return PREPARE_STMT_UNKNOWN
	}
}
