package main

type StmtType int

const (
	STMT_INSERT StmtType = iota
	STMT_SELECT
)

type Stmt struct {
	tp  StmtType
	row Row
}
