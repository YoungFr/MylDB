package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type ExecuteResult int

const (
	EXEC_SUCCESS ExecuteResult = iota
	EXEC_TABLE_FULL
)

func execStmt(stmt *Stmt, table *Table) ExecuteResult {
	switch stmt.tp {
	case STMT_INSERT:
		return execInsert(stmt, table)
	case STMT_SELECT:
		return execSelect(stmt, table)
	default:
		return EXEC_SUCCESS
	}
}

func execInsert(stmt *Stmt, table *Table) ExecuteResult {
	if table.rowCnt >= TABLE_MAX_ROWS {
		return EXEC_TABLE_FULL
	}
	serializeRow(&(stmt.row), tableEnd(table))
	table.rowCnt++
	return EXEC_SUCCESS
}

func serializeRow(row *Row, cursor *Cursor) {
	pageId, pageOffset := pageinfo(cursor)
	page := cursor.table.pager.pages[pageId]
	buf := make([]byte, 0)
	buf = append(buf, id2bytes(row.id)...)
	buf = append(buf, row.username[:]...)
	buf = append(buf, row.email[:]...)
	for i := 0; i < len(buf); i++ {
		page.rows[pageOffset+i] = buf[i]
	}
}

func id2bytes(n int32) []byte {
	bytebuf := bytes.NewBuffer([]byte{})
	binary.Write(bytebuf, binary.BigEndian, n)
	return bytebuf.Bytes()
}

func execSelect(stmt *Stmt, table *Table) ExecuteResult {
	var row Row
	for cursor := tableStart(table); !cursor.isEnd; cursorAdvance(cursor) {
		deserializeRow(&row, cursor)
		printRow(&row)
	}
	return EXEC_SUCCESS
}

func deserializeRow(row *Row, cursor *Cursor) {
	pageId, pageOffset := pageinfo(cursor)
	page := cursor.table.pager.pages[pageId]
	row.id = bytes2id(page.rows[pageOffset+ID_OFFSET : pageOffset+ID_OFFSET+ID_SIZE])
	row.username = [USERNAME_SIZE]byte(page.rows[pageOffset+USERNAME_OFFSET : pageOffset+USERNAME_OFFSET+USERNAME_SIZE])
	row.email = [EMAIL_SIZE]byte(page.rows[pageOffset+EMAIL_OFFSET : pageOffset+EMAIL_OFFSET+EMAIL_SIZE])
}

func bytes2id(bs []byte) int32 {
	bytebuf := bytes.NewBuffer(bs)
	var data int32
	binary.Read(bytebuf, binary.BigEndian, &data)
	return data
}

func printRow(row *Row) {
	fmt.Printf("(%d %s %s)\n", row.id, row.username, row.email)
}
