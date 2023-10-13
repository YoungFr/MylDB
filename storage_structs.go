package main

import (
	"fmt"
	"io"
	"os"
	"unsafe"
)

const (
	COLUMN_USERNAME_SIZE = 16
	COLUMN_EMAIL_SIZE    = 64
)

const (
	ID_SIZE   = int(unsafe.Sizeof(int32(0)))
	ID_OFFSET = 0

	USERNAME_SIZE   = int(unsafe.Sizeof(byte(0)) * COLUMN_USERNAME_SIZE)
	USERNAME_OFFSET = ID_OFFSET + ID_SIZE

	EMAIL_SIZE   = int(unsafe.Sizeof(byte(0)) * COLUMN_EMAIL_SIZE)
	EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE

	ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE
)

type Row struct {
	id       int32
	username [USERNAME_SIZE]byte
	email    [EMAIL_SIZE]byte
}

const PAGE_SIZE = 4096
const ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE

type Page struct {
	rows [PAGE_SIZE]byte
}

const TABLE_MAX_PAGES = 128
const TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES

type Pager struct {
	dbfile *os.File
	length int64
	pages  [TABLE_MAX_PAGES]*Page
}

func openPager(name string) *Pager {
	dbfile, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open pager error: %s\n", err.Error())
		os.Exit(1)
	}
	length, err := dbfile.Seek(0, io.SeekEnd)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open pager error: %s\n", err.Error())
		os.Exit(1)
	}
	return &Pager{dbfile: dbfile, length: length}
}

type Table struct {
	rowCnt int
	pager  *Pager
}

func openDB(name string) *Table {
	pager := openPager(name)
	rowCnt := (int(pager.length/PAGE_SIZE) * ROWS_PER_PAGE) + (int(pager.length%PAGE_SIZE) / ROW_SIZE)
	return &Table{rowCnt: rowCnt, pager: pager}
}

type Cursor struct {
	table *Table
	rowId int
	isEnd bool
}

func tableStart(table *Table) *Cursor {
	return &Cursor{table: table, rowId: 0, isEnd: table.rowCnt == 0}
}

func tableEnd(table *Table) *Cursor {
	return &Cursor{table: table, rowId: table.rowCnt, isEnd: true}
}

func cursorAdvance(cursor *Cursor) {
	cursor.rowId++
	if cursor.rowId >= cursor.table.rowCnt {
		cursor.isEnd = true
	}
}

func pageinfo(cursor *Cursor) (pageId int, pageOffset int) {
	pageId = cursor.rowId / ROWS_PER_PAGE
	pageOffset = (cursor.rowId % ROWS_PER_PAGE) * ROW_SIZE
	if pageId > TABLE_MAX_PAGES-1 {
		fmt.Fprintf(os.Stderr, "page id is out of bound: %d > %d\n", pageId, TABLE_MAX_PAGES-1)
		os.Exit(1)
	}
	pager := cursor.table.pager
	if pager.pages[pageId] == nil {
		pager.pages[pageId] = &Page{}
		if _, err := pager.dbfile.Seek(int64(pageId*PAGE_SIZE), io.SeekStart); err != nil {
			fmt.Fprintf(os.Stderr, "load page error: %s\n", err.Error())
			os.Exit(1)
		}
		if _, err := pager.dbfile.Read(pager.pages[pageId].rows[:]); err != nil {
			if err == io.EOF {
				return
			}
			fmt.Fprintf(os.Stderr, "load page error: %s\n", err.Error())
			os.Exit(1)
		}
	}
	return
}
