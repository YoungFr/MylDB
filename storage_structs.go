package main

import (
	"fmt"
	"io"
	"os"
	"unsafe"
)

// 数据库中只有一张 users 表，定义如下：
//
// +------------+------------+----------+
// | field name | MySQL type |  Go type |
// +------------+------------+----------+
// |     id     |    int     |   int32  |
// +------------+------------+----------+
// |  username  |  char(16)  | [16]byte |
// +------------+------------+----------+
// |    email   |  char(64)  | [64]byte |
// +------------+------------+----------+

const (
	COLUMN_USERNAME_SIZE = 16
	COLUMN_EMAIL_SIZE    = 64
)

const (
	// sizes
	ID_SIZE       = int(unsafe.Sizeof(int(0)))
	USERNAME_SIZE = int(unsafe.Sizeof(byte(0)) * COLUMN_USERNAME_SIZE)
	EMAIL_SIZE    = int(unsafe.Sizeof(byte(0)) * COLUMN_EMAIL_SIZE)
	ROW_SIZE      = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE

	// offsets
	ID_OFFSET       = 0
	USERNAME_OFFSET = ID_OFFSET + ID_SIZE
	EMAIL_OFFSET    = ID_OFFSET + ID_SIZE + USERNAME_SIZE
)

// 一个 user 对应表中的一行数据，使用 Row 结构存储
// 
// ID_OFFSET  USERNAME_OFFSET  EMAIL_OFFSET  ROW_SIZE-1
//  |              |                |             |
// +--------------+----------------+---------------+
// |      id      |    username    |     email     |
// +--------------+----------------+---------------+
// |                                               |
// +---------------------- Row --------------------+
type Row struct {
	id       int32
	username [USERNAME_SIZE]byte
	email    [EMAIL_SIZE]byte
}

const PAGE_SIZE = 4096
const ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE

// 一个页（Page）用于（连续地）存储尽可能多的（完整的）行（Row）中的数据，
// 一个行不会被跨页存储，所以页尾部的空间可能会被浪费。页中的内容在数据库
// 打开时从磁盘文件中加载（读入）、在数据库关闭时持久化（写入）到磁盘文件，
// 所以它的结构是一个字节数组
//
// +----+----------+-------+----+----------+-------+-----+----+------------+-------+----------+
// | id | username | email | id | username | email | ... | id |  username  | email |  wasted  |
// +----+----------+-------+----+----------+-------+-----+----+------------+-------+----------+
// |                       |                       |     |                         |          |
// +------- Row (0) -------+------- Row (1) -------+-...-+- Row (ROWS_PER_PAGE-1) -+- wasted -+
// |                                                                                          |
// +------------------------------------------- Page -----------------------------------------+
type Page struct {
	rows [PAGE_SIZE]byte
}

const TABLE_MAX_PAGES = 128
const TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES

// 一张表最多可以包含 TABLE_MAX_PAGES 个页，一个 Pager 结构用于管理这些页；
// 为了节省空间，对这些页的管理是使用页指针数组来实现的：只有在需要访问某个页时
// 才会为它分配空间，然后从磁盘中加载内容到内存，这是在 pageinfo 函数中实现的；
// 一个 Pager 结构包含一个打开的数据库文件，页中数据的加载和持久化是通过读写该文件实现的
//
//            +-----------------------+
//        --> | database file in disk |
//       /    +-----------------------+
//      /
// +---/----+--------+-----------------+-----------------+-----+---------------------------------+
// | dbfile | length | ptr to Page (0) | ptr to Page (1) | ... | ptr to Page (TABLE_MAX_PAGES-1) |
// +--------+--------+-----------------+-----------------+-----+---------------------------------+
// |                                                                                             |
// +-------------------------------------------- Pager ------------------------------------------+
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

// 一个 Table 结构表示一张打开的表
// 它包含表中的行数 rowCnt 和一个用于页管理的 Pager 结构体指针
//
//                      +-------+
//                 ---> | Pager |
//                /     +-------+
//               /
// +--------+---/---+
// | rowCnt | pager |
// +--------+-------+
// |                |
// +----- Table ----+
type Table struct {
	rowCnt int
	pager  *Pager
}

// 打开数据库 => 也就是打开 users 表
func openDB(name string) *Table {
	pager := openPager(name)
	rowCnt := (int(pager.length/PAGE_SIZE) * ROWS_PER_PAGE) + (int(pager.length%PAGE_SIZE) / ROW_SIZE)
	return &Table{rowCnt: rowCnt, pager: pager}
}

// 一个 Cursor 结构的作用类似于迭代器，成员 rowId 的值表示它当前指向表中的哪一行
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

// 根据游标（实际上是游标当前所指的行号）计算出对应的页号和页偏移量
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
