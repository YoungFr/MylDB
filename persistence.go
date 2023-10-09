package main

import (
	"fmt"
	"io"
	"os"
)

// 关闭数据库时将所有非空页中的数据写入磁盘文件
func closeDB(table *Table) {
	pager := table.pager
	// 先写入所有非空的完整的页
	fullPageCnt := table.rowCnt / ROWS_PER_PAGE
	for i := 0; i < fullPageCnt; i++ {
		if pager.pages[i] == nil {
			continue
		}
		flushPager(pager, i, PAGE_SIZE)
	}
	// 表中最后的几行可能占不满一个页，在持久化时应该写入实际的行数而不是一个完整的页
	// 否则 openDB 函数中使用文件长度计算出的行数是错误的
	if additionalRowCnt := table.rowCnt % ROWS_PER_PAGE; additionalRowCnt > 0 {
		flushPager(pager, fullPageCnt, additionalRowCnt*ROW_SIZE)
	}
	pager.dbfile.Close()
}

// 将页号为 pageId 的页中的前 nbytes 个字节写入磁盘文件
func flushPager(pager *Pager, pageId int, nbytes int) {
	if pager.pages[pageId] == nil {
		fmt.Fprintf(os.Stderr, "tried to flush null page\n")
		os.Exit(1)
	}
	if _, err := pager.dbfile.Seek(int64(pageId*PAGE_SIZE), io.SeekStart); err != nil {
		fmt.Fprintf(os.Stderr, "flush page error: %s\n", err.Error())
		os.Exit(1)
	}
	if _, err := pager.dbfile.Write(pager.pages[pageId].rows[:nbytes]); err != nil {
		fmt.Fprintf(os.Stderr, "flush page error: %s\n", err.Error())
		os.Exit(1)
	}
}
