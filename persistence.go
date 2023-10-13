package main

import (
	"fmt"
	"io"
	"os"
)

func closeDB(table *Table) {
	pager := table.pager
	fullPageCnt := table.rowCnt / ROWS_PER_PAGE
	for i := 0; i < fullPageCnt; i++ {
		if pager.pages[i] == nil {
			continue
		}
		flushPager(pager, i, PAGE_SIZE)
	}
	if additionalRowCnt := table.rowCnt % ROWS_PER_PAGE; additionalRowCnt > 0 {
		flushPager(pager, fullPageCnt, additionalRowCnt*ROW_SIZE)
	}
	pager.dbfile.Close()
}

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
