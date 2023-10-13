😄 V1 😄

# 1. 基本流程

数据库的基本执行流程：

1. 输出命令提示符

   ```go
   // main.go
   fmt.Print("MylDB > ")
   ```

2. 读取用户输入

3. 判断是否要执行元命令（以 `.` 字符开头的命令）

   ```go
   // main.go
   if cmd[0] == '.' {
       ...
   }
   ```

4. 准备 SQL 语句
5. 执行 SQL 语句

# 2. 存储结构

## 2.1 表的定义

数据库中只有一张 `users` 表，它的定义如下：

```
+------------+------------+----------+
| field name | MySQL type |  Go type |
+------------+------------+----------+
|  id (PK)   |    int     |   int32  |
+------------+------------+----------+
|  username  |  char(16)  | [16]byte |
+------------+------------+----------+
|    email   |  char(64)  | [64]byte |
+------------+------------+----------+
```

## 2.2 行

一个 `user` 对应表中的一行数据，使用 `Row` 结构存储：

```go
// storage_structs.go
type Row struct {
	id       int32
	username [USERNAME_SIZE]byte
	email    [EMAIL_SIZE]byte
}
```

一个 `Row` 结构体后续会被序列化为一个字节数组持久化到磁盘中，在 `storage_structs.go` 文件中定义了一些常量表示结构体成员的大小和偏移量，它们用于后续的反序列化。一个 `Row` 结构体的存储布局如下：

```
ID_OFFSET   USERNAME_OFFSET   EMAIL_OFFSET
 ^               ^                 ^
 |               |                 |
+---------------+-----------------+----------------+
|      id       |     username    |      email     |
+---------------+-----------------+----------------+
+--- ID_SIZE ---+- USERNAME_SIZE -+-- EMAIL_SIZE --+
+--------------------- ROW_SIZE -------------------+
```

## 2.3 页

一个页（ `Page` ）是一个字节数组，用于（连续地）存储尽可能多的（完整的）行中的数据。一个行不会被跨页存储，所以页尾部的空间可能会被浪费。页中的内容在数据库打开时从磁盘文件中加载（读入）、在数据库关闭时持久化（写入）到磁盘文件。

```go
// storage_structs.go
type Page struct {
	rows [PAGE_SIZE]byte
}
```

一个页的存储布局如下：

```
 0                                                                                   PAGE_SIZE-1
 ^                                                                                        ^
 |                                                                                        |
+----+----------+-------+----+----------+-------+-----+----+------------+-------+----------+
| id | username | email | id | username | email | ... | id |  username  | email |  wasted  |
+----+----------+-------+----+----------+-------+-----+----+------------+-------+----------+
|                       |                       |     |                         |          |
+------- Row (0) -------+------- Row (1) -------+-...-+- Row (ROWS_PER_PAGE-1) -+- wasted -+
+------------------------------------------- Page -----------------------------------------+
```

## 2.4 页管理器

表中最多可以包含 `TABLE_MAX_PAGES` 个页，一个 `Pager` 结构用于管理这些页。为了节省空间，对这些页的管理是使用页指针数组来实现的：只有在需要访问某个页中的行时才会为它分配空间，然后从磁盘中加载页中的内容到内存，这个功能是在 `pageinfo` 函数中（2.7 节）实现的。一个 `Pager` 结构包含一个打开的数据库文件的文件描述符，页中数据的加载和持久化是通过读写该文件实现的。结构体 `Pager` 的定义如下：

```go
// storage_structs.go
type Pager struct {
	dbfile *os.File
	length int64
	pages  [TABLE_MAX_PAGES]*Page
}
```

一个 `Pager` 的储存布局如下：

```
             +-----------------------+
        ---> | database file in disk |
      /      +-----------------------+
     /
+---/----+--------+-----------------+-----------------+-----+---------------------------------+
| dbfile | length | ptr to Page (0) | ptr to Page (1) | ... | ptr to Page (TABLE_MAX_PAGES-1) |
+--------+--------+-----------------+-----------------+-----+---------------------------------+
|                                                                                             |
+-------------------------------------------- Pager ------------------------------------------+
```

## 2.5 表

一个 `Table` 结构体表示一张打开的表，它由当前表中的行数 `rowCnt` 和指向 `Pager` 结构体的指针组成：

```go
// storage_structs.go
type Table struct {
	rowCnt int
	pager  *Pager
}
```

## 2.6 游标

结构体 `Cursor` 的作用类似于一个迭代器。使用游标可以仅通过行号来访问数据库中的一条记录，我们还可以通过 `cursorAdvance` 函数来移动游标从而访问不同的行。它的定义如下：

```go
// storage_structs.go
type Cursor struct {
	table *Table
	rowId int
	isEnd bool
}
```

## 2.7 总结

数据库中的每一行数据被序列化成字节数组后再被连续存储在磁盘上（处于两个页交界处的行可能是不连续的），若干个行又被组织成一个抽象的 “页” 。假设一个页中能存储 48 行，那么整个数据库在磁盘上大致是这样存储的：

```
+-------+-------+-----+--------+--------+--------+--------+-----+--------+--------+-----
| row 0 | row 1 | ... | row 47 | wasted | row 48 | row 49 | ... | row 95 | wasted | ...
+-------+-------+-----+--------+--------+--------+--------+-----+--------+--------+-----
|              page 0                   |                    page 1               | ...
+---------------------------------------+-----------------------------------------+-----
```

当通过行号来访问某一行的数据时，这一行所在的整个页都会被加载到内存，这样在后续访问该行附近的行时就不用再进行磁盘 `I/O` 。因为每一个行号都对应唯一的页号和页偏移量，所以在页中的数据被加载到内存后就可以通过页号和页偏移量来访问我们想要的数据。函数 `pageinfo` 负责做这个转换以及加载页到内存：

```go
// storage_structs.go
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
```

# 3. 语句的准备和执行

一条 SQL 语句用一个 `Stmt` 结构体表示：

```go
// stmt.go
type StmtType int

type Stmt struct {
	tp  StmtType
	row Row
}
```

目前支持插入和查询语句。准备语句时首先要设置其类型，然后对于格式为 `INSERT [id] [username] [email]` 的插入语句，则会将成员 `row` 中对应的字段设置为对应的值；对于格式为 `SELECT` 的查询语句则不做其他处理。

语句准备好后就可以执行了。总体上来看，插入语句就是将一行数据序列化后添加到表末尾：

```go
// exec.go
func execInsert(stmt *Stmt, table *Table) ExecuteResult {
	if table.rowCnt >= TABLE_MAX_ROWS {
		return EXEC_TABLE_FULL
	}
	serializeRow(&(stmt.row), tableEnd(table))
	table.rowCnt++
	return EXEC_SUCCESS
}
```

查询语句就是遍历每一行，然后将其反序列化后存储到一个 `Row` 结构体中并打印出来：

```go
// exec.go
func execSelect(stmt *Stmt, table *Table) ExecuteResult {
	var row Row
	for cursor := tableStart(table); !cursor.isEnd; cursorAdvance(cursor) {
		deserializeRow(&row, cursor)
		printRow(&row)
	}
	return EXEC_SUCCESS
}
```

# 4. 数据持久化

执行插入语句后，数据都存储在位于内存中由 `Pager` 管理的多个 `Page` 中。当输入元命令 `.exit` 关闭数据库时，位于内存中的数据应该被写入到由 `Pager` 管理的 `dbfile` 文件中，所以数据的持久化就是将 `Pager` 管理的所有非空页写入磁盘文件：

```go
// persistence.go
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
```





