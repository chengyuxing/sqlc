# 关于SQLC

- 一个简单的sql命令行工具，支持增、删、改、查，批量执行，事务，导出结果；
- 支持`mysql, oracle, postgreSQL, sqlServer，sqlite`等数据库，可自行添加jdbc驱动文件到**drivers**目录下；
- 支持导出文件类型：`xlsx，json，tsv，csv, sql`；
- 支持数据库关键字提示，根据指定数据库加载**completion**文件下的配置文件**（依赖C库：`readline`，`rlwrap`）**；
- 支持路径自动完成提示**（依赖C库：`readline`，`rlwrap`）**；
- 支持命令行历史记录**（依赖C库：`readline`，`rlwrap`）**；
- 支持命令模式和交互模式；

## 预编译SQL参数占位符

仅支持执行单条预编译sql

预编译sql使用的参数占位符为**传名参数**，除基本**数据类型**和**文件**外，其余需要使用**类型转换**
```sql
select * from test.tb where id < :id;
```
- 支持的基本数据类型：string(`"some string"`), boolean(`true`, `false`), double, int, null

- 支持的数据类型：int[], float[], double[], string[], long[]，需要使用类型声明，如下：

  ```bash
  [1,2,3,4]::int[] #默认使用,号
  [a,b,c]::string[]
  [a;b;c]::string[;] #使用;号分隔数组
  ```

- 支持文件：`/usr/local/a.txt`

- 支持日期格式：`yyyy-MM-dd`, `yyyy/MM/dd`, `yyyy-MM-dd HH:mm:ss`, `HH:mm:ss`，需要使用类型声明如下：

  ```
  2021-12-23::date
  ```

## 关于批量执行SQL

默认的多行sql块以双分号(`;;`)分隔，参数`-d`和`:d`可自定义

如果文件路径以`@`开头，则文件使用按行流式读取，内部则调用jdbc的`executeBatch`
方法进行批量执行ddl和dml语句，一般用于导入数据等大型文本文件执行大量sql；

- `-e@/usr/local/a.sql`
- `:load @/usr/local/a.sql`

一般情况则执行预编译操作批量执行（建议小数据量）

- `-e"select * from mytable;;select now();;create or replace function..." -d";;"`
- `-e/usr/local/a.sql`
- `:load /usr/local/a.sql`

## 关于导出查询结果

使用重定向符 `>` 导出查询结果，如果以`.sql`结尾，则导出`insert`语句，表名为文件名，否则根据`-f`或`:[tsv|csv|json|excel]`导出相应类型文件。

- 导出缓存：`:get res0 > /usr/local/res.sql`
- `:load /usr/local/query.sql > /usr/local/res.sql`
- `-e"select * from table > /usr/local/res" -fjson`
- `-e"/usr/local/query.sql > /usr/local/res.sql"`

## 例子

- `./sqlc.sh -ujdbc:postgresql://127.0.0.1:5432/postgres -nchengyuxing -p`
- `./sqlc.sh -ujdbc:postgresql://127.0.0.1:5432/postgres -nchengyuxing -p -e"select * from test.region where id < 10" -fjson`
- `./sqlc.sh -ujdbc:postgresql://127.0.0.1:5432/postgres -nchengyuxing -p -e/Users/chengyuxing/Downloads/a.sql`

## 帮助文档

- 命令模式 `sqlc.sh -h` 或 `sqlc.sh --help`
- 交互模式 `:help`

