# 关于SQLC 2.x

基于jdk8，支持：Linux | macOS | Windows

一个简单的sql命令行工具，支持增、删、改、查、存储过程/函数、事务、批量导入数据、导出结果；

## 命令

`-u[url]` **jdbcUrl**（**必要参数**）

> 例如：`-ujdbc:postgresql://127.0.0.1:5432/postgres`

`-n[username]`

`-p[password]`

`-e"[sql|path[$> output]]"` 或者 `-e"[@path]"`

> sql语句或者读取文件内的sql语句执行，如果是查询语句，则可以使用重定向符号`$>`将结果导出到文件，结果类型取决于命令`-f`，不指定则默认为`tsv`；
>
> 如果路径前面有`@`符号，那么此指令的逻辑是执行批量导入数据，文件类型支持：`tsv|csv|xlsx|xls|json|sql`，sql为**insert**语句。
>
> 如果没有此参数，则进入**交互模式**。

> 更多参数说明请执行命令：`-h[elp]` 来查看。

## 交互模式

交互模式下，通过输入`:` 按下`Tab` 来查看内置的指令，可输入 `:help` 来查看所有命令的详细说明，以下，对其中几个指令做一些说明：

### :exec

**参数**：`[sql-file] [$> output]`

如指令（`-e"[sql|path[$> output]]"`）的读取sql文件，如果是查询，则可重定向输出到文件；

### :exec@

**参数**：`[input-file] [sheet-index] [header-index]`

如指令： `-e"[@path]"`，执行批量导入数据操作：

- 如果是excel文件，可以指定第二个可选参数 `sheet-index` 来读取指定的sheet，第三个可选参数 `header-index` 指定表头在第几行；
- 如果是tsv和csv文件，第二个可选参数为：`header-index`；

### :exec&

**参数**：`[sql-name]`

执行一条由[XQLFileManager](https://github.com/chengyuxing/rabbit-sql/tree/rabbit-sql-7#XQLFileManager)加载的sql，可支持动态sql，执行此指令的前提是通过 **:load** 指令加载了xql文件；

### :load

**参数**：`[xql-file]` as `[alias]`

加载一个xql文件，并为其命名别名；

### :tx

**参数**：`[begin|commit|rollback]`

### :edit

**参数**：[[proc|tg|view]:object]

编辑并保存**存储过程/函数 | 触发器 | 视图**，可通过输入前缀按下 `Tab` 来获取建议，类似的还有指令 `:ddl`。

> 更多指令和使用说明可以通过输入 `:help` 来查看。

## 附录

- 大多数时候可以通过 `Tab` 来获取一些输入建议；
- 建议的输入记录可以通过 `Ctrl + o` 来前进每个单词，而不是直接到结尾；
- `Ctrl + r` 调用搜索历史记录；
- 重定向操作符 `$> ` 只能作用于查询语句；
- 重定向导出sql类型文件和批量导入sql文件，支持二进制文件类型；

- 批量导入`@`文件操作，**文件名即是表名**，如：`test.user.json`，生成的sql语句形如：

  ```sql
  insert into test.user (...) values (...);
  ```

- 自定义数据库的自动完成提示：`/completion` 文件夹下，文件格式为：`数据库名.cnf`；

- 添加jdbc驱动：`/drivers` 文件夹下；

- 当事务启用生效时，题词颜色显示为==高亮黄色==；

### 执行sql脚本

- 预编译sql语句参数占位符格式为**传名参数**形如：

  ```sql
  select ... from table where id = :id;
  ```

- sql语句中的字符串模版格式为：

  ```sql
  select * from table where ${cnd};
  ```

- 存储过程/函数预编译参数格式同样为传**名参数**形如：

  无返回值：

  ```sql
  {call test.multi_query(:num, :factorial, :users, :animals)};
  ```

  > ```plsql
  > create function multi_query(num integer, OUT factorial bigint, OUT users refcursor, OUT animals refcursor) returns record
  >     language plpgsql
  > as
  > $$
  > begin
  >     factorial := factorial(num);
  >     open users for select * from test.big limit 10;
  >     open animals for select 'cat' as name, 12 as age, 'fish' as hobby;
  > end;
  > $$;
  > ```

  有返回值：

  ```sql
  {:res = call myfunc(:arg)}
  ```

  参数类型支持：**IN**、**OUT**、**IN OUT**，格式如下：

  - **IN**: `[IN value]`
  - **OUT**:  `out [OUT code]`
  - **IN OUT**: `inout [OUT code] [IN value]`

### 预编译sql参数

- 支持的基本数据类型：string(`"some string"`), boolean(`true`, `false`), double, int, null；

- 支持的数据类型：int[], float[], double[], string[], long[]，需要使用类型声明，如下：

  ```shell
  [1,2,3,4]::int[] #默认使用,号
  [a,b,c]::string[]
  [a;b;c]::string[;] #使用;号分隔数组
  ```

- 支持文件语法为以路径开头，例如：`./`，`../` ，`/`；

- 支持日期格式：`yyyy-MM-dd`, `yyyy/MM/dd`, `yyyy-MM-dd HH:mm:ss`, `HH:mm:ss`，需要使用类型声明如下：`2021-12-23::date`。

## 例子

- 登录：`./sqlc -ujdbc:postgresql://127.0.0.1:5432/postgres`

  > 如果没有指定 -n 和 -p，则进行交互式输入。

- 读取并执行sql：`-e/usr/local/a.sql`

- 批量导入insert sql文件：``-e@/usr/local/a.sql``

- 命令模式导出一个查询结果：

  ```shell
  ./sqlc -ujdbc:postgresql://127.0.0.1:5432/postgres -e"select * from big $> /Users/chengyuxing/Downloads/test.big" -fjson
  ```

  :warning: 如果sql中有字符串模版常量，需要改为使用单引号：

  ```shell
  -e'select * from big where ${cnd}'
  ```

- 执行查询并导出结果：

  ```sql
  select * from big $> /Users/chengyuxing/Downloads/big;
  ```

## 截图
- [命令模式执行预编译sql](screen_shot/command_prepare_query.gif)
- [切换结果打印视图](screen_shot/change_view.gif)
- [加载xql文件执行动态sql](screen_shot/xql.gif)
- [查询结果重定向到文件](screen_shot/redirect_to_file.gif)
- [批量导入数据](screen_shot/exec@.gif)
- [插入文件](screen_shot/insert_blob.gif)
- [执行存储过程](screen_shot/procedure.gif)
- [编辑存储过程](screen_shot/edit.gif)