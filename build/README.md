# 关于SQLC

- 一个简单的sql命令行工具，支持增、删、改、查；
- 支持`mysql, oracle, postgreSQL, sqlServer，sqlite`数据库，可自行添加jdbc驱动文件到**drivers**目录下；
- 支持导出文件类型：`xlsx，json，tsv，csv`；
- 支持数据库关键字提示，根据指定数据库加载**completion**文件下的配置文件**（依赖C库：`readline`，`rlwrap`）**；
- 支持路径自动完成提示**（依赖C库：`readline`，`rlwrap`）**；
- 支持命令行历史记录**（依赖C库：`readline`，`rlwrap`）**；
- 支持命令模式和交互模式；

## 关于批量执行SQL（-b）
- sql文件内多条sql以分号(;)分隔；
- sql名和sql必须成对，如果没有使用过rabbit-sql的`XQLFileManager`，格式为：`/*[扩展注释的sql名（包含中括号）]*/`占一行，下一行为sql并以分号结尾；
## 例子
- `./sqlc.sh -ujdbc:postgresql://127.0.0.1:5432/postgres -nchengyuxing -p`
- `./sqlc.sh -ujdbc:postgresql://127.0.0.1:5432/postgres -nchengyuxing -p -e"select * from test.region where id < 10" -fjson`
- `./sqlc.sh -ujdbc:postgresql://127.0.0.1:5432/postgres -nchengyuxing -p -x/Users/chengyuxing/Downloads/a.sql`
## 帮助文档

- 命令模式 `sqlc.sh -h` 或 `sqlc.sh --help`
- 交互模式 `:help`

