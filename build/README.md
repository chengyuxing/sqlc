# 关于SQLC

- 一个简单的sql命令行工具，支持增、删、改、查；
- 支持`mysql, oracle, postgreSQL, sqlServer，sqlite`数据库，可自行添加jdbc驱动文件到**drivers**目录下；
- 支持导出文件类型：`xlsx，json，tsv，csv`；
- 支持数据库关键字提示，根据指定数据库加载**completion**文件下的配置文件**（依赖C库：`readline`，`rlwrap`）**；
- 支持路径自动完成提示**（依赖C库：`readline`，`rlwrap`）**；
- 支持命令行历史记录**（依赖C库：`readline`，`rlwrap`）**；
- 支持命令模式和交互模式；

## 帮助文档

`sqlc.sh -h` 或 `sqlc.sh --help`

