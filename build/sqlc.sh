#!/usr/bin/env bash

DIRNAME=$0
if [ "${DIRNAME:0:1}" = "/" ]; then
  CURDIR=$(dirname $DIRNAME)
else
  CURDIR="$(pwd)"/"$(dirname $DIRNAME)"
fi
# 如果rlwrap存在，则使用rlwrap启动
if hash rlwrap 2>/dev/null; then
  # 指定命令行历史记录文件
  H_FILE="$HOME"/".sqlc_history"
  if [ ! -f $H_FILE ]; then
    touch $H_FILE
  fi
  # 根据数据库加载指定的关键字提示自动完成配置文件
  DBNAME="default"
  for item in "$@"; do
    if [[ $item == -ujdbc:* ]]; then
      DBNAME=${item%:/*}
      DBNAME=${DBNAME#-ujdbc:}
      break
    fi
  done

  rlwrap -c \
    -H $H_FILE \
    -f $CURDIR/completion/$DBNAME.cnf \
    java -jar $CURDIR/sqlc.jar "$@"
else
  printf '\033[93mWarning: "rlwrap" not found, some functions can not be use, but still work.\033[0m\n'
  printf '\033[93mSuggest install:\033[0m\n'
  printf '\t\033[93mMacOs: brew install rlwrap\033[0m\n'
  printf '\t\033[93mLinux: 1. yum –y install readline-devel\033[0m\n'
  printf '\t\t\033[93m2. Download source: https://github.com/hanslub42/rlwrap(or app dir deps/rlwrap-0.43.tar.gz) and make install.\033[0m\n'
  printf '\t\033[93mWindows:Enm...\033[0m\n'
  java -jar $CURDIR/sqlc.jar "$@"
fi
