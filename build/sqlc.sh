#!/usr/bin/env bash

DIRNAME=$0
if [ "${DIRNAME:0:1}" = "/" ]; then
  CURDIR=$(dirname "$DIRNAME")
else
  CURDIR="$(pwd)"/"$(dirname "$DIRNAME")"
fi
# 如果rlwrap存在，则使用rlwrap启动
if hash rlwrap 2>/dev/null; then
  # 默认情况是交互模式（1）命令模式（0）
  MODE=1
  # 只有一个参数那么就根本不具备进入交互模式的可能，所以在此排除
  if [ $# = 1 ]; then
    MODE=0
  else
    DBNAME=default
    for item in "$@"; do
      # 如果存在参数-e，则说明不是交互模式
      if [[ $item == -e* ]]; then
        MODE=0
        break
      fi
      # 根据数据库加载指定的关键字提示自动完成配置文件
      if [[ $item == -ujdbc:* ]]; then
        UN=$item\ $UN
        DBNAME=${item#-ujdbc:*}
        DBNAME=${DBNAME%%:*}

        if [ "$DBNAME" = mysql ]; then
          HAS_DB=$(awk -v url=$item 'BEGIN{print match(url,/:[0-9]+\//) ? "y" : "n"}')
          if [ "$HAS_DB" = y ]; then
            MYSQL_SCHEMA=${item##*/}
            MYSQL_SCHEMA=${MYSQL_SCHEMA%\?*}
          fi
        fi
      fi
      if [[ $item == -n* ]]; then
        UN=$item\ $UN
        USERNAME=${item#-n*}
      fi
      if [[ $item == -p* ]]; then
        UN=$item\ $UN
      fi
    done
  fi
  # 如果是交互模式，那么才需要创建历史记录文件，和关键字与表名的自动完成
  if [ $MODE = 1 ]; then
    # 指定命令行历史记录文件
    H_FILE="$HOME"/.sqlc_history
    if [ ! -f "$H_FILE" ]; then
      touch "$H_FILE"
    fi

    TEMP_COMPLETION="$CURDIR"/temp_completion.tsv
    if [ ! -f "$TEMP_COMPLETION" ]; then
      touch "$TEMP_COMPLETION"
    fi

    echo "loading completion..."
    if [ "$DBNAME" = postgresql ]; then
      rlwrap -c java -jar "$CURDIR"/sqlc.jar $UN "-eselect tablename from pg_tables where tableowner = '$USERNAME' > $TEMP_COMPLETION" >/dev/null
    elif [ "$DBNAME" = oracle ]; then
      rlwrap -c java -jar "$CURDIR"/sqlc.jar $UN "-eselect table_name from user_tables > $TEMP_COMPLETION" >/dev/null
    elif [ "$DBNAME" = mysql ] && [ "$HAS_DB" = y ]; then
      rlwrap -c java -jar "$CURDIR"/sqlc.jar $UN "-eselect TABLE_NAME from information_schema.TABLES where TABLE_SCHEMA = '$MYSQL_SCHEMA' > $TEMP_COMPLETION" >/dev/null
    fi

    # 把内置的关键字自动完成文件追加到临时文件中
    while read -r line; do
      echo "$line" >>"$TEMP_COMPLETION"
    done <"$CURDIR/completion/$DBNAME.cnf"
  fi

  rlwrap -c -H $H_FILE -f $TEMP_COMPLETION java -jar "$CURDIR"/sqlc.jar "$@"

  # 程序启动后，如果有临时文件，那么就删除
  if [ -f "$TEMP_COMPLETION" ]; then
    rm -f "$TEMP_COMPLETION"
  fi

else
  printf '\033[93mWarning: "rlwrap" not found, some functions can not be use, but still work.\033[0m\n'
  printf '\033[93mSuggest install:\033[0m\n'
  printf '\t\033[93mMacOs: brew install rlwrap\033[0m\n'
  printf '\t\033[93mLinux: 1. yum –y install readline-devel\033[0m\n'
  printf '\t\t\033[93m2. Download source: https://github.com/hanslub42/rlwrap(or app dir deps/rlwrap-0.43.tar.gz) and make install.\033[0m\n'
  printf '\t\033[93mWindows:Enm...\033[0m\n'
  java -jar "$CURDIR"/sqlc.jar "$@"
fi