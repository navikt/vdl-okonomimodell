#!/bin/bash
export DBT_TARGET='dev_sso'
use="n"
if [ $DBT_USR ]; then
  echo "Current user:" $DBT_USR
  echo ""
fi

if [[ -z ${DBT_USR} ]]; then
  echo "Username:"
  read username
  export DBT_USR=$username
fi

use="n"
if [ $OKONOMIMODELL_DB ]; then
  echo "Current DB:" $OKONOMIMODELL_DB
  echo "Do you still want use the current database? Y/n"
  read use
  echo ""
  use=${use:-y}
fi

prod_db=okonomimodell

if [[ -z ${OKONOMIMODELL_DB+x} || $use = 'n' ]]; then
  echo "Choose a database:"
  select db in $prod_db dev_"$USER"_"$prod_db" other; do
    break;
  done
  if [ $db = 'other' ]; then
    echo Database:
    read db
    echo ""
  fi
  export OKONOMIMODELL_DB=$db
fi

limit=n
if [ $OKONOMIMODELL_DB != $prod_db ]; then
  echo "Limit source data? Y/n"
  read limit
  echo ""
  limit=${limit:-y}
fi
export LIMIT_SOURCE=$limit

recreate_db=n
if [ $OKONOMIMODELL_DB != $prod_db ]; then
  echo "Do you want to create / recreate the database: $OKONOMIMODELL_DB? y/N"
  read recreate_db
  echo ""
  recreate_db=${recreate_db:-n}
fi

if [ $recreate_db = 'y' ]; then
  snowbird clone $prod_db $OKONOMIMODELL_DB --usage "$prod_db"_transformer
fi
