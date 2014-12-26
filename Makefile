PROJECT = mysql

SHELL_OPTS = -s mysql_dev_reloader -s mysql

include erlang.mk

export MYSQL_TEST_HOST ?= localhost
export MYSQL_TEST_PORT ?= 3306
export MYSQL_TEST_USER ?=
export MYSQL_TEST_PASSWORD ?=
export MYSQL_TEST_DATABASE ?= test

test:
	erl -pa ebin -noshell -eval 'mysql_test:run_all_cli()' -s init stop

readme:
	multimarkdown README.md > /tmp/README.html

watch-3306:
	sudo ngrep -x -q -d lo '' 'port 3306'
