PROJECT = mysql

SHELL_OPTS = -s mysql_dev_reloader -s mysql

include erlang.mk

test:
	erl -pa ebin -noshell -eval 'mysql_test:run_all()' -s init stop

readme:
	multimarkdown README.md > /tmp/README.html

watch-3306:
	sudo ngrep -x -q -d lo '' 'port 3306'
