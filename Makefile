PROJECT = mysql

SHELL_OPTS = -s mysql_dev_reloader -s mysql

include erlang.mk

readme:
	multimarkdown README.md > /tmp/README.html
