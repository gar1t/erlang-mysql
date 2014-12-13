PROJECT = mysql

DEPS = emysql
dep_emysql = git https://github.com/gar1t/Emysql.git

SHELL_OPTS = -s mysql_dev_reloader -s mysql

include erlang.mk

readme:
	multimarkdown README.md > /tmp/README.html
