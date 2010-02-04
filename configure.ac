#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.63])
AC_INIT([FULL-PACKAGE-NAME], [VERSION], [BUG-REPORT-ADDRESS])
AC_CONFIG_SRCDIR([priv/erl_comm.c])
#AC_CONFIG_HEADERS([config.h])

# Checks for programs.
AC_PROG_CC
AC_PROG_MAKE_SET

## Checks for libraries.
## FIXME: Replace `main' with a function in `-lei':
#AC_CHECK_LIB([ei], [main])
## FIXME: Replace `main' with a function in `-lerl_interface':
#AC_CHECK_LIB([erl_interface], [main])
## FIXME: Replace `main' with a function in `-lsqlite3':
#AC_CHECK_LIB([sqlite3], [main])

# Checks for header files.
AC_CHECK_HEADERS([stdlib.h string.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_C_INLINE

# Checks for library functions.
AC_FUNC_MALLOC
AC_FUNC_REALLOC
AC_CHECK_FUNCS([bzero strstr])

AC_ERLANG_NEED_ERLC
AC_ERLANG_SUBST_ROOT_DIR
AC_ERLANG_SUBST_LIB_DIR
AC_ERLANG_CHECK_LIB(erl_interface)
AC_ERLANG_CHECK_LIB(kernel)
AC_ERLANG_CHECK_LIB(stdlib)
AC_ERLANG_CHECK_LIB(crypto)
AC_ERLANG_CHECK_LIB(compiler)
AC_ERLANG_CHECK_LIB(hipe)
AC_ERLANG_CHECK_LIB(syntax_tools)

AC_CONFIG_FILES([Makefile
                 priv/Makefile])
AC_OUTPUT
