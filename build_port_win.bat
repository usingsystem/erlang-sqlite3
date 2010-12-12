@echo off
call "C:\Program Files (x86)\Microsoft Visual Studio 10.0\VC\bin\vcvars32.bat"
set ERL_ROOT="F:\ProgLangs\erl"
set ERL_INTERFACE="%ERL_ROOT%\lib\erl_interface-3.7.2"
set ERTS="%ERL_ROOT%\erts-5.8.2"
set SQLITE_SRC="F:\MyProgramming\sqlite-amalgamation"

cl /W4 /wd4100 /wd4204 /I%ERL_INTERFACE%/include /I%ERTS%/include /I%SQLITE_SRC% /Ic_src c_src/*.c /link /dll %ERL_INTERFACE%\lib\ei.lib %SQLITE_SRC%\sqlite3.lib /out:priv\sqlite3_drv.dll
