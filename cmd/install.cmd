@echo off

cd ..

call :delete env
call :delete build
call :delete dist

echo Создаю новое виртуальное окуржение "env" ...
python35 -m venv env
if %errorlevel% neq 0 goto error

call env\Scripts\activate
if %errorlevel% neq 0 goto error

python -m pip install --upgrade pip
if %errorlevel% neq 0 goto error

pip install --upgrade setuptools
if %errorlevel% neq 0 goto error

python setup.py install
if %errorlevel% neq 0 goto error

goto exit

:error
pause
exit

:delete
if exist %1 (
  echo Удаляю папку "%1" ...
  rmdir %1 /S /Q
)

:exit