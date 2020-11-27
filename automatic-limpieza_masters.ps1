Powershell.exe -ExecutionPolicy ByPass -NoExit -Command "& '~\AppData\Local\Continuum\anaconda3\shell\condabin\conda-hook.ps1';
cd $PSScriptRoot;
conda activate herramienta_distr;
python scripts\limpieza_masters.py | Out-File messages\limpieza_masters.txt -Encoding UTF8;
exit"