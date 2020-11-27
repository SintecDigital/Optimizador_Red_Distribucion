Powershell.exe -ExecutionPolicy ByPass -NoExit -Command "& '~\AppData\Local\Continuum\anaconda3\shell\condabin\conda-hook.ps1';
cd $PSScriptRoot;
conda activate herramienta_distr;
python scripts\global.py | Out-File messages\herramienta_distr.txt -Encoding UTF8;
exit"