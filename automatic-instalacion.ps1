Powershell.exe -ExecutionPolicy ByPass -NoExit -Command "& '~\AppData\Local\Continuum\anaconda3\shell\condabin\conda-hook.ps1';
cd $PSScriptRoot;
conda create --name herramienta_distr --file requirements.txt --channel default --channel conda-forge;
exit"

