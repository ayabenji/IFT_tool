rem Dashboard Risques V0 - 08-07-2025
rem Python environment installation script (Windows)
call "%USERPROFILE%\\anaconda3\Scripts\conda.exe" init cmd.exe > nul
rem Set GAM proxy in the environment variables
set HTTP_PROXY=127.0.0.1:9000
set HTTPS_PROXY=127.0.0.1:9000
set REQUESTS_CA_BUNDLE=C:\Outils\cacert.pem

rem Clear Conda cache to prevent any issue
call conda clean --all

rem Create dedicated Python environment using the YAML file
call conda env update -f outil_dashboard.yml

rem Activate dedicated Python environment
call conda activate env_outil_dashboard


rem Return to base environment
call conda deactivate

rem Pause to keep the output visible
pause