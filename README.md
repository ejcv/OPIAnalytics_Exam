# OPIAnalytics_Exam

This is a really simple approach to solve the task. 

## Environment variables
You can modify the path to match the location where the database is. It should contain the tamales_inc and teinvento_inc folders inside and also you can modify the path were the results will be stored.

## Requirements 
To install all the required dependencies you can run inside the project folder:

    conda create --name <env> --file requirements.txt
  
## Spark
This script uses spark to process the data. It is not going to be explained here how to install it. You should have an active session and launch the scripts from it.
  
## Run scripts
Each script processes the data from their respective name, to run either of them you can use:

    python cleaning_TamalesInc.py 
  
    python cleaning_TeInventoInc.py 
  
## Results

Results are saved in the OUTPUT_PATH folder and follow this structure:
  1) ./crudo/generador/fuente/AAAAMMDD/
  2) ./procesado/generador/fuente/AAAAMMDD/
  
