# pycaruna
Caruna API for Python

I just added Python script which downloads and save data to local InfluxDB.

Example how to get data from yesterday: python3 getCarunaData.py USERNAME PW CARUNAID $(date -d "yesterday" +"%Y") $(date -d "yesterday" +"%m") $(date -d "yesterday" +"%d")
