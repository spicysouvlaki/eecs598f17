# How to build this project

Download this git repo
```
git clone git@github.com:william-schmitt/eecs598f17
```
run the test environment (cloned from: docker repo @ https://hub.docker.com/r/cargoembargo/eecs598schmittw/)
```
docker run -it --cpus='2' --memory='8g' -v /location/of/repo/on/host:/root/vaastr cargoembargo/eecs598schmittw
```
to reproduce all data in the experiment:
```
python driver.py
python ./data/data_parsing_utils.py
```
Meta-Data from each experiment is now available in data/master_data.csv
Data collected from the window of each experiment is available in the respective files under data.py
Timing data will be output to the standard console

** WARN: It will take a number of hours to run driver.py **
