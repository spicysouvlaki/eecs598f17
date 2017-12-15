# How to build this project

Download this git repo
```
git clone <repo_url>
```
run the test environment
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

Troubleshooting:
- Python package missing, interpreter errors, etc:
    you likely need to rebuild the virtualenv in the docker container
    this is relatively easy to do
    ```
    # ensure that the correct versions of python are installed
    sudo apt-get update
    sudo apt-get install python3-pip # python3.4 should be installed by default on Ubuntu 14.04 (LTS)
    rm -rf ubuntu_venv # remove old package
    virtualenv --no-site-packages --always-copy -p $(which python3.4) ubuntu_venv # init new virtualenv
    pip install -r requirements.txt # install the dependencies specified in requirements
    ```

- Clearing docker images, containers
```
docker rm $(docker ps -a -q)
docker rmi $(docker images -q)
```

