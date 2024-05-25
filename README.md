# de_technical_challenge
This repository contains Samuel Rojo technicall challenge solution for the Bitso Data Engineering Challenge.

## Description

This Repository centralizes dags, source code (src) separated by reusable applications and requested folders for
the airflow volumes (as a non-productive example for Airflow outputs).

Detailed information of the requested solutions are described within the README.md files
in the src/ directories individually.

```
+---dags
|   \---scrapping_dags
|   \---warehouse_dags
+---logs
+---outputs
+---src
|   \---order_book_scrapping
|       \---README.md
|   \---warehouse
|       \---README.md
+---tests
```

## Local setup

In order to setup the local execution of an airflow environment, the shell files provided on this repository 
should work as a setup, execution, and deletion.

****ACTION ITEM****: Generate your .env file using the .env_example adding your bitso API keys.
(Specifically, copy the file and rename it for ".env" with your BITSO API Key values)

### Build Airflow Docker Image

Build the airflow image and initialize the airflow-db and install dependencies.
```commandline
build_image.sh
```

Start the airflow server
```commandline
run_image.sh
```

Kill everything (just to keep images clean and tidy)
```commandline
clean_image.sh
```

NOTE: In case it's intended to review/reuse the src codes, the requirements.txt can be installed locally
and manually execute the files providing the required arguments. 

This steps won't be required on this documentation due all interactions should be executed through
airflow webserver and outputs generation.

### Execute Order Books Scrapping

1. Log-in into [Airflow Webserver](http://localhost:8080/home) (Local host)
    * user: airflow
    * pwd:  airflow

2. Activate process_order_book DAG
   * NOTE: Your hardware could be overwhelmed with an Airflow webserver running and a process making API calls 
   second, it's recommended to ensure resources can be enough to avoid killsignals in airflow server.

3. Pause the execution.
4. Review results on the output/ directory.


### Execute Warehouse Pipeline

1. Log-in into [Airflow Webserver](http://localhost:8080/home) (Local host)
    * user: airflow
    * pwd:  airflow

2. Activate warehouse DAG
3. Wait for completion
4. Review results on the output/ directory.