# snowflake-cdc-script-generator

### Objective

Data engineering and data migration projects often involve the development of various Slowly Changing Dimension (SCD) Type-1 and Type-2 tables. These tables require complex pattern such as: 
1.	Change Data Capture (CDC) where the data is compared with earlier version and the new version. This helps in capturing the changes for each record,
2.	the generation of unique keys, such as surrogate keys, to ensure data integrity.
3.	Merge the new data into the target table. 


### Local Dev Environment

Set up a virtual environment to install the libraries and run the code.

In case of using Anaconda,

Check conda is installed and in your PATH

1. Open a terminal client.
2. Enter `conda -V` into the terminal command line and press enter.
3. If conda is installed you should see somehting like the following.
4. Check conda is up to date
    ```bash
    conda update conda
    ```
5. Create a virtual environment for your project
    ```bash
    conda create -n yourenvname python=3.9 anaconda
    ```
    
6. Activate your virtual environment
    ```bash
    source activate yourenvname
    ```

7. Install additional Python packages to a virtual environment
    ```bash
    conda install -n yourenvname [package]
    ```

8. Deactivate your virtual environment
    ```bash
    source deactivate
    ```


### Install Dependencies

Install the below libraries

    conda install streamlit
    conda install snowflake-connector-python
    conda install snowflake-snowpark-python

  

### Run the code

1. Navigate to the folder you cloned the repo,

```bash
cd <folder>

streamlit run CDC_SCRIPT_GENERATOR.py
```
