# Tech-Summit-2022


How to create the DLT pipeline using the given codes in this repo?

Step 1. Please run both notebooks given in the "Appendix notebooks: InputData & ML model" folder. Notebook "0. Loan Data Generator" is needed to generate the input data needed for the creation of tables in the DLT pipeline . Then you need to run the notebook "01. Loan Risk Analysis" in that folder to create, register and deploy your loan risk analysis ML model. Command 18 in the notebook "1a-DLT-Loan-pipeline-SQL", and the whole notebook "1b-SQL-Delta-Live-Table-Python-UDF" under "DLT Source Notebooks" folder, both are relying on the registration of this ML model. If you want to skip from this step you can comment out these parts in both notebooks before running your DLT pipeline.

Step 2. After successfully generating your input data and registering your ML model, run command 3 in the notebook "1a-DLT-Loan-pipeline-SQL" under "DLT Source Notebooks" folder, to generate the json configuration you need to create your DLT pipeline. Alternatively you can manually add these configuration in the UI when creating your pipeline. 

Step 3. In the side bar menue in your workspace click on the + sign "New" and navigate to "Pipeline" option among the available options and click on Pipeline. Now you can create your pipeline using JSON config you got from step 2. You can simply copy and paste the JSON output from step 2 in the JSON section of the settings by overwriting the existing JSON content. Then click on Create. 

Step 4. Now you can click on the Start button in the upper right side of your pipeline environment, and your DLT pipeline get triggered and will start running. 


-------------
Additional Notebooks:

- For SCD Type I and SDC Type 2 use cases, you can use "2. SCDs In DLT Pipeline: Use CDC data (Python)" and create a new DLT pipeline based on this notebook to explore Apply Changes Into API in DLT.
- If you are interested to analyze the logs populated from the DLT pipeline you can find an example notebook "3. Log Analysis" and use the storage location you defined in your DLT pipeline to analyze it's logs.
- Refer to "4. Retail Sales" notebook if you are interested in creating your DLT pipeline using Python APIs. 