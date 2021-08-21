** Challenge Steps

- [ ] ETL JOB. Create a Python compute job that runs on a daily schedule. You could do this by creating a Python Lambda function, and then triggering it from a once-daily CloudWatch rule. Alternatively, you could create a scheduled Fargate task, or look into scheduling a job using AWS Glue. The only requirement is that the underlying compute must be triggered once daily, not on a continuously polling server.

- [x] EXTRACTION. In your Python code, download this CSV file from Github. (This is a daily dump of US COVID-19 data from a repository maintained by the New York Times. Every day, the file updates with an additional row of data.) Load the data into an object in memory.

- [x] TRANSFORMATION. Perform data manipulations in Python code.

    - [x] Cleaning – The date field should be converted to a date object, not a string.

    - [x] Joining – We want to show recovered cases as well as confirmed cases and deaths. The NYT data does not track recoveries, so you will need to pull US recovery data from this Johns Hopkins dataset and merge it into your record for each day. Note: the case and death counts for the Johns Hopkins dataset disagree with the NYT data. We will treat the NYT data as authoritative and only copy the recovery data from Johns Hopkins.)

    - [x] Filtering – Remove non-US data from the Johns Hopkins dataset. Remove any days that do not exist in both datasets. (There is an off-by-one issue.)

- [x] CODE CLEANUP. Abstract your data manipulation work into a Python module. This module should only perform transformations. It should not care where the CSV files are stored and it should not know anything about the database in the next step.

- [ ] LOAD. Now, write code to load your transformed data into a database. For the purposes of this exercise, you can use any database you choose. I suggest using DynamoDB with boto3 or RDS Postgres with pyscopg. Either way, you’ll want each record in the table to have the date, US case count, deaths, and recoveries for a day of the pandemic.

- [ ] NOTIFICATION. When the database has been updated, your code should trigger an SNS message to notify any interested consumers that the ETL job has completed. The message should include the number of rows updated in the database.

- [ ] ERROR HANDLING. Your code should be able to handle these common control flow situations:

    - [ ] Initial load vs update — you should be able to load the entire historical data set into your database the first time the job is run, and then update with only the most recent day’s data thereafter.

    - [ ] If the data contains unexpected or malformed input, your code should fail gracefully and report an error message via SNS. Next time your job runs, it should remember that it did not succeed in processing the previous data, and try again before moving on to more recent data.

- [ ] TESTS. To ensure that your code can handle unexpected situations, include unit tests for your code that substitute invalid data for the COVID-19 CSV files, and confirm that your code responds correctly.

- [ ] IaC. Make sure your infrastructure (Lambda function, CloudWatch rule, SNS trigger, database, etc) is defined in code (CloudFormation, Terraform, or similar)

- [ ] SOURCE CONTROL. Store your code and config in source control (GitHub, or similar)

- [ ] DASHBOARD. What would an ETL process be without a report? See if you can hook your database up to AWS Quicksight or another BI service like Tableau to generate a visualization of US case counts, fatality, and recoveries over time.

- [ ] BLOG POST. (very important) Write a short blog post explaining your learnings and your approach to the challenge. Link to your data visualization so we can try it out! If you do not have your own blog, Hashnode or dev.to is a great place to start.
