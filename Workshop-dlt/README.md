# Data Engineering Zoomcamp Cohort 2.024 - Workshop 1
# Data ingestion with dlt

​In this hands-on workshop, we’ll learn how to build data ingestion pipelines.

​We’ll cover the following steps:

* ​Extracting data from APIs, or files.
* ​Normalizing and loading data
* ​Incremental loading

​By the end of this workshop, you’ll be able to write data pipelines like a senior data engineer: Quickly, concisely, scalable, and self-maintaining.

Video: https://www.youtube.com/live/oLXhBM7nf2Q

--- 
## Resources
- Website and community: Visit our [docs](https://dlthub.com/docs/intro), discuss on our slack (Link at top of docs).
- dlthub [community Slack](https://dlthub.com/community).


# Course
The course has 3 parts
- Extraction Section
- Normalisation Section
- Loading Section

---

# Homework

The [Colab notebook with my solutions](./homework_dlt_workshop.ipynb). You can go over it to get the answers.


#### Question 1: What is the sum of the outputs of the generator for limit = 5?
- **A**: 10.23433234744176
- **B**: 7.892332347441762
- **C**: 8.382332347441762
- **D**: 9.123332347441762

**Answer**: C - 8.38

#### Question 2: What is the 13th number yielded by the generator?
- **A**: 4.236551275463989
- **B**: 3.605551275463989
- **C**: 2.345551275463989
- **D**: 5.678551275463989

**Answer**: B - 3.60

#### Question 3: Append the 2 generators. After correctly appending the data, calculate the sum of all ages of people.
- **A**: 353
- **B**: 365
- **C**: 378
- **D**: 390

**Answer**: A - 353

#### Question 4: Merge the 2 generators using the ID column. Calculate the sum of ages of all the people loaded as described above.
- **A**: 215
- **B**: 266
- **C**: 241
- **D**: 258

**Answer**: B - 266

--- 
# Next steps

As you are learning the various concepts of data engineering, 
consider creating a portfolio project that will further your own knowledge.

By demonstrating the ability to deliver end to end, you will have an easier time finding your first role. 
This will help regardless of whether your hiring manager reviews your project, largely because you will have a better 
understanding and will be able to talk the talk.

Here are some example projects that others did with dlt:
- Serverless dlt-dbt on cloud functions: [Article](https://docs.getdbt.com/blog/serverless-dlt-dbt-stack)
- Bird finder: [Part 1](https://publish.obsidian.md/lough-on-data/blogs/bird-finder-via-dlt-i), [Part 2](https://publish.obsidian.md/lough-on-data/blogs/bird-finder-via-dlt-ii)
- Event ingestion on GCP: [Article and repo](https://dlthub.com/docs/blog/streaming-pubsub-json-gcp)
- Event ingestion on AWS: [Article and repo](https://dlthub.com/docs/blog/dlt-aws-taktile-blog)
- Or see one of the many demos created by our working students: [Hacker news](https://dlthub.com/docs/blog/hacker-news-gpt-4-dashboard-demo), 
[GA4 events](https://dlthub.com/docs/blog/ga4-internal-dashboard-demo), 
[an E-Commerce](https://dlthub.com/docs/blog/postgresql-bigquery-metabase-demo), 
[google sheets](https://dlthub.com/docs/blog/google-sheets-to-data-warehouse-pipeline), 
[Motherduck](https://dlthub.com/docs/blog/dlt-motherduck-demo), 
[MongoDB + Holistics](https://dlthub.com/docs/blog/MongoDB-dlt-Holistics), 
[Deepnote](https://dlthub.com/docs/blog/deepnote-women-wellness-violence-tends), 
[Prefect](https://dlthub.com/docs/blog/dlt-prefect),
[PowerBI vs GoodData vs Metabase](https://dlthub.com/docs/blog/semantic-modeling-tools-comparison),
[Dagster](https://dlthub.com/docs/blog/dlt-dagster),
[Ingesting events via gcp webhooks](https://dlthub.com/docs/blog/dlt-webhooks-on-cloud-functions-for-event-capture),
[SAP to snowflake replication](https://dlthub.com/docs/blog/sap-hana-to-snowflake-demo-blog),
[Read emails and send sumamry to slack with AI and Kestra](https://dlthub.com/docs/blog/dlt-kestra-demo-blog),
[Mode +dlt capabilities](https://dlthub.com/docs/blog/dlt-mode-blog),
[dbt on cloud functions](https://dlthub.com/docs/blog/dlt-dbt-runner-on-cloud-functions)
- If you want to use dlt in your project, [check this list of public APIs](https://dlthub.com/docs/blog/practice-api-sources)


If you create a personal project, consider submitting it to our blog - we will be happy to showcase it. Just drop us a line in the dlt slack.
