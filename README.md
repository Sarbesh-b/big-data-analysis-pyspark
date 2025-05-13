# Big Data Analysis with PySpark

This project consists of four separate tasks analyzing large-scale real-world datasets using Apache Spark. It was developed as part of my coursework project for the Big Data Processing module in the MSc Big Data Science program at Queen Mary University of London.

Each task explores a unique dataset and uses Spark’s distributed processing capabilities to extract insights, perform transformations, and generate visualizations using PySpark, Spark SQL, and Pandas.

---

##  Project Structure

The repository is organized by task:

- `task1_twitter_hashtags/`  
  ➤ Extracted hashtags from tweet data, performed frequency analysis, and visualized trending topics.

- `task2_movie_ratings/`  
  ➤ Analyzed movie ratings data to compute average ratings by genre, user patterns, and rating distributions.

- `task3_taxi_trips/`  
  ➤ Explored Chicago taxi trip data to uncover trends in distance, fare amounts, and trip durations.

- `task4_hdfs_logs/`  
  ➤ Processed large HDFS logs to extract operation-level insights and identify storage access patterns.

---

##  Tools & Technologies

- Apache Spark (PySpark)
- Spark SQL
- Python, Pandas
- Jupyter Notebooks
- Matplotlib, Seaborn (for visualizations)

---

##  Project Highlights

- Worked with large datasets using distributed computing on Spark.
- Applied transformations using RDDs and DataFrames.
- Performed aggregations, joins, filters, and time-based analysis.
- Visualized key insights using Matplotlib and Seaborn.

---

##  Data Access Note

> ⚠️ **Note:**  
The datasets used in this project were stored on a shared Spark cluster (JupyterHub) provided by Queen Mary University of London. Due to access restrictions, these datasets are not included in this repository.  
The notebooks are provided to demonstrate the analysis pipeline and Spark logic.

---

##  How to Use

These notebooks are meant for review and reference. To run them:
1. Set up Apache Spark and Jupyter locally or in a cloud environment.
2. Modify the file paths to point to your local or accessible data.
3. Run the `.ipynb and .py` notebooks inside each task folder.

---



## Sample Output
**Tweet Distribution by Geography and Time of Day**
 ![USA Map](https://github.com/user-attachments/assets/7eceb92c-c4c8-40c9-a85e-0597eced86fc)
 ![Tweet Count by Hour](https://github.com/user-attachments/assets/047581b7-e688-409b-940c-3cada0347516)

 **Genre and Temporal Trends in Movie Ratings**
 ![Ratings by time of the year](https://github.com/user-attachments/assets/9500c6f2-7613-4b5a-9573-952fce5636d0)
 ![Genre Distribution](https://github.com/user-attachments/assets/2cf3048c-d739-4d46-855f-6dae9b221a45)

 

##  Author

**Sarbesh Bhatta**  
MSc Big Data Science — Queen Mary University of London  
[LinkedIn](https://www.linkedin.com/in/sarbeshb7) | 
