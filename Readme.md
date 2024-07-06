First we need to create an databricks account (the community edition)

**Then first we need to create a compute**

![image](https://github.com/Lxrocky/project/assets/164576033/dc24aabf-79ff-4d98-8303-26a210eeaa04)

Now we can see the copute is in running state

![image](https://github.com/Lxrocky/project/assets/164576033/f0c12509-47fe-444e-a3cf-c3c86fa3470d)

Now we need to create a Notebook as "IPL-data-analytics-2024-07-06 11:44:28"

![image](https://github.com/Lxrocky/project/assets/164576033/2e784521-8ed4-4c5f-9c54-4f10f7d1311e)

We need to select the compute service here
![image](https://github.com/Lxrocky/project/assets/164576033/078e9117-4c0c-47b7-9129-3b9d8d657d9f)

First we need to check spark is available or not writing on code written section as "spark"

![image](https://github.com/Lxrocky/project/assets/164576033/bc92b38a-8d8d-44df-9e85-e83ac7fb0ba7)

Now we need create our own spark session
![image](https://github.com/Lxrocky/project/assets/164576033/0fa72d23-dbbf-4ce6-82a8-af3f1efb5460)

we need to read one by one so
      (i) ball_by_ball
      ![image](https://github.com/Lxrocky/project/assets/164576033/e53342c6-b06b-4eea-a87a-2355291eb5f3)

      Here we can see there are different types of value which is not similar the actual source so we need to fix this in different steps:
          (a) writing the code as:  ball_by_ball_df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("s3://ipl-data-analysics/Ball_By_Ball.csv")
             the data type updated but this is not actual data type.
![Screenshot 2024-07-06 124106](https://github.com/Lxrocky/project/assets/164576033/45106163-3212-4005-b08b-cfb1c19f7068)

            


          






















      



