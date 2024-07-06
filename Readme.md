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

            for fixing this issue we need to import some packages:
![image](https://github.com/Lxrocky/project/assets/164576033/2733e82e-b274-4425-bece-249944e93a9b)

            now we need to write a schema code for updating the datatype
![image](https://github.com/Lxrocky/project/assets/164576033/7bc9a0ae-fa65-4b61-8899-299d47332804)

            Again writing the code for showing the datatype
             As we can see that the datatype is changed.
![image](https://github.com/Lxrocky/project/assets/164576033/966043c8-c741-4a4f-a691-49fb50b6e1cb)

            (ii) match

![image](https://github.com/Lxrocky/project/assets/164576033/1e4782f8-998f-4556-b601-bd9dd48e81fa)

            (iii) player match 
![image](https://github.com/Lxrocky/project/assets/164576033/c59843da-c487-4bdc-abbd-09413edd4bbd)

            (iv) player
![image](https://github.com/Lxrocky/project/assets/164576033/9aac2dba-9525-416a-8eeb-2664365793d1)

            (v) team
![image](https://github.com/Lxrocky/project/assets/164576033/befb32a0-33f5-4d0c-87aa-d5c09a22e633)

Now we can add filter
            # Filter to include only valid deliveries (excluding extras like wides and no balls for specific analyses)
            ball_by_ball_df = ball_by_ball_df.filter((col("wides") == 0) & (col("noballs")==0))

            # Aggregation: Calculate the total and average runs scored in each match and inning
            total_and_avg_runs = ball_by_ball_df.groupBy("match_id", "innings_no").agg(
                sum("runs_scored").alias("total_runs"),
                avg("runs_scored").alias("average_runs")
            )

![image](https://github.com/Lxrocky/project/assets/164576033/1218d8bc-3486-40e6-af7e-073a0e1d1332)


# Window Function: Calculate running total of runs in each match for each over

![image](https://github.com/Lxrocky/project/assets/164576033/7e4a3f7d-7b3f-4603-bab2-456441bafebc)

 # Conditional Column: Flag for high impact balls (either a wicket or more than 6 runs including extras)
 
 ![image](https://github.com/Lxrocky/project/assets/164576033/a60f7e59-9c50-4a61-945a-00a779ee5649)

#

![image](https://github.com/Lxrocky/project/assets/164576033/441cc5fb-c7e8-4dfa-94d0-24670c23e13e)

#


![image](https://github.com/Lxrocky/project/assets/164576033/68431920-e218-484a-af51-dcc7ab3ccfeb)
#







            


            

            


          






















      



