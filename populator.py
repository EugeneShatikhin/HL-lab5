from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import lit, col, rand, when
from math import sin, cos, sqrt, atan2
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import math
import random
from functools import reduce

def generate_normal_distribution(mean, std_dev):
    u = 1 - random.random()  # Converting [0,1) to (0,1]
    v = random.random()
    z = math.sqrt(-2.0 * math.log(u)) * math.cos(2.0 * math.pi * v)
    return z * std_dev + mean

driver_positive_feedbacks = [
    "Fast ride",
    "Polite driver",
    "Clean car",
    "Comfortable car",
    "Excellent service",
    "Great communication",
    "Prompt arrival",
]

driver_negative_feedbacks = [
    "Late arrival",
    "Rude driver",
    "Dirty car",
    "Uncomfortable ride",
    "Poor service",
    "Unsafe driving",
    "Ignored instructions",
    "Overcharged",
]

passenger_positive_feedbacks = [
    "Polite & friendly",
    "Pleasant conversation",
    "Clear instructions",
    "In time for pickup",
    "Clear car",
]

passenger_negative_feedbacks = [
    "Late for pickup",
    "Impolite and unfriendly behavior",
    "Messy and dirty in the car",
    "Didn't follow safety guidelines",
    "Provided unclear destination instructions",
    "Disruptive during the ride",
]

def generate_random_rating(skew):
    base_rating = random.random() * skew
    rand = generate_normal_distribution(3, 2)
    skewed_rating = abs(rand + base_rating)  # Calculate the skewed rating
    return min(5, skewed_rating)  # Ensure the rating is not greater than 5

def generate_feedback(is_driver, feedback_probability, skew=2):
    rating = generate_random_rating(skew)

    

    if 4.5 <= rating:
        num_feedbacks = 3
    elif 3.5 <= rating < 4.5:
        num_feedbacks = 2
    elif 2.5 <= rating < 3.5:
        num_feedbacks = 1
    elif 1.5 <= rating < 2.5:
        num_feedbacks = 2
    else:
        num_feedbacks = 3

    feedback_list = (
        driver_positive_feedbacks if is_driver and rating > 3
        else passenger_positive_feedbacks if not is_driver and rating > 3
        else driver_negative_feedbacks if is_driver
        else passenger_negative_feedbacks
    )

    selected_feedbacks = random.sample(feedback_list, num_feedbacks)

    if random.random() < feedback_probability:
        return {
            "Rating": np.round(rating, 0),
            "Notes": selected_feedbacks
        }
    else:
        return {
            "Rating": None,
            "Notes": []
        }

def calculate_distance(lat1, lon1, lat2, lon2):
    # Haversine formula to calculate distance between two points
    R = 6371  # Radius of Earth in kilometers
    d_lat = (lat2 - lat1) * (3.14159 / 180)
    d_lon = (lon2 - lon1) * (3.14159 / 180)
    a = (
        pow(sin(d_lat / 2), 2)
        + cos(lat1 * (3.14159 / 180)) * cos(lat2 * (3.14159 / 180)) * pow(sin(d_lon / 2), 2)
    )
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = R * c  # Distance in kilometers
    return distance

def calculate_time_to_reach_destination(distance, speed):
    # Calculate time (in hours) to reach the destination at the given speed
    time_in_hours = distance / speed
    return time_in_hours * 60 * 60 * 1000  # Convert hours to milliseconds

def generate_route(start_date_time, start_lat, start_lon, end_lat, end_lon, interval_seconds, speed):
    route = []

    end_date_time = start_date_time + timedelta(
        milliseconds=calculate_time_to_reach_destination(
            calculate_distance(start_lat, start_lon, end_lat, end_lon), speed
        )
    )

    route.append({
        "Location": {
            "type": "Point",
            "coordinates": [start_lon, start_lat],
        },
        "DateTime": start_date_time,
    })

    route.append({
        "Location": {
            "type": "Point",
            "coordinates": [end_lon, end_lat],
        },
        "DateTime": end_date_time,
    })

    return route

# Initialize Spark session
spark = SparkSession.builder.appName("LondonTaxiSimulation").getOrCreate()

postcodes = spark.read.option('header','true').csv('/content/drive/MyDrive/Colab Notebooks/London postcodes.csv')
# postcodes = ((postcodes.unionAll(postcodes)).unionAll(postcodes)).unionAll(postcodes)
# print(pos tcodes.count())
postcodes.cache()

text_comments = spark.read.option('header','true').csv('/content/drive/MyDrive/Colab Notebooks/Uber_Ride_Reviews.csv')
text_comments.cache()

text_reviews_1 = text_comments.filter(text_comments.ride_rating == 1.0).collect()
text_reviews_2 = text_comments.filter(text_comments.ride_rating == 2.0).collect()
text_reviews_3 = text_comments.filter(text_comments.ride_rating == 3.0).collect()
text_reviews_4 = text_comments.filter(text_comments.ride_rating == 4.0).collect()
text_reviews_5 = text_comments.filter(text_comments.ride_rating == 5.0).collect()


# df.select("Postcode", "Latitude", "Longitude", "District").show()

# spark.stop()

debug = False

orders_to_create = 100000

drivers = 3000
driverFeedbackProb = 0.7
driverFeedbackCommentProb = 0.1

passengers = 5000
passengerFeedbackProb = 0.3

discreteTimeSeconds = 5
taxiAvgSpeed = 30
minDistance = 0.5

meanHour = 18
stdDevHour = 3


def generateRandomDateTime(meanHour, stdDevHour):
    randomHour = generate_normal_distribution(meanHour % 24, stdDevHour) % 24
    randomMinutes = random.randint(0, 59)
    randomSeconds = random.randint(0, 59)

    dateTime = datetime.now()
    dateTime = dateTime.replace(
        hour=int(randomHour), minute=randomMinutes, second=randomSeconds
    )

    return dateTime



def create_orders():
    orders = []
    read_rows = postcodes.orderBy(rand()).limit(2 * orders_to_create)
    read_rows.cache()
    read_rows_list = read_rows.collect()
    for j in range(orders_to_create):
    # for j in range(2):
        start_point = read_rows_list[j]
        # print("j + orders_to_create: ", j + orders_to_create)
        end_point = read_rows_list[j + orders_to_create]
        distance = calculate_distance(
        float(start_point["Latitude"]),
        float(start_point["Longitude"]),
        float(end_point["Latitude"]),
        float(end_point["Longitude"])
        )

        # if distance is less than permitted, resample
        while distance < minDistance:
            index = random.randint(0, int(read_rows.count()) - 1)
            while index == j:
                index = random.randint(0, int(read_rows.count()) - 1)
            end_point = read_rows_list[index]

            distance = calculate_distance(
            float(start_point["Latitude"]),
            float(start_point["Longitude"]),
            float(end_point["Latitude"]),
            float(end_point["Longitude"])
            )
            if debug:
                print(f"Resampled j={j} to index={index}, distance={distance}")

        # get driver
        driverId = random.randint(0, drivers - 1)
        driverFeedback = generate_feedback(True, driverFeedbackProb)
        # get passenger
        passengerId = random.randint(0, passengers - 1)
        passengerFeedback = generate_feedback(False, passengerFeedbackProb)
        # get time of ride
        time = generateRandomDateTime(meanHour, stdDevHour)
        # get route
        route = generate_route(
            time,
            float(start_point["Latitude"]),
            float(start_point["Longitude"]),
            float(end_point["Latitude"]),
            float(end_point["Longitude"]),
            discreteTimeSeconds,
            taxiAvgSpeed
        )

        driver_rating = driverFeedback["Rating"]

        if (random.random() < driverFeedbackCommentProb and (len(text_reviews_1) > 0 or len(text_reviews_2) > 0 or len(text_reviews_3) > 0 or len(text_reviews_4) > 0 or len(text_reviews_5) > 0)):
            if driver_rating == 1.0:
                driverFeedbackComment = random.choice(text_reviews_1)["ride_review"]
            elif driver_rating == 2.0:
                driverFeedbackComment = random.choice(text_reviews_2)["ride_review"]
            elif driver_rating == 3.0:
                driverFeedbackComment = random.choice(text_reviews_3)["ride_review"]
            elif driver_rating == 4.0:
                driverFeedbackComment = random.choice(text_reviews_4)["ride_review"]
            elif driver_rating == 5.0:
                driverFeedbackComment = random.choice(text_reviews_5)["ride_review"]
            else:  driverFeedbackComment = ''
        else: driverFeedbackComment = ''

        # get duration
        duration = (route[-1]["DateTime"] - route[0]["DateTime"])  # in seconds
        # get price
        if (int(route[0]["DateTime"].hour) >= meanHour - stdDevHour & int(route[0]["DateTime"].hour) < meanHour + stdDevHour):
            price = duration.total_seconds() * 0.03
        else:
            price = duration.total_seconds() * 0.02

        # compose row
        result = [
            driverId,
            driverFeedback["Rating"],
            driverFeedback["Notes"],
            driverFeedbackComment,
            passengerId,
            passengerFeedback["Rating"],
            passengerFeedback["Notes"],
            [start_point["Longitude"], start_point["Latitude"]],
            route[0]["DateTime"],
            [end_point["Longitude"], end_point["Latitude"]],
            route[-1]["DateTime"],
            distance,
            duration,
            np.round(float(price), 2)
        ]
        orders.append(result)
    read_rows.unpersist()
    return orders

# try:
print("Generating the data...")
orders = create_orders()

# print(orders)
# print("debug 1")
headers = [
        'DriverId',
        'DriverFeedbackRating',
        'DriverFeedbackNotes',
        'DriverFeedbackComment',
        'PassengerId',
        'PassengerFeedbackRating',
        'PassengerFeedbackNotes',
        'DepartureLocation',
        'DepartureTimestamp',
        'DestinationLocation',
        'DestinationTimestamp',
        'Distance',
        'Duration',
        'Price'
            ]

df = pd.DataFrame(data=orders,columns=headers)
orders_df = spark.createDataFrame(df)

print("The data is generated and put into a dataframe!")

    # result_df.repartition(1).write.mode('overwrite').csv("./output/", header='True')
    # result_df.coalesce(1).write.format("csv").option("header",True).option("sep","|").save("./output/")

# except Exception as e:
#     print(f"An error occurred: {e}")

# finally:
#     print('End of session')
#     spark.stop()

######################## VARIANT 3 #########################

hour = functions.udf(lambda x: int(x.hour))
report1_df = orders_df.withColumn("DepartureHour", hour("DepartureTimestamp"))
# truncate=False

num_of_drivers = orders_df.agg(functions.countDistinct("DriverId")).withColumnRenamed('count(DISTINCT DriverId)', 'Number of drivers')
num_of_drivers.cache()
num_of_drivers.show()

num_of_passengers = orders_df.agg(functions.countDistinct("PassengerId")).withColumnRenamed('count(DISTINCT PassengerId)', 'Number of passengers')
num_of_passengers.cache()
num_of_passengers.show()

report1_df = report1_df.groupBy("DepartureHour").count().withColumnRenamed('count', 'CountByDepartureHour')
# report1_df.show()
report1_df = report1_df.orderBy(col("CountByDepartureHour").desc()).limit(1).withColumnRenamed('DepartureHour', 'Most popular departure hour')
report1_df.cache()
report1_df.show()

######################## VARIANT 1 #########################

num_of_drivers = orders_df.agg(functions.countDistinct("DriverId")).withColumnRenamed('count(DISTINCT DriverId)', 'Number of drivers')
num_of_drivers.cache()
num_of_drivers.show()

num_of_passengers = orders_df.agg(functions.countDistinct("PassengerId")).withColumnRenamed('count(DISTINCT PassengerId)', 'Number of passengers')
num_of_passengers.cache()
num_of_passengers.show()

report2_df = orders_df.dropna().groupBy("DriverId").mean("DriverFeedbackRating").select("*",functions.round("avg(DriverFeedbackRating)",2)).withColumnRenamed('round(avg(DriverFeedbackRating), 2)', 'Average driver rating').drop('avg(DriverFeedbackRating)')
# report1_df.show()
report2_df = report2_df.orderBy(col("Average driver rating").desc()).limit(100).withColumnRenamed('DriverId', 'Top 100 best rated drivers')
report2_df.cache()
report2_df.show(100)

orders_df.show(50)

text_comments.show(10,False)