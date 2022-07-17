from pyspark.sql import SparkSession
import findspark
import logging
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField

logger = logging.getLogger(__name__)

findspark.init()


def create_spark_session():
    sp = SparkSession.builder \
        .master('local[*]') \
        .appName('NewYorker') \
        .getOrCreate()
    return sp


def load_tip(spark) -> None:
    """
     read json tip data from file and write them into a parquet file.

    :param spark: SparkSession
    :return: None
    """
    tip = spark.read \
        .option('inferSchema', True) \
        .json('archive/yelp_academic_dataset_tip.json') \
        .select(trim(col('business_id')).alias('business_id'),
                col('compliment_count'),
                trim(col('date')).alias('date'),
                col('text'),
                trim(col('user_id')).alias('user_id'))

    tip.write \
        .format('parquet') \
        .mode('overwrite') \
        .save('out_put_raw/tip')


def load_business(spark) -> None:
    """
    read json business data and write them into a parquet file.
    and turn every nested json field into a column for easier use in the future
    and clean extra charachters from fields

    :param spark: SparkSession
    :return: None
    """
    business_1 = spark.read \
        .option('inferSchema', True) \
        .json('archive/yelp_academic_dataset_business.json') \
        .select(trim(col('business_id')).alias('business_id'),
                trim(col('city')).alias('city'),
                trim(col('name')).alias('name'),
                trim(col('postal_code')).alias('postal_code'),
                trim(col('state')).alias('state'),
                trim(col('address')).alias('address'),
                col('categories'),
                col('is_open'),
                col('latitude'),
                col('longitude'),
                col('review_count'),
                col('stars'),
                col('attributes').getItem('AcceptsInsurance').alias('AcceptsInsurance'),
                col('attributes').getItem('AgesAllowed').alias('AgesAllowed'),
                regexp_replace(regexp_replace(col('attributes').getItem('Alcohol'), "u'", ""), "'", "").alias(
                    'Alcohol'),
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                col('attributes').getItem('Ambience'), "'", '"'), 'False', '"False"'),
                        'True', '"True"'), 'None', '"False"').alias('Ambience'),
                col('attributes').getItem('BYOB').alias('BYOB'),
                col('attributes').getItem('BYOBCorkage').alias('BYOBCorkage'),
                col('attributes').getItem('BestNights').alias('BestNights'),
                col('attributes').getItem('BikeParking').alias('BikeParking'),
                col('attributes').getItem('BusinessAcceptsBitcoin').alias('BusinessAcceptsBitcoin'),
                col('attributes').getItem('BusinessAcceptsCreditCards').alias('BusinessAcceptsCreditCards'),
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                col('attributes').getItem('BusinessParking'), "'", '"'), 'False', '"False"'),
                        'True', '"True"'), 'None', '"False"').alias('BusinessParking'),
                col('attributes').getItem('ByAppointmentOnly').alias('ByAppointmentOnly'),
                col('attributes').getItem('Caters').alias('Caters'),
                col('attributes').getItem('CoatCheck').alias('CoatCheck'),
                col('attributes').getItem('Corkage').alias('Corkage'),
                col('attributes').getItem('DietaryRestrictions').alias('DietaryRestrictions'),
                col('attributes').getItem('DogsAllowed').alias('DogsAllowed'),
                col('attributes').getItem('DriveThru').alias('DriveThru'),
                col('attributes').getItem('GoodForDancing').alias('GoodForDancing'),
                col('attributes').getItem('GoodForKids').alias('GoodForKids'),
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                col('attributes').getItem('GoodForMeal'), "'", '"'), 'False', '"False"'),
                        'True', '"True"'), 'None', '"False"').alias('GoodForMeal'),
                col('attributes').getItem('HairSpecializesIn').alias('HairSpecializesIn'),
                col('attributes').getItem('HappyHour').alias('HappyHour'),
                col('attributes').getItem('HasTV').alias('HasTV'),
                col('attributes').getItem('Music').alias('Music'),
                regexp_replace(regexp_replace(col('attributes').getItem('NoiseLevel'), "u'", ""), "'", "").alias(
                    'NoiseLevel'),
                col('attributes').getItem('Open24Hours').alias('Open24Hours'),
                col('attributes').getItem('OutdoorSeating').alias('OutdoorSeating'),
                regexp_replace(regexp_replace(col('attributes').getItem('RestaurantsAttire'), "u'", ""), "'", "").alias(
                    'RestaurantsAttire'),
                col('attributes').getItem('RestaurantsCounterService').alias('RestaurantsCounterService'),
                col('attributes').getItem('RestaurantsDelivery').alias('RestaurantsDelivery'),
                col('attributes').getItem('RestaurantsGoodForGroups').alias('RestaurantsGoodForGroups'),
                col('attributes').getItem('RestaurantsPriceRange2').alias('RestaurantsPriceRange2'),
                col('attributes').getItem('RestaurantsReservations').alias('RestaurantsReservations'),
                col('attributes').getItem('RestaurantsTableService').alias('RestaurantsTableService'),
                col('attributes').getItem('RestaurantsTakeOut').alias('RestaurantsTakeOut'),
                col('attributes').getItem('Smoking').alias('Smoking'),
                col('attributes').getItem('WheelchairAccessible').alias('WheelchairAccessible'),
                regexp_replace(regexp_replace(col('attributes').getItem('WiFi'), "u'", ""), "'", "").alias('WiFi'),
                col('hours').getItem('Monday').alias('hours_Monday'),
                col('hours').getItem('Tuesday').alias('hours_Tuesday'),
                col('hours').getItem('Wednesday').alias('hours_Wednesday'),
                col('hours').getItem('Thursday').alias('hours_Thursday'),
                col('hours').getItem('Friday').alias('hours_Friday'),
                col('hours').getItem('Saturday').alias('hours_Saturday'),
                col('hours').getItem('Sunday').alias('hours_Sunday')) \
        .na.fill('0')

    BusinessParking_schema = StructType([StructField("garage", StringType(), True),
                                         StructField("street", StringType(), True),
                                         StructField("validated", StringType(), True),
                                         StructField("lot", StringType(), True),
                                         StructField("valet", StringType(), True)],
                                        )

    Ambience_schema = StructType([StructField("romantic", StringType(), True),
                                  StructField("intimate", StringType(), True),
                                  StructField("touristy", StringType(), True),
                                  StructField("hipster", StringType(), True),
                                  StructField("divey", StringType(), True),
                                  StructField("classy", StringType(), True),
                                  StructField("trendy", StringType(), True),
                                  StructField("upscale", StringType(), True),
                                  StructField("casual", StringType(), True)],
                                 )

    GoodForMeal_schema = StructType([StructField("dessert", StringType(), True),
                                     StructField("latenight", StringType(), True),
                                     StructField("lunch", StringType(), True),
                                     StructField("dinner", StringType(), True),
                                     StructField("brunch", StringType(), True),
                                     StructField("breakfast", StringType(), True)],
                                    )

    business = business_1 \
        .withColumn('BusinessParking', from_json(col("BusinessParking"), BusinessParking_schema)) \
        .withColumn('Ambience', from_json(col("Ambience"), Ambience_schema)) \
        .withColumn('GoodForMeal', from_json(col("GoodForMeal"), GoodForMeal_schema)) \
        .withColumn('BusinessParking_garage', col('BusinessParking').getItem('garage')) \
        .withColumn('BusinessParking_street', col('BusinessParking').getItem('street')) \
        .withColumn('BusinessParking_validated', col('BusinessParking').getItem('validated')) \
        .withColumn('BusinessParking_lot', col('BusinessParking').getItem('lot')) \
        .withColumn('BusinessParking_valet', col('BusinessParking').getItem('valet')) \
        .withColumn('Ambience_romantic', col('Ambience').getItem('romantic')) \
        .withColumn('Ambience_intimate', col('Ambience').getItem('intimate')) \
        .withColumn('Ambience_touristy', col('Ambience').getItem('touristy')) \
        .withColumn('Ambience_hipster', col('Ambience').getItem('hipster')) \
        .withColumn('Ambience_divey', col('Ambience').getItem('divey')) \
        .withColumn('Ambience_classy', col('Ambience').getItem('classy')) \
        .withColumn('Ambience_trendy', col('Ambience').getItem('trendy')) \
        .withColumn('Ambience_upscale', col('Ambience').getItem('upscale')) \
        .withColumn('Ambience_casual', col('Ambience').getItem('casual')) \
        .withColumn('GoodForMeal_dessert', col('GoodForMeal').getItem('dessert')) \
        .withColumn('GoodForMeal_latenight', col('GoodForMeal').getItem('latenight')) \
        .withColumn('GoodForMeal_lunch', col('GoodForMeal').getItem('lunch')) \
        .withColumn('GoodForMeal_dinner', col('GoodForMeal').getItem('dinner')) \
        .withColumn('GoodForMeal_brunch', col('GoodForMeal').getItem('brunch')) \
        .withColumn('GoodForMeal_breakfast', col('GoodForMeal').getItem('breakfast')) \
        .drop('BusinessParking') \
        .drop('Ambience') \
        .drop('GoodForMeal')

    business.write \
        .format('parquet') \
        .mode('overwrite') \
        .save('out_put_raw/business')

    # /////////////////////////////////// business_categories ////////////////////////////////

    business_categories = business_1.select(explode(split(col('categories'), ',')).alias('categories')) \
        .withColumn('categories', trim(col('categories'))) \
        .distinct()

    business_categories.write \
        .format('parquet') \
        .mode('overwrite') \
        .save('out_put_raw/business_categories')

    # /////////////////////////////////// postal_code ////////////////////////////////

    business_postal_code = business_1.select(col('postal_code')).distinct()

    business_postal_code.write \
        .format('parquet') \
        .mode('overwrite') \
        .save('out_put_raw/business_postal_code')

    # /////////////////////////////////// business_city ////////////////////////////////

    business_city = business_1.select(col('city')).distinct()

    business_city.write \
        .format('parquet') \
        .mode('overwrite') \
        .save('out_put_raw/business_city')

    # /////////////////////////////////// business_state ////////////////////////////////

    business_state = business_1.select(col('state')).distinct()

    business_state.write \
        .format('parquet') \
        .mode('overwrite') \
        .save('out_put_raw/business_state')

    # ////////////////////////////////////////////////////////////////////////////////////////////


def load_checkin(spark) -> None:
    """
     read json checkin data from file and write them into a parquet file and turn the data to row data.

    :param spark: SparkSession
    :return: None
    """
    checkin = spark.read \
        .option('inferSchema', True) \
        .json('archive/yelp_academic_dataset_checkin.json') \
        .select(trim(col('business_id')).alias('business_id'),
                explode(split(col('date'), ',')).alias('date')) \
        .withColumn('date', trim(col('date')))

    checkin.write \
        .format('parquet') \
        .mode('overwrite') \
        .save('out_put_raw/checkin')


def load_review(spark) -> None:
    """
     read json review data from file and write them into a parquet file.

    :param spark: SparkSession
    :return: None
    """
    review = spark.read \
        .option('inferSchema', True) \
        .json('archive/yelp_academic_dataset_review.json') \
        .select(trim(col('business_id')).alias('business_id'),
                col('cool'),
                trim(col('date')).alias('date'),
                col('funny'),
                trim(col('review_id')).alias('review_id'),
                col('stars'),
                col('text'),
                col('useful'),
                trim(col('user_id')).alias('user_id'))

    review.write \
        .format('parquet') \
        .mode('overwrite') \
        .save('out_put_raw/review')


def load_user(spark) -> None:
    """
     read json user data from file and write them into a parquet file. I create another dataset based on user friends.
     If we just need the user's information we can use the user dataset only and if we need to have user's friends
     we can join with friends dataset.
     Also the friends dataset is a useful one to use in graph tools for creating a graph of people and
     the relation between them.

    :param spark: SparkSession
    :return: None
    """
    user = spark.read \
        .option('inferSchema', True) \
        .json('archive/yelp_academic_dataset_user.json') \
        .select(col('average_stars'),
                col('compliment_cool'),
                col('compliment_cute'),
                col('compliment_funny'),
                col('compliment_hot'),
                col('compliment_list'),
                col('compliment_more'),
                col('compliment_note'),
                col('compliment_photos'),
                col('compliment_plain'),
                col('compliment_profile'),
                col('compliment_writer'),
                col('cool'),
                trim(col('elite')).alias('elite'),
                col('fans'),
                col('funny'),
                trim(col('name')).alias('name'),
                col('review_count'),
                col('useful'),
                trim(col('user_id')).alias('user_id'),
                trim(col('yelping_since')).alias('yelping_since'),
                col('friends'))

    user_friends = user \
        .select(col('user_id'),
                explode(split(col('friends'), ',')).alias('friends')) \
        .withColumn('friends', trim(col('friends')))

    user = user.drop('friends')

    user.write \
        .format('parquet') \
        .mode('overwrite') \
        .save('out_put_raw/user')

    user_friends.write \
        .format('parquet') \
        .mode('overwrite') \
        .save('out_put_raw/user_friends')


def main() -> None:
    spark = create_spark_session()

    load_tip(spark)
    load_business(spark)
    load_checkin(spark)
    load_review(spark)
    load_user(spark)


if __name__ == '__main__':
    main()
