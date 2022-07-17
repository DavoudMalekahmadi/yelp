from pyspark.sql import SparkSession
import findspark
import logging
from pyspark.sql.functions import *
import time

logger = logging.getLogger(__name__)
findspark.init()


def create_spark_session():
    sp = SparkSession.builder \
        .master('local[*]') \
        .appName('NewYorker') \
        .getOrCreate()
    return sp


def review_star(df_business, df_review) -> None:
    """
    review count per business containing the 'business_name' and 'is_open' fields.(could contain more fields form business)
    :param df_business: business dataframe
    :param df_review: review dataframe
    :return: None
    """
    df_review_count = ((df_review.select(col('review_id'), col('business_id'))
                        .groupBy("business_id").count())
                       .withColumnRenamed('count', 'review_count')
                       .join(df_business
                             .select(col('business_id').alias('business_id_2'),
                                     col('name').alias('business_name'),
                                     col('is_open')),
                             col('business_id') == col('business_id_2'),
                             'inner')
                       .drop('business_id_2')
                       .orderBy(col('is_open'), col('review_count').desc()))

    df_review_count.write \
        .format('parquet') \
        .mode('overwrite') \
        .save('out_put_agg/review_count')


def review_words(df_business, df_review) -> None:
    """
    Calculate the repeated words in each review per business and join with business
    useful for datamining and machine learning for finding a pattern for closed businesses in reviews due to advise
    running businesses to prevent the same.
    :param df_business: business dataframe
    :param df_review: review dataframe
    :return: None
    """
    df_review_words = (df_review.select(explode(split(col('text'), ' ')).alias('words'),
                                        col('business_id'))
                       .withColumn('words', trim(col('words')))
                       .groupBy(col('business_id'), col('words')).count()
                       .withColumnRenamed('count', 'word_count')) \
        .join(df_business.select(col('business_id').alias('business_id_2'),
                                 col('name'),
                                 col('is_open'),
                                 col('stars'),
                                 col('postal_code'),
                                 col('categories'),
                                 col('WiFi')), col('business_id') == col('business_id_2'), 'inner') \
        .drop('business_id_2')

    df_review_words.write \
        .format('parquet') \
        .mode('overwrite') \
        .save('out_put_agg/business_closed_words')


def business_stars_avg(df_review) -> None:
    """
    Average of stars per business based on the reviews for that business.
    :param df_review: review dataframe
    :return: None
    """
    df_business_stars_avg = df_review.groupBy('business_id').avg('stars')\
        .withColumn('avg_stars', round(col('avg(stars)')))\
        .drop('avg(stars)')

    df_business_stars_avg.write \
        .format('parquet') \
        .mode('overwrite') \
        .save('out_put_agg/business_stars_avg')


def business_checkin_daily(spark, df_business, df_checkin, df_review) -> None:
    """
    Count of checkin per business in each day and comparing the average of stars for that business
     from 'business_stars_avg' dataset with the business stars.
    :param spark: SparkSession
    :param df_business: business dataframe
    :param df_checkin: checkin dataframe
    :param df_review: review dataframe
    :return: None
    """
    df_checkin_daily = df_checkin.groupBy(date_format(col('date'), "yyyy-MM-dd").alias('day'), col('business_id'))\
        .count()\
        .createOrReplaceTempView('df_checkin_daily')

    df_business_checkin_daily = spark.sql('select a.*, b.name, b.city, b.address, b.stars'
                                          'from df_checkin_daily a inner join df_business b on a.business_id = b.business_id')
    df_business_checkin_daily.createOrReplaceTempView('df_business_checkin_daily')

    df_business_stars_avg = spark.read \
        .option('inferSchema', True) \
        .parquet('out_put_agg/business_stars_avg')
    df_business_stars_avg.createOrReplaceTempView('df_business_stars_avg')

    df_business_checkin_daily_stars_compare = spark.sql('select a.*, b.avg_stars '
                                                        'from df_business_checkin_daily a inner join df_business_stars_avg b '
                                                        'on a.business_id = b.business_id')

    df_business_checkin_daily_stars_compare.write \
        .format('parquet') \
        .mode('overwrite') \
        .save('out_put_agg/business_checkin_daily_stars_compare')


def business_checkin_weekly(spark, df_business, df_checkin, df_review) -> None:
    """
    Count of checkin per business in each week and comparing the average of stars for that business
     from 'business_stars_avg' dataset with the business stars.
    :param spark: SparkSession
    :param df_business: business dataframe
    :param df_checkin: checkin dataframe
    :param df_review: review dataframe
    :return: None
    """
    df_checkin_weekly = df_checkin.groupBy(weekofyear(col('date')).alias('week_of_year'), col('business_id'))\
        .count()\
        .createOrReplaceTempView('df_checkin_weekly')

    df_business_checkin_weekly = spark.sql('select a.*, b.name, b.city, b.address, b.stars '
                                          'from df_checkin_weekly a inner join df_business b on a.business_id = b.business_id')
    df_business_checkin_weekly.createOrReplaceTempView('df_business_checkin_weekly')

    df_business_stars_avg = spark.read \
        .option('inferSchema', True) \
        .parquet('out_put_agg/business_stars_avg')
    df_business_stars_avg.createOrReplaceTempView('df_business_stars_avg')

    df_business_checkin_weekly_stars_compare = spark.sql('select a.*, b.avg_stars '
                                                        'from df_business_checkin_weekly a inner join df_business_stars_avg b '
                                                        'on a.business_id = b.business_id')

    df_business_checkin_weekly_stars_compare.write \
        .format('parquet') \
        .mode('overwrite') \
        .save('out_put_agg/business_checkin_weekly_stars_compare')


def business_checkin_monthly(spark, df_business, df_checkin, df_review) -> None:
    """
    Count of checkin per business in each month and comparing the average of stars for that business
     from 'business_stars_avg' dataset with the business stars.
    :param spark: SparkSession
    :param df_business: business dataframe
    :param df_checkin: checkin dataframe
    :param df_review: review dataframe
    :return: None
    """
    df_checkin_monthly = df_checkin.groupBy(date_format(col('date'), "yyyy-MM").alias('month'), col('business_id'))\
        .count()\
        .createOrReplaceTempView('df_checkin_monthly')

    df_business_checkin_monthly = spark.sql('select a.*, b.name, b.city, b.address, b.stars '
                                          'from df_checkin_monthly a inner join df_business b on a.business_id = b.business_id')
    df_business_checkin_monthly.createOrReplaceTempView('df_business_checkin_monthly')

    df_business_stars_avg = spark.read \
        .option('inferSchema', True) \
        .parquet('out_put_agg/business_stars_avg')
    df_business_stars_avg.createOrReplaceTempView('df_business_stars_avg')

    df_business_checkin_monthly_stars_compare = spark.sql('select a.*, b.avg_stars '
                                                        'from df_business_checkin_monthly a inner join df_business_stars_avg b '
                                                        'on a.business_id = b.business_id')

    df_business_checkin_monthly_stars_compare.write \
        .format('parquet') \
        .mode('overwrite') \
        .save('out_put_agg/business_checkin_monthly_stars_compare')


def main() -> None:
    spark = create_spark_session()

    df_business = spark.read \
        .option('inferSchema', True) \
        .parquet('out_put_raw/business')
    df_business.createOrReplaceTempView('df_business')

    df_user = spark.read \
        .option('inferSchema', True) \
        .parquet('out_put_raw/user')
    df_user.createOrReplaceTempView('df_user')

    df_review = spark.read \
        .option('inferSchema', True) \
        .parquet('out_put_raw/review')
    df_review.createOrReplaceTempView('df_review')

    df_checkin = spark.read \
        .option('inferSchema', True) \
        .parquet('out_put_raw/checkin')
    df_review.createOrReplaceTempView('checkin')

    review_star(df_business, df_review)
    review_words(df_business, df_review)
    business_stars_avg(df_review)
    business_checkin_daily(spark, df_business, df_checkin, df_review)
    business_checkin_weekly(spark, df_business, df_checkin, df_review)
    business_checkin_monthly(spark, df_business, df_checkin, df_review)


if __name__ == '__main__':
    main()
