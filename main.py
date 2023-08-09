from pyspark.sql import SparkSession


def init_spark():
    spark = SparkSession.builder.appName("BatchCompanyPipeline").getOrCreate()
    sc = spark.sparkContext
    return spark, sc


def main():
    spark, sc = init_spark()

    # data
    df_etab = spark.read.options(header='True', delimiter=',') \
        .csv(r'C:\Users\YuanJI\Desktop\Trustpair\sample\StockEtablissementHistorique.csv')
    df_ul = spark.read.options(header='True', delimiter=',') \
        .csv(r'C:\Users\YuanJI\Desktop\Trustpair\sample\StockUniteLegaleHistorique.csv')

    df_etab.printSchema()
    df_ul.printSchema()


if __name__ == '__main__':
    main()
