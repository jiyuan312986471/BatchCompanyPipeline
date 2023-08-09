from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def init_spark():
    spark = SparkSession.builder.appName("BatchCompanyPipeline").getOrCreate()
    sc = spark.sparkContext
    return spark, sc


def main():
    spark, sc = init_spark()

    # input data
    df_etab = spark.read.options(header='True', delimiter=',') \
        .csv(r'C:\Users\YuanJI\Desktop\Trustpair\sample\StockEtablissementHistorique.csv')
    df_ul = spark.read.options(header='True', delimiter=',') \
        .csv(r'C:\Users\YuanJI\Desktop\Trustpair\sample\StockUniteLegaleHistorique.csv')

    # parse data - etab
    df_etab_parsed = df_etab \
        .withColumn('dateFin', col('dateFin').cast('date')) \
        .withColumn('dateDebut', col('dateDebut').cast('date')) \
        .withColumn('etatAdministratifEtablissement',
            when(col('etatAdministratifEtablissement').isin(['A', 'F']),
                 col('etatAdministratifEtablissement')) \
            .otherwise(None)) \
        .withColumn('nomenclatureActivitePrincipaleEtablissement',
            when(col('nomenclatureActivitePrincipaleEtablissement') \
                    .isin(['NAFRev2', 'NAFRev1', 'NAF1993', 'NAP']),
                 col('nomenclatureActivitePrincipaleEtablissement')) \
            .otherwise(None)) \
        .withColumn('caractereEmployeurEtablissement',
            when(col('caractereEmployeurEtablissement').isin(['O', 'N']),
                 col('caractereEmployeurEtablissement')) \
            .otherwise(None))

    # parse data - ul
    df_ul_parsed = df_ul \
        .withColumn('dateFin', col('dateFin').cast('date')) \
        .withColumn('dateDebut', col('dateDebut').cast('date')) \
        .withColumn('etatAdministratifUniteLegale',
            when(col('etatAdministratifUniteLegale').isin(['A', 'C']),
                 col('etatAdministratifUniteLegale')) \
            .otherwise(None)) \
        .withColumn('nomenclatureActivitePrincipaleUniteLegale',
            when(col('nomenclatureActivitePrincipaleUniteLegale') \
                 .isin(['NAFRev2', 'NAFRev1', 'NAF1993', 'NAP']),
                 col('nomenclatureActivitePrincipaleUniteLegale')) \
            .otherwise(None)) \
        .withColumn('economieSocialeSolidaireUniteLegale',
            when(col('economieSocialeSolidaireUniteLegale').isin(['O', 'N']),
                 col('economieSocialeSolidaireUniteLegale')) \
            .otherwise(None)) \
        .withColumn('caractereEmployeurUniteLegale',
            when(col('caractereEmployeurUniteLegale').isin(['O', 'N']),
                 col('caractereEmployeurUniteLegale')) \
            .otherwise(None))

    # extract SIREN for etablissement
    df_etab_parsed = df_etab_parsed.withColumn('siren',
        expr("regexp_replace(siret, concat(coalesce(nic, ''), '$'), '')"))

    # merge etablissement and unite legale
    etab = df_etab_parsed \
        .select(*(col(x).alias('etab_' + x) for x in df_etab_parsed.columns))
    ul = df_ul_parsed \
        .select(*(col(x).alias('ul_' + x) for x in df_ul_parsed.columns))
    df = etab.join(ul, etab['etab_siren'] == ul['ul_siren'], how='left')

    df.printSchema()
    df.show()


if __name__ == '__main__':
    main()
