# -*- coding: utf-8 -*-
"""
Batch Company Pipeline
Simple pipeline for Stock Etablissement / Unite Legale Historique data.

Written by Yuan Ji

------------------------------------------------------------

Usage:

# Run pipeline for full-sync mode
python main.py -e /path/to/etablissement/file -u /path/to/unit/legale/file -o /path/to/output/dir/ --full

# Run pipeline for daily (incremental) mode
python main.py -e /path/to/etablissement/file -u /path/to/unit/legale/file -o /path/to/output/dir/

------------------------------------------------------------

Workflow architecture:

This script runs the data processing pipeline once. In order to keep Stock
Historique data up-to-date, here's one of possible workflow architectures:

             run              output to
Scheduler ---------> Script -------------> Datalake

where Scheduler could be a simple Python script, Azure Data Factory pipeline
trigger or equivalents.

Sink Datalake could be designed as tree structure: /year/month/day/ so that the
output of pipeline runs are stored day by day and incrementally.
"""

from datetime import date, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def init_spark():
    spark = SparkSession.builder.appName("BatchCompanyPipeline").getOrCreate()
    sc = spark.sparkContext
    return spark, sc


def main(f_etab: str, f_ul: str, dir_output: str, full_sync: bool = False):
    spark, sc = init_spark()

    # input data
    df_etab = spark.read.options(header='True', delimiter=',').csv(f_etab)
    df_ul = spark.read.options(header='True', delimiter=',').csv(f_ul)

    # date parse
    df_etab_parsed = df_etab \
        .withColumn('dateFin', col('dateFin').cast('date')) \
        .withColumn('dateDebut', col('dateDebut').cast('date'))
    df_ul_parsed = df_ul \
        .withColumn('dateFin', col('dateFin').cast('date')) \
        .withColumn('dateDebut', col('dateDebut').cast('date'))

    # filter data by date based on full-sync mode or incremental mode
    if not full_sync:
        # keep only rows with dateFin=d-1 or dateDebut=d-1
        today = date.today()
        yesterday = (today - timedelta(days=1)).strftime('%Y-%m-%d')
        df_etab_parsed = df_etab_parsed.filter(
            (col('dateDebut') >= lit(yesterday)) | \
            (col('dateFin') >= lit(yesterday)))
        df_ul_parsed = df_ul_parsed.filter(
            (col('dateDebut') >= lit(yesterday)) | \
            (col('dateFin') >= lit(yesterday)))

    # parse data - etab
    df_etab_parsed = df_etab_parsed \
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
    df_ul_parsed = df_ul_parsed \
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
    df = etab.join(ul, etab['etab_siren'] == ul['ul_siren'], how='full')

    # output
    df.write.csv(dir_output, header=True)


if __name__ == '__main__':
    import argparse

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='BatchCompanyPipeline')
    parser.add_argument('-e', '--stock-etablissement-historique',
                        required=True, metavar="/path/to/file",
                        help='Path to etablissement dataset')
    parser.add_argument('-u', '--stock-unite-legale-historique',
                        required=True, metavar="/path/to/file",
                        help='Path to unite legale dataset')
    parser.add_argument('-o', '--output', default='./output/',
                        required=False, metavar="/path/to/output/dir/",
                        help='Path to output directory')
    parser.add_argument('--full', default=False, required=False,
                        action='store_true', help='Whether pipeline runs under '
                                                  'full sync mode.')
    args = parser.parse_args()

    # args setting
    f_etab = args.stock_etablissement_historique
    f_ul = args.stock_unite_legale_historique
    dir_output = args.output

    # run main program
    main(f_etab, f_ul, dir_output, args.full)
