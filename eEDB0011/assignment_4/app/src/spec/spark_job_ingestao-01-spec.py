import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import regexp_extract
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql import functions as F
# Author: Jones Coelho


class ETLJob:
    """
    A class that performs ETL (Extract, Transform, Load)
    """

    def __init__(self):
        """
        Initializes the Spark session for the ETL job.
        """
        self.sc: SparkContext = SparkContext()
        self.glueContext: GlueContext = GlueContext(self.sc)
        self.spark: SparkSession = self.glueContext.spark_session
        self.job = Job(self.glueContext)
        self.bancos: str = "db_sot.tb_bancos"
        self.empregados: str = "db_sot.tb_empregados"
        self.reclamacoes: str = "db_sot.tb_reclamacoes"
        self.path_output: str = "s3://pecepoli-usp-spec-458982960441/report_atividade/"

    def extract(self) -> dict:
        """
        Reads a CSV file from a specified location and
        returns a Spark DataFrame.

        Returns:
            DataFrame: A Spark DataFrame containing the file contents.
        """
        dataframes: dict = {
            "bancos": self.spark
                          .read
                          .table(self.bancos)
                          .select("nom_segto_instituicao",
                                  "num_cnpj_instituicao",
                                  "nom_instituicao"),
            "empregados": self.spark
                              .read
                              .table(self.empregados)
                              .select("vlr_ind_satis_geral",
                                      "vlr_ind_remu_bene",
                                      "nom_instituicao"),
            "reclamacoes": self.spark
                               .read
                               .table(self.reclamacoes)
                               .select("num_cnpj_instituicao",
                                       "nom_instituicao",
                                       "qtd_totl_clie_css_scr",
                                       "qtd_totl_recl",
                                       "vlr_ind_recl",
                                       "ano",
                                       "num_trimestre")
        }
        return dataframes
    
    def _parse_columns(self, dataframe: DataFrame) -> DataFrame:
        parser: dict = {
            "nom_instituicao": {
                "type": "string",
                "name": "Nome do Banco"
            },
            "num_cnpj_instituicao": {
                "type": "bigint",
                "name": "CNPJ"
            },
            "nom_segto_instituicao": {
                "type": "string",
                "name": "Classificação do Banco"
            },
            "max_qtd_totl_clie_ccs_scr": {
                "type": "int",
                "name": "Quantidade de Clientes do Bancos"
            },
            "avg_ind_recl": {
                "type": "float",
                "name": "Índice de reclamações"
            },
            "sum_qtd_totl_recl": {
                "type": "int",
                "name": "Quantidade de reclamações"
            },
            "avg_ind_geral_instituicao": {
                "type": "float",
                "name": "Índice de satisfação dos funcionários dos bancos"
            },
            "avg_ind_remu_bene_instituicao": {
                "type": "float",
                "name": "Índice de satisfação com salários dos funcionários dos bancos"
            }}
        dataframe = dataframe.select([c for c in list(parser.keys())])
        for coluna, details in parser.items():
            dataframe = dataframe.withColumnRenamed(coluna,
                                                    details["name"])
            dataframe = dataframe.withColumn(details["name"],
                                             F.col(details["name"]).cast(details["type"]))
        return dataframe

    def transform(self, dataframes: dict) -> DataFrame:
        """
        Executes transformations on the input DataFrame based
        on business requirements.

        Args:
            dataframe (DataFrame): The input Spark DataFrame.

        Returns:
            DataFrame: A Spark DataFrame ready to be loaded.
        """
        dataframes["bancos"].createOrReplaceTempView("bancos")
        dataframes["empregados"].createOrReplaceTempView("empregados")
        dataframes["reclamacoes"].createOrReplaceTempView("reclamacoes")
        QUERY = """
            SELECT max(bancos.nom_segto_instituicao) as nom_segto_instituicao,
                   bancos.num_cnpj_instituicao,
                   max(bancos.nom_instituicao) as nom_instituicao,
                   max(reclamacoes.qtd_totl_clie_css_scr) as qtd_totl_clie_css_scr,
                   max(reclamacoes.qtd_totl_recl) as qtd_totl_recl,
                   max(reclamacoes.vlr_ind_recl) as vlr_ind_recl,
                   max(empregados.vlr_ind_remu_bene) as vlr_ind_remu_bene,
                   max(empregados.vlr_ind_satis_geral) as vlr_ind_satis_geral
            FROM bancos
            LEFT JOIN reclamacoes
            ON bancos.num_cnpj_instituicao = reclamacoes.num_cnpj_instituicao OR bancos.nom_instituicao = reclamacoes.nom_instituicao
            LEFT JOIN empregados
            ON bancos.nom_instituicao = empregados.nom_instituicao
            GROUP BY bancos.num_cnpj_instituicao
        """
        self.spark.sql(QUERY).createOrReplaceTempView("temp_report_analitico")
        
        QUERY = """
            SELECT nom_instituicao,
                   num_cnpj_instituicao,
                   nom_segto_instituicao,
                   max(qtd_totl_clie_css_scr) as max_qtd_totl_clie_ccs_scr,
                   avg(vlr_ind_recl) as avg_ind_recl,
                   sum(qtd_totl_recl) as sum_qtd_totl_recl,
                   avg(vlr_ind_satis_geral) as avg_ind_geral_instituicao,
                   avg(vlr_ind_remu_bene) as avg_ind_remu_bene_instituicao
            FROM temp_report_analitico
            GROUP BY nom_instituicao, num_cnpj_instituicao, nom_segto_instituicao
        """
        dataframe = self.spark.sql(QUERY)
        dataframe = self._parse_columns(dataframe)
        dataframe = dataframe.withColumn("dat_hor_psst",
                                         F.current_timestamp())
        dataframe = dataframe.withColumn("anomesdia",
                                         F.lit(datetime.now().strftime("%Y%m%d")))
        return dataframe

    def load(self, dataframe: DataFrame) -> None:
        """
        Saves a DataFrame to an output path as a Parquet file.

        Args:
            dataframe (DataFrame): The Spark DataFrame containing
            the data to be loaded.

        Returns:
            None
        """
        # Reorganize partitions and write a single file using coalesce
        # This is done to avoid generating too many small files,
        # which could degrade performance
        # on subsequent Spark jobs that read the output files.
        # In a production environment,
        # calculating the appropriate number of output files
        # should be considered.
        df_final: DataFrame = dataframe.coalesce(1)

        # Write data to output file in Parquet format with Snappy compression
        # If a file already exists at the output location,
        # it will be overwritten.
        
        df_final.write\
                .mode("overwrite")\
                .partitionBy("anomesdia")\
                .parquet(path=self.path_output,
                         compression="snappy")

    def run(self) -> None:
        """
        Executes the complete ETL process.

        Args:
            None

        Returns:
            None
        """
        self.load(self.transform(self.extract()))

if __name__ == '__main__':
    args = getResolvedOptions(sys.argv,
                          ['JOB_NAME'])
    ETL: ETLJob = ETLJob()
    ETL.job.init(args['JOB_NAME'], args)
    ETL.run()