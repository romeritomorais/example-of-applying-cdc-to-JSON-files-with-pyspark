#!/usr/bin/env python3
# coding: utf-8

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#                                                                                                   #
#    mmm  mmmm     mmm          mmmm                       ""#                                      #
#  m"   " #   "m m"   "        #"   "  mmm   mmmmm  mmmm     #     mmm                              #
#  #      #    # #             "#mmm  "   #  # # #  #" "#    #    #"  #                             #
#  #      #    # #                 "# m"""#  # # #  #   #    #    #""""                             #
#   "mmm" #mmm"   "mmm"        "mmm#" "mm"#  # # #  ##m#"    "mm  "#mm"                             #
#                                                   #                                               #
#                                                   "                                               #
# desenvolvido por Romerito Morais https://www.linkedin.com/in/romeritomorais/                      #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

from delta import *
from pyspark.sql.functions import col, dense_rank
from pyspark.sql.window import Window
from cdc.internals import session

spark = session.spark()
tableRaw = "/home/romeritomorais/Dropbox/tecnology/develop/bigdata/lakehouse/datawarehouse/raw_cadastro"

# main spark program
if __name__ == '__main__':
    # spark.sparkContext.setLogLevel("info")

    cadastroRawDF = spark.read.format("delta").option("path", f"{tableRaw}").load()

    cadastroRawDF.createOrReplaceTempView("transformData")

    cadastroRawDF = spark.sql("""
        SELECT 
          CAST(nome AS STRING) AS nome, 
          CAST(idade AS INT) AS idade, 
          CAST(altura AS FLOAT) AS altura, 
          CAST(sexo AS STRING) AS sexo, 
          CAST(peso AS FLOAT) AS peso, 
          CAST(cor AS STRING) AS cor, 
          CAST(
            replace(data_nasc, '/', '-') AS STRING
          ) AS data_nasc, 
          CAST(signo AS STRING) AS signo, 
          CAST(tipo_sanguineo AS STRING) AS tipo_sanguineo, 
          CAST(
            replace(
              replace(cpf, '.', ''), 
              '-', 
              ''
            ) AS long
          ) AS cpf, 
          CAST(
            replace(
              replace(rg, '.', ''), 
              '-', 
              ''
            ) AS long
          ) AS rg, 
          CAST(mae AS STRING) AS mae, 
          CAST(pai AS STRING) AS pai, 
          CAST(endereco AS STRING) AS endereco, 
          CAST(numero AS STRING) AS numero, 
          CAST(bairro AS STRING) AS bairro, 
          CAST(
            replace(cep, '-', '') AS STRING
          ) AS cep, 
          CAST(cidade AS STRING) AS cidade, 
          CAST(estado AS STRING) AS estado, 
          CAST(
            replace(
              replace(
                replace(
                  replace(celular, ')', ''), 
                  '(', 
                  ''
                ), 
                ' ', 
                ''
              ), 
              '-', 
              ''
            ) AS STRING
          ) AS celular, 
          CAST(
            replace(
              replace(
                replace(
                  replace(telefone_fixo, ')', ''), 
                  '(', 
                  ''
                ), 
                ' ', 
                ''
              ), 
              '-', 
              ''
            ) AS STRING
          ) AS telefone_fixo, 
          CAST(email AS STRING) AS email
        FROM 
          transformData
    """)

    cadastroRawDF.write.format("delta").mode("overwrite").saveAsTable("default.trusted_cadastro")

    spark.stop()
