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
tableRaw = "raw_cadastro"
tableTrusted = "trusted_cadastro"

# main spark program
if __name__ == '__main__':
    # spark.sparkContext.setLogLevel("info")

    cadastroTrustedDF = DeltaTable.forName(spark, f"{tableTrusted}")

    change_data = spark.read.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", 0) \
        .table(f"{tableRaw}") \
        .filter("_change_type != 'update_preimage'")

    change_data.createOrReplaceTempView("transformChangeData")

    change_data = spark.sql("""
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
          CAST(email AS STRING) AS email,
          _change_type,
          _commit_version,
          _commit_timestamp
        FROM 
          transformChangeData
    """)


    windowPartition = Window.partitionBy(
        "cpf").orderBy(col("_commit_version").desc())

    apply_change_data = change_data \
        .withColumn("dense_rank", dense_rank().over(windowPartition)) \
        .where("dense_rank=1") \
        .distinct()

    cadastroTrustedDF.alias("cadastroTrustedDF").merge(apply_change_data.alias("cadastroRawDF"),
                                                       "cadastroRawDF.cpf = cadastroTrustedDF.cpf") \
        .whenMatchedDelete(
        condition="cadastroRawDF._change_type = 'delete'") \
        .whenMatchedUpdate(
        set={
            "altura": "cadastroRawDF.altura",
            "bairro": "cadastroRawDF.bairro",
            "celular": "cadastroRawDF.celular",
            "cep": "cadastroRawDF.cep",
            "cidade": "cadastroRawDF.cidade",
            "cor": "cadastroRawDF.cor",
            "cpf": "cadastroRawDF.cpf",
            "data_nasc": "cadastroRawDF.data_nasc",
            "email": "cadastroRawDF.email",
            "endereco": "cadastroRawDF.endereco",
            "estado": "cadastroRawDF.estado",
            "idade": "cadastroRawDF.idade",
            "mae": "cadastroRawDF.mae",
            "nome": "cadastroRawDF.nome",
            "numero": "cadastroRawDF.numero",
            "pai": "cadastroRawDF.pai",
            "peso": "cadastroRawDF.peso",
            "rg": "cadastroRawDF.rg",
            "sexo": "cadastroRawDF.sexo",
            "signo": "cadastroRawDF.signo",
            "telefone_fixo": "cadastroRawDF.telefone_fixo",
            "tipo_sanguineo": "cadastroRawDF.tipo_sanguineo"
        }
    ) \
        .whenNotMatchedInsert(
        condition="cadastroRawDF._change_type != 'delete'",
        values={
            "altura": "cadastroRawDF.altura",
            "bairro": "cadastroRawDF.bairro",
            "celular": "cadastroRawDF.celular",
            "cep": "cadastroRawDF.cep",
            "cidade": "cadastroRawDF.cidade",
            "cor": "cadastroRawDF.cor",
            "cpf": "cadastroRawDF.cpf",
            "data_nasc": "cadastroRawDF.data_nasc",
            "email": "cadastroRawDF.email",
            "endereco": "cadastroRawDF.endereco",
            "estado": "cadastroRawDF.estado",
            "idade": "cadastroRawDF.idade",
            "mae": "cadastroRawDF.mae",
            "nome": "cadastroRawDF.nome",
            "numero": "cadastroRawDF.numero",
            "pai": "cadastroRawDF.pai",
            "peso": "cadastroRawDF.peso",
            "rg": "cadastroRawDF.rg",
            "sexo": "cadastroRawDF.sexo",
            "signo": "cadastroRawDF.signo",
            "telefone_fixo": "cadastroRawDF.telefone_fixo",
            "tipo_sanguineo": "cadastroRawDF.tipo_sanguineo"
        }
    ).execute()

    print(f"estado atual da {tableTrusted} ... ")
    query = f"SELECT  altura,cep,data_nasc,email,nome,pai,mae,endereco FROM default.{tableTrusted}"
    spark.sql(query).show(truncate=False)

    spark.stop()
