#!/usr/bin/env python
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
from pathlib import Path
from cdc.internals import session

spark = session.spark()
tableRaw = "rw_cadastro"
tableTrusted = "tr_cadastro"

# main spark program
if __name__ == '__main__':
    # spark.sparkContext.setLogLevel("info")

    cadastroTrustedDF = DeltaTable.forName(spark, f"{tableTrusted}")

    change_data = spark.read.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", 0) \
        .table(f"{tableRaw}") \
        .filter("_change_type != 'update_preimage'")

    #print("alteracoes da tabela Raw ... ")
    # change_data.show(truncate=False)

    windowPartition = Window.partitionBy(
        "cpf").orderBy(col("_commit_version").desc())

    apply_change_data = change_data\
        .withColumn("dense_rank", dense_rank().over(windowPartition))\
        .where("dense_rank=1")\
        .distinct()

    #print("alteracoes da Raw a serem aplicadas da Trusted ... ")
    # apply_change_data.show()

    cadastroTrustedDF.alias("cadastroTrustedDF").merge(apply_change_data.alias("cadastroRawDF"), "cadastroRawDF.cpf = cadastroTrustedDF.cpf") \
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
            "senha": "cadastroRawDF.senha",
            "sexo": "cadastroRawDF.sexo",
            "signo": "cadastroRawDF.signo",
            "telefone_fixo": "cadastroRawDF.telefone_fixo",
            "tipo_sanguineo": "cadastroRawDF.tipo_sanguineo"
        }
    )\
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
            "senha": "cadastroRawDF.senha",
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
