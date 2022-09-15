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

from pathlib import Path
from cdc.internals import session

spark = session.spark()
table = "tr_cadastro"

# main spark program
if __name__ == '__main__':
    # spark.sparkContext.setLogLevel("info")

    # spark.sql(f"INSERT INTO default.{table} VALUES (1,12,'forcas especiais ginew','000','2000-02-29 00:00:00',160.90,200) ")
    # spark.sql(f"INSERT INTO default.{table} VALUES (16000,42223,'tenchiham','988908908','1992-02-29 00:00:00',188.90,200) ")
    # spark.sql(f"INSERT INTO default.{table} VALUES (17000,42223,'majin boo magro','988908908','1992-02-29 00:00:00',200.90,200) ")
    # spark.sql(f"UPDATE default.{table} SET idade='40' WHERE mae='Bárbara Isabel'")
    # spark.sql(f"DELETE FROM default.{table} WHERE id=1")

    # change_data = spark.read.format("delta") \
    #     .option("readChangeFeed", "true") \
    #     .option("startingVersion", 1) \
    #     .table(f"{table}") \
    #     .filter("_change_type != 'update_preimage'")

    # print(f"alteracoes da tabela {table}... ")
    # change_data.select("nome", "peso", "mae", "pai", "idade", "endereco", "bairro", "numero", "_change_type", "_commit_version", "_commit_timestamp")\
    #     .where("cpf='677.958.886-58'")\
    #     .orderBy(col("nome").asc(), col("_commit_version").asc())\
    #     .show(truncate=False)

    # windowPartition = Window.partitionBy(
    #     "nome").orderBy(col("_commit_version").desc())

    # apply_changes = change_data\
    #     .withColumn("dense_rank", dense_rank().over(windowPartition))\
    #     .where("dense_rank=1")\
    #     .drop("dense_rank")\
    #     .distinct()

    # print("mudancas as serem aplicadas na trusted... ")
    # apply_changes.select("nome", "peso", "mae", "pai", "idade", "endereco", "bairro",
    #                      "numero", "_change_type", "_commit_version", "_commit_timestamp").show(truncate=True)

    query = f"SELECT * FROM default.{tableTrusted}"
    spark.sql(query).show(truncate=False)

    spark.stop()
