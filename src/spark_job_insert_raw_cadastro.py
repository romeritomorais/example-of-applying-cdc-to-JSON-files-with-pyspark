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

from pathlib import Path

from cdc.internals import session

spark = session.spark()
path = Path.cwd()
storage = f"{path.parent}/storage/stage"

if __name__ == '__main__':
    spark.sparkContext.setLogLevel("info")

    # carregando dados e escrevendo no formato delta na bronze
    spark.sql("set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true")

    df = spark.read.option("multiline", "true").json(f"{storage}/*")

    appl = df.selectExpr(
        "REPLACE(CAST(altura AS STRING),',','.') AS altura",
        "CAST(bairro AS STRING) AS bairro",
        "CAST(celular AS STRING) AS celular",
        "CAST(cep AS STRING) AS cep",
        "CAST(cidade AS STRING) AS cidade",
        "CAST(cor AS STRING) AS cor",
        "CAST(cpf AS STRING) AS cpf",
        "CAST(data_nasc AS STRING) AS data_nasc",
        "CAST(email AS STRING) AS email",
        "CAST(endereco AS STRING) AS endereco",
        "CAST(estado AS STRING) AS estado",
        "CAST(idade AS STRING) AS idade",
        "CAST(mae AS STRING) AS mae",
        "CAST(nome AS STRING) AS nome",
        "CAST(numero AS STRING) AS numero",
        "CAST(pai AS STRING) AS pai",
        "CAST(peso AS STRING) AS peso",
        "CAST(rg AS STRING) AS rg",
        "CAST(senha AS STRING) AS senha",
        "CAST(sexo AS STRING) AS sexo",
        "CAST(signo AS STRING) AS signo",
        "CAST(telefone_fixo AS STRING) AS telefone_fixo",
        "CAST(tipo_sanguineo AS STRING) AS tipo_sanguineo"
    )

    # escreve os dados no formato delta na bronze
    appl.write.format("delta").mode("append").saveAsTable("default.raw_cadastro")
    # appl.write.format("delta").mode("overwrite").saveAsTable("default.tr_cadastro")

    spark.stop()
