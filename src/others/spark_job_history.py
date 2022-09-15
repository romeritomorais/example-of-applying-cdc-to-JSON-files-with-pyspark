#!/usr/bin/env python

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
table = "rw_cadastro"

if __name__ == '__main__':
    #spark.sparkContext.setLogLevel("info")

    #print("alteracoes que a tabela sofreu ...")
    #spark.sql(f"DESCRIBE HISTORY default.{table}").show()

    #print("todas as versoes de registros em linhas ...")
    spark.sql(f"SELECT * FROM default.{table} WHERE mae='Bárbara Isabel'").show()

    spark.stop()
