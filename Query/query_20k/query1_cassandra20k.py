import xlsxwriter
import time
from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('cassandra20k')

tempo = lambda: int(round(time.time() * 1000))
workbook = xlsxwriter.Workbook('Risultati_query1_Cassandra_20k.xlsx')
worksheet = workbook.add_worksheet()
row = 1
col = 0

def query1():
    session.execute("""SELECT nome,cognome
                    FROM cassandra20k.cliente
                    WHERE id_cliente > 280 ALLOW FILTERING
                    """)

for x in range(31): 
    a1 = tempo() 
    query1() 
    a2 = tempo() 
    worksheet.write(row, col, a2-a1) 
    row += 1
    print("Il risultato di " ,x+1, " e'" ,a2-a1, " millisecondi\n") 

workbook.close()

session.shutdown()
