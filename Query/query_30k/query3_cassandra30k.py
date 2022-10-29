import xlsxwriter
import time
from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('cassandra30k')

tempo = lambda: int(round(time.time() * 1000))
workbook = xlsxwriter.Workbook('Risultati_query3_Cassandra_30k.xlsx')
worksheet = workbook.add_worksheet()
row = 1
col = 0

def query3():
    session.execute("""SELECT id_cliente, n_carta_di_credito
                    FROM cassandra30k.acquisto
                    WHERE id_cliente > 250 AND id_cliente < 5000 ALLOW FILTERING
                    """)

for x in range(31): 
    a1 = tempo() 
    query3() 
    a2 = tempo() 
    worksheet.write(row, col, a2-a1) 
    row += 1
    print("Il risultato di " ,x+1, " e'" ,a2-a1, " millisecondi\n") 

workbook.close()

session.shutdown()
