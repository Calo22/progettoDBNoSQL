import xlsxwriter
import time
from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('cassandra30k')

tempo = lambda: int(round(time.time() * 1000))
workbook = xlsxwriter.Workbook('Risultati_query2_Cassandra_30k.xlsx')
worksheet = workbook.add_worksheet()
row = 1
col = 0

def query2():
    session.execute("""SELECT id_cliente,cognome
                    FROM cassandra30k.cliente
                    WHERE nome = 'Eric' ALLOW FILTERING
                    """)

for x in range(31): 
    a1 = tempo() 
    query2() 
    a2 = tempo() 
    worksheet.write(row, col, a2-a1) 
    row += 1
    print("Il risultato di " ,x+1, " e'" ,a2-a1, " millisecondi\n") 

workbook.close()

session.shutdown()
