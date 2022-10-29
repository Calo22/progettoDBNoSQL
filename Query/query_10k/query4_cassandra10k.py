import xlsxwriter
import time
from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('cassandra10k')

tempo = lambda: int(round(time.time() * 1000))
workbook = xlsxwriter.Workbook('Risultati_query4_Cassandra_10k.xlsx')
worksheet = workbook.add_worksheet()
row = 1
col = 0

def query4():
    session.execute("""SELECT id_transazione, prodotto
                    FROM cassandra10k.acquisto
                    WHERE citta_spedizione = 'Boston'
                    AND id_transazione > 5000 AND id_transazione < 35000
                    ALLOW FILTERING
                    """)

for x in range(31): 
    a1 = tempo() 
    query4() 
    a2 = tempo() 
    worksheet.write(row, col, a2-a1) 
    row += 1
    print("Il risultato di " ,x+1, " e'" ,a2-a1, " millisecondi\n") 

workbook.close()

session.shutdown()
