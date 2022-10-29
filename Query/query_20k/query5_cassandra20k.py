import xlsxwriter
import time
from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('cassandra20k')

tempo = lambda: int(round(time.time() * 1000))
workbook = xlsxwriter.Workbook('Risultati_query5_Cassandra_20k.xlsx')
worksheet = workbook.add_worksheet()
row = 1
col = 0

def query5():
    session.execute("""SELECT id_cliente, id_transazione, timestamp_ordine
                    FROM cassandra20k.acquisto
                    WHERE rischio_nazione_spedizione = true
                    AND importo_ordine < 100 AND quantita_prodotto < 50
                    ALLOW FILTERING
                    """)

for x in range(31): 
    a1 = tempo() 
    query5() 
    a2 = tempo() 
    worksheet.write(row, col, a2-a1) 
    row += 1
    print("Il risultato di " ,x+1, " e'" ,a2-a1, " millisecondi\n") 

workbook.close()

session.shutdown()
