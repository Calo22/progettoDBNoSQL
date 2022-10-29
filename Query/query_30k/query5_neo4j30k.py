import xlsxwriter
import time
from neo4j import GraphDatabase

uri = "neo4j://localhost:7687"
user = "neo4j"
psw = "admin"

driver = GraphDatabase.driver(uri, auth=(user, psw))
session = driver.session()

tempo = lambda: int(round(time.time() * 1000))
workbook = xlsxwriter.Workbook('Risultati_query5_Neo4j_30k.xlsx')
worksheet = workbook.add_worksheet()
row = 1
col = 0

def query5():
    session.run("""MATCH (c:CLIENTE)-[r1:PAGA_CON]->(n:CARTA_DI_CREDITO)-
                [r2:EFFETTUA]->(t:TRANSAZIONE)-[r3:ORDINA]->(p:PRODOTTO)-
                [r4:CONSEGNATO_A]->(i:INDIRIZZO)-[r5:SI_TROVA_IN]->(c2:CITTA)-
                [r6:SITUATA_IN]->(nz:NAZIONE{A_rischio : true})
                WHERE t.Importo_ordine < 100 AND p.Quantita < 50
                RETURN c.ID_cliente, t.ID_transazione, t.Timestamp_ordine
                """)
    
for x in range(31): 
    a1 = tempo() 
    query5() 
    a2 = tempo() 
    worksheet.write(row, col, a2-a1) 
    row += 1
    print("Il risultato di " ,x+1, " e'" ,a2-a1, " millisecondi\n") 

workbook.close()

driver.close()
