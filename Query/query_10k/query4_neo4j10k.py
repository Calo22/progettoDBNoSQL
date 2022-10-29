import xlsxwriter
import time
from neo4j import GraphDatabase

uri = "neo4j://localhost:7687"
user = "neo4j"
psw = "admin"

driver = GraphDatabase.driver(uri, auth=(user, psw))
session = driver.session()

tempo = lambda: int(round(time.time() * 1000))
workbook = xlsxwriter.Workbook('Risultati_query4_Neo4j_10k.xlsx')
worksheet = workbook.add_worksheet()
row = 1
col = 0

def query4():
    session.run("""MATCH (t:TRANSAZIONE)-[r1:ORDINA]->(p:PRODOTTO)-
                [r2:CONSEGNATO_A]->(i:INDIRIZZO)-[r3:SI_TROVA_IN]->
                (ci:CITTA {Citta : 'Boston'})
                WHERE t.ID_transazione > 5000 AND t.ID_transazione < 35000
                RETURN t.ID_transazione, p.Prodotto
                """)
    
for x in range(31): 
    a1 = tempo() 
    query4() 
    a2 = tempo() 
    worksheet.write(row, col, a2-a1) 
    row += 1
    print("Il risultato di " ,x+1, " e'" ,a2-a1, " millisecondi\n") 

workbook.close()

driver.close()
