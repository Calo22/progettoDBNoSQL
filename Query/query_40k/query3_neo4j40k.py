import xlsxwriter
import time
from neo4j import GraphDatabase

uri = "neo4j://localhost:7687"
user = "neo4j"
psw = "admin"

driver = GraphDatabase.driver(uri, auth=(user, psw))
session = driver.session()

tempo = lambda: int(round(time.time() * 1000))
workbook = xlsxwriter.Workbook('Risultati_query3_Neo4j_40k.xlsx')
worksheet = workbook.add_worksheet()
row = 1
col = 0

def query3():
    session.run("""MATCH (c:CLIENTE)-[r:PAGA_CON]->(n:CARTA_DI_CREDITO)
                WHERE c.ID_cliente > 250 AND c.ID_cliente < 5000
                RETURN c.ID_cliente, n
                """)
    
for x in range(31): 
    a1 = tempo() 
    query3() 
    a2 = tempo() 
    worksheet.write(row, col, a2-a1) 
    row += 1
    print("Il risultato di " ,x+1, " e'" ,a2-a1, " millisecondi\n") 

workbook.close()

driver.close()
