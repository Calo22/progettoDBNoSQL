import xlsxwriter
import time
from neo4j import GraphDatabase

uri = "neo4j://localhost:7687"
user = "neo4j"
psw = "admin"

driver = GraphDatabase.driver(uri, auth=(user, psw))
session = driver.session()

tempo = lambda: int(round(time.time() * 1000))
workbook = xlsxwriter.Workbook('Risultati_query2_Neo4j_40k.xlsx')
worksheet = workbook.add_worksheet()
row = 1
col = 0

def query2():
    session.run("""MATCH (c:CLIENTE{Nome : 'Eric'})
                RETURN c.ID_cliente, c.Cognome
                """)
    
for x in range(31): 
    a1 = tempo() 
    query2() 
    a2 = tempo() 
    worksheet.write(row, col, a2-a1) 
    row += 1
    print("Il risultato di " ,x+1, " e'" ,a2-a1, " millisecondi\n") 

workbook.close()

driver.close()
