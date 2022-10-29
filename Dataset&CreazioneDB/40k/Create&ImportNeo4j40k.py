from neo4j import GraphDatabase

uri = "neo4j://localhost:7687"
user = "neo4j"
psw = "admin"

driver = GraphDatabase.driver(uri, auth=(user, psw))
session = driver.session()



session.run("CREATE CONSTRAINT ON (c:CLIENTE) ASSERT c.ID_cliente IS UNIQUE;")
session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset40k.csv" AS row
            MERGE (c:CLIENTE{ID_cliente : toInteger(row.ID_CLIENTE),
            Nome : row.NOME, Cognome : row.COGNOME});
            """)

session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset40k.csv" AS row
            MERGE (c:CONTATTO{Telefono : toInteger(row.TELEFONO),
            eMail : row.EMAIL});
            """)

session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset40k.csv" AS row
            MERGE (l:LOCALIZZAZIONE{Indirizzo_IP : row.INDIRIZZO_IP});
            """)

session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset40k.csv" AS row
            MERGE (n:CARTA_DI_CREDITO{N_carta_di_credito : toInteger(row.N_CARTA_DI_CREDITO)});
            """)

session.run("CREATE CONSTRAINT ON (t:TRANSAZIONE) ASSERT t.ID_transazione IS UNIQUE;")
session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset40k.csv" AS row
            MERGE (t:TRANSAZIONE{ID_transazione : toInteger(row.ID_TRANSAZIONE), 
            Importo_ordine : toInteger(row.IMPORTO_ORDINE), Timestamp_ordine : row.TIMESTAMP_ORDINE});
            """)

session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset40k.csv" AS row
            MERGE (i:INDIRIZZO{Indirizzo : row.INDIRIZZO_RESIDENZA});
            """)
session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset40k.csv" AS row
            MERGE (i:INDIRIZZO{Indirizzo : row.INDIRIZZO_SPEDIZIONE});
            """)

session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset40k.csv" AS row
            MERGE (p:PRODOTTO{Prodotto : row.PRODOTTO, Quantita : toInteger(row.QUANTITA)});
            """)

session.run("CREATE CONSTRAINT ON (c:CITTA) ASSERT c.Citta IS UNIQUE;")
session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset40k.csv" AS row
            MERGE (c:CITTA{Citta : row.CITTA_RESIDENZA});
            """)
session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset40k.csv" AS row
            MERGE (c:CITTA{Citta : row.CITTA_LOCALIZZAZIONE});
            """)
session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset40k.csv" AS row
            MERGE (c:CITTA{Citta : row.CITTA_SPEDIZIONE});
            """)

session.run("CREATE CONSTRAINT ON (n:NAZIONE) ASSERT n.Nazione IS UNIQUE;")
session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset40k.csv" AS row
            MERGE (n:NAZIONE{Nazione : row.NAZIONE_RESIDENZA,
            A_rischio : toBoolean(row.RISCHIO_NAZIONE_RESIDENZA)});
            """)
session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset40k.csv" AS row
            MERGE (n:NAZIONE{Nazione : row.NAZIONE_LOCALIZZAZIONE,
            A_rischio : toBoolean(row.RISCHIO_NAZIONE_LOCALIZZAZIONE)});
            """)
session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset40k.csv" AS row
            MERGE (n:NAZIONE{Nazione : row.NAZIONE_SPEDIZIONE,
            A_rischio : toBoolean(row.RISCHIO_NAZIONE_SPEDIZIONE)});
            """)





session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset40k.csv" AS row 
            MATCH (cl:CLIENTE{ID_cliente : toInteger(row.ID_CLIENTE)}) 
            MATCH (co:CONTATTO{Telefono : toInteger(row.TELEFONO), eMail : row.EMAIL}) 
            MERGE (cl)-[r:POSSIEDE]->(co);
            """)

session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset40k.csv" AS row 
            MATCH (cl:CLIENTE{ID_cliente : toInteger(row.ID_CLIENTE)}) 
            MATCH (lo:LOCALIZZAZIONE{Indirizzo_IP : row.INDIRIZZO_IP}) 
            MERGE (cl)-[r:HA]->(lo);
            """)

session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset40k.csv" AS row 
            MATCH (cl:CLIENTE{ID_cliente : toInteger(row.ID_CLIENTE)}) 
            MATCH (in:INDIRIZZO{Indirizzo : row.INDIRIZZO_RESIDENZA}) 
            MERGE (cl)-[r:RESIDENTE_IN]->(in);
            """)

session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset40k.csv" AS row 
            MATCH (cl:CLIENTE{ID_cliente : toInteger(row.ID_CLIENTE)}) 
            MATCH (cd:CARTA_DI_CREDITO{N_carta_di_credito : toInteger(row.N_CARTA_DI_CREDITO)}) 
            MERGE (cl)-[r:PAGA_CON]->(cd);
            """)

session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset40k.csv" AS row 
            MATCH (lo:LOCALIZZAZIONE{Indirizzo_IP : row.INDIRIZZO_IP}) 
            MATCH (ci:CITTA{Citta : row.CITTA_LOCALIZZAZIONE}) 
            MERGE (lo)-[r:LOCALIZZATO_IN]->(ci);
            """)

session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset40k.csv" AS row 
            MATCH (in:INDIRIZZO{Indirizzo : row.INDIRIZZO_RESIDENZA}) 
            MATCH (ci:CITTA{Citta : row.CITTA_RESIDENZA}) 
            MERGE (in)-[r:SI_TROVA_IN]->(ci);
            """)

session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset40k.csv" AS row 
            MATCH (cd:CARTA_DI_CREDITO{N_carta_di_credito : toInteger(row.N_CARTA_DI_CREDITO)}) 
            MATCH (tr:TRANSAZIONE{ID_transazione : toInteger(row.ID_TRANSAZIONE)}) 
            MERGE (cd)-[r:EFFETTUA]->(tr);
            """)

session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset40k.csv" AS row 
            MATCH (tr:TRANSAZIONE{ID_transazione : toInteger(row.ID_TRANSAZIONE)}) 
            MATCH (pr:PRODOTTO{Prodotto : row.PRODOTTO, Quantita : toInteger(row.QUANTITA)}) 
            MERGE (tr)-[r:ORDINA]->(pr);
            """)

session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset40k.csv" AS row 
            MATCH (pr:PRODOTTO{Prodotto : row.PRODOTTO, Quantita : toInteger(row.QUANTITA)}) 
            MATCH (in:INDIRIZZO{Indirizzo : row.INDIRIZZO_SPEDIZIONE}) 
            MERGE (pr)-[r:CONSEGNATO_A]->(in);
            """)

session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset40k.csv" AS row 
            MATCH (in:INDIRIZZO{Indirizzo : row.INDIRIZZO_SPEDIZIONE}) 
            MATCH (ci:CITTA{Citta : row.CITTA_SPEDIZIONE}) 
            MERGE (in)-[r:SI_TROVA_IN]->(ci);
            """)

session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset40k.csv" AS row 
            MATCH (ci:CITTA{Citta : row.CITTA_RESIDENZA}) 
            MATCH (na:NAZIONE{Nazione : row.NAZIONE_RESIDENZA,
                A_rischio : toBoolean(row.RISCHIO_NAZIONE_RESIDENZA)})
            MERGE (ci)-[r:SITUATA_IN]->(na);
            """)

session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset40k.csv" AS row 
            MATCH (ci:CITTA{Citta : row.CITTA_LOCALIZZAZIONE}) 
            MATCH (na:NAZIONE{Nazione : row.NAZIONE_LOCALIZZAZIONE,
                A_rischio : toBoolean(row.RISCHIO_NAZIONE_LOCALIZZAZIONE)})
            MERGE (ci)-[r:SITUATA_IN]->(na);
            """)

session.run("""LOAD CSV WITH HEADERS FROM "file:///dataset40k.csv" AS row 
            MATCH (ci:CITTA{Citta : row.CITTA_SPEDIZIONE}) 
            MATCH (na:NAZIONE{Nazione : row.NAZIONE_SPEDIZIONE,
                A_rischio : toBoolean(row.RISCHIO_NAZIONE_SPEDIZIONE)})
            MERGE (ci)-[r:SITUATA_IN]->(na);
            """)

driver.close()

