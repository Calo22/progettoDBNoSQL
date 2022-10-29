import csv
from cassandra.cluster import Cluster

KEYSPACE = "cassandra40k"

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

session.execute("""
    CREATE KEYSPACE IF NOT EXISTS %s 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} 
    """ % KEYSPACE)

session.set_keyspace(KEYSPACE)


session.execute("""
    CREATE TABLE IF NOT EXISTS Cliente (
        id_cliente int PRIMARY KEY, 
        nome varchar, 
        cognome varchar, 
        telefono int, 
        email varchar, 
        indirizzo_residenza varchar, 
        citta_residenza varchar,
        nazione_residenza varchar,
        rischio_nazione_residenza boolean);
    """)


session.execute("""
    CREATE TABLE IF NOT EXISTS Acquisto (
        id_transazione int PRIMARY KEY,
        id_cliente int,  
        indirizzo_IP varchar, 
        citta_localizzazione varchar, 
        nazione_localizzazione varchar, 
        rischio_nazione_localizzazione boolean,
        n_carta_di_credito int,
        importo_ordine int, 
        timestamp_ordine varchar,
        prodotto varchar,
        quantita_prodotto int,
        indirizzo_spedizione varchar,
        citta_spedizione varchar,
        nazione_spedizione varchar,
        rischio_nazione_spedizione boolean);
    """)



prepared1 = session.prepare("""
        INSERT INTO Cliente (id_cliente, nome, cognome, telefono, email, indirizzo_residenza, citta_residenza,
            nazione_residenza, rischio_nazione_residenza)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)

prepared2 = session.prepare("""
        INSERT INTO Acquisto (id_transazione, id_cliente, indirizzo_IP, citta_localizzazione,
            nazione_localizzazione, rischio_nazione_localizzazione, n_carta_di_credito, importo_ordine, 
            timestamp_ordine, prodotto, quantita_prodotto, indirizzo_spedizione, citta_spedizione,
            nazione_spedizione, rischio_nazione_spedizione)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)


with open('dataset40k.csv','r') as f_csv:
    data = csv.reader(f_csv)
    header = next(data)

    for row in data:
         ID_cliente = int(row[1])     
         nome_cliente = row[2]
         cognome_cliente = row[3]
         telefono_cliente = int(row[4])
         email_cliente = row[5]
         indirizzo_residenza = row[6]
         citta_residenza = row[7]
         nazione_residenza = row[8]
         rischio_nazione_residenza = bool(row[9])

         session.execute(prepared1, [ID_cliente, nome_cliente, cognome_cliente, telefono_cliente,
                                     email_cliente, indirizzo_residenza, citta_residenza, nazione_residenza,
                                     rischio_nazione_residenza])

f_csv.close()

with open('dataset40k.csv','r') as f_csv:
    data = csv.reader(f_csv)
    header = next(data)

    for row1 in data:
         ID_transazione = int(row1[0])
         ID_cliente = int(row1[1])        
         indirizzo_IP = row1[10] 
         citta_localizzazione = row1[11] 
         nazione_localizzazione = row1[12] 
         rischio_nazione_localizzazione = bool(row1[13])
         n_carta_di_credito = int(row1[14])
         importo_ordine = int(row1[15]) 
         timestamp_ordine = row1[16]
         prodotto = row1[17]
         quantita_prodotto = int(row1[18])
         indirizzo_spedizione = row1[19]
         citta_spedizione = row1[20]
         nazione_spedizione = row1[21]
         rischio_nazione_spedizione = bool(row1[22])     
             

         session.execute(prepared2, [ID_transazione, ID_cliente, indirizzo_IP, citta_localizzazione,
                                     nazione_localizzazione, rischio_nazione_localizzazione, n_carta_di_credito,
                                     importo_ordine, timestamp_ordine, prodotto, quantita_prodotto, indirizzo_spedizione,
                                     citta_spedizione, nazione_spedizione, rischio_nazione_spedizione])
         
f_csv.close()

session.shutdown()

         
