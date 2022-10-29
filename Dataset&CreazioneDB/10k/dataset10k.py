import csv
from faker import Faker
import random

fake = Faker(['en-US'])

nazione_array = []
a_rischio_array = []

id_cliente = []
nome = []
cognome = []
tel = []
email = []
ind_res = []
citta_res = []
naz_res = []
rischio_naz_res = []


with open('countries001.csv','r') as f:
    data = csv.reader(f)

    for row in data:
        nazione_array.append(row[0])
        a_rischio_array.append(row[1])


with open('clienti2.csv','r') as f:
    data = csv.reader(f)

    for row in data:
        id_cliente.append(row[0])
        nome.append(row[1])
        cognome.append(row[2])
        tel.append(row[3])
        email.append(row[4])
        ind_res.append(row[5])
        citta_res.append(row[6])
        naz_res.append(row[7])
        rischio_naz_res.append(row[8])


with open('dataset10k.csv', mode='w', newline='') as csv_file:
    fieldnames = ['ID_TRANSAZIONE', 'ID_CLIENTE', 'NOME', 'COGNOME', 'TELEFONO', 'EMAIL',
'INDIRIZZO_RESIDENZA',
                    'CITTA_RESIDENZA', 'NAZIONE_RESIDENZA', 'RISCHIO_NAZIONE_RESIDENZA', 'INDIRIZZO_IP',
'CITTA_LOCALIZZAZIONE', 'NAZIONE_LOCALIZZAZIONE', 'RISCHIO_NAZIONE_LOCALIZZAZIONE', 'N_CARTA_DI_CREDITO',
                  'IMPORTO_ORDINE', 'TIMESTAMP_ORDINE', 'PRODOTTO', 'QUANTITA', 'INDIRIZZO_SPEDIZIONE', 'CITTA_SPEDIZIONE',
                  'NAZIONE_SPEDIZIONE', 'RISCHIO_NAZIONE_SPEDIZIONE']

    writer = csv.DictWriter(csv_file, fieldnames = fieldnames)

    writer.writeheader()
    id = 1

    for x in range(10000):
            rand = random.randint(1,239)
            rand1 = random.randint(1,499)
            rand2 = random.randint(1,239)

            ID_cliente = id_cliente[rand1]       
            nome_cliente = nome[rand1]
            cognome_cliente = cognome[rand1]
            telefono_cliente = tel[rand1]
            email_cliente = email[rand1]
            indirizzo_residenza = ind_res[rand1]
            citta_residenza = citta_res[rand1]
            nazione_residenza = naz_res[rand1]
            rischio_nazione_residenza = rischio_naz_res[rand1]
            ip = fake.ipv4()
            citta_localizzazione = fake.city()
            nazione_localizzazione = nazione_array[rand]
            rischio_nazione_localizzazione = a_rischio_array[rand]
            n_carta = random.randint(1111111111, 2111111111)
            importo = random.randint(1, 2000)
            timestamp = fake.date_time_between(start_date = '-3y', end_date = 'now')
            prodotto = fake.catch_phrase()
            quantita = random.randint(1,50)
            indirizzo_spedizione = fake.address()
            citta_spedizione = fake.city()
            nazione_spedizione = nazione_array[rand2]
            rischio_nazione_spedizione = a_rischio_array[rand2]
            
            
            writer.writerow({'ID_TRANSAZIONE' :id, 'ID_CLIENTE' : ID_cliente, 'NOME' : nome_cliente,
'COGNOME' : cognome_cliente,
                        'TELEFONO' : telefono_cliente, 'EMAIL' : email_cliente, 'INDIRIZZO_RESIDENZA' : indirizzo_residenza,
'CITTA_RESIDENZA' : citta_residenza,
                        'NAZIONE_RESIDENZA' : nazione_residenza, 'RISCHIO_NAZIONE_RESIDENZA' : rischio_nazione_residenza,
'INDIRIZZO_IP' : ip, 'CITTA_LOCALIZZAZIONE' : citta_localizzazione, 'NAZIONE_LOCALIZZAZIONE' : nazione_localizzazione,
'RISCHIO_NAZIONE_LOCALIZZAZIONE' : rischio_nazione_localizzazione, 'N_CARTA_DI_CREDITO' : n_carta,
                        'IMPORTO_ORDINE' : importo, 'TIMESTAMP_ORDINE' : timestamp,
'PRODOTTO' : prodotto, 'QUANTITA' : quantita, 'INDIRIZZO_SPEDIZIONE' : indirizzo_spedizione,
'CITTA_SPEDIZIONE' : citta_spedizione, 'NAZIONE_SPEDIZIONE' : nazione_spedizione,
                             'RISCHIO_NAZIONE_SPEDIZIONE' : rischio_nazione_spedizione})
            id+=1
