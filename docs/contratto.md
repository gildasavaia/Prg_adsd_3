# Contratto Pubblico — Retry Idempotenti Con `request_id`

## Premessa

In un sistema distribuito un client puo' inviare una scrittura, perdere la
risposta e non sapere se il server l'abbia applicata. Se ritenta alla cieca,
rischia di applicare due volte lo stesso effetto.

Questo sistema garantisce che i retry delle operazioni mutative siano
**idempotenti**: ripetere la stessa richiesta produce la stessa risposta senza
riapplicare l'effetto.

---

## Identificazione delle richieste

Ogni richiesta mutativa porta un `request_id` con formato:

```
<client_id>:<sequence_number>
```

- `client_id`: stringa alfanumerica che identifica univocamente il client
  (es. `clientA`, `node7`)
- `sequence_number`: intero >= 0, **monotono crescente** per ogni client

Esempio: `clientA:42` e' la richiesta numero 42 del client `clientA`.

---

## Comandi mutanti (idempotenti)

### `SET_REQ <request_id> <key> <value>`

Crea o sovrascrive il valore di `key`.

| Caso | Risposta |
| --- | --- |
| Successo (prima esecuzione) | `OK version=<v>` |
| Retry (request_id gia' visto) | Replay della risposta originale |
| request_id scaduto dalla finestra | `ERR request_id_expired` |
| Formato request_id errato | `ERR invalid_request_id` |
| Argomenti mancanti | `ERR usage: SET_REQ <request_id> <key> <value>` |

### `CAS_REQ <request_id> <key> <expected_version> <value>`

Compare-and-set: aggiorna `key` solo se la versione corrente corrisponde.

| Caso | Risposta |
| --- | --- |
| Successo | `OK version=<v>` |
| Version mismatch | `ERR version_mismatch current=<v>` |
| Chiave inesistente | `ERR version_mismatch current=-1` |
| Retry | Replay della risposta originale |
| request_id scaduto | `ERR request_id_expired` |
| Formato errato | `ERR invalid_request_id` |
| Argomenti mancanti | `ERR usage: CAS_REQ <request_id> <key> <expected_version> <value>` |

### `DELETE_REQ <request_id> <key>`

Elimina la chiave.

| Caso | Risposta |
| --- | --- |
| Successo | `OK` |
| Chiave inesistente | `NOT_FOUND` |
| Retry | Replay della risposta originale |
| request_id scaduto | `ERR request_id_expired` |
| Formato errato | `ERR invalid_request_id` |
| Argomenti mancanti | `ERR usage: DELETE_REQ <request_id> <key>` |

---

## Comandi non mutanti (invariati)

Questi comandi non modificano lo stato e non richiedono `request_id`.

| Comando | Descrizione | Risposta |
| --- | --- | --- |
| `PING` | Health check | `OK PONG` |
| `GET <key>` | Legge il valore | `OK <value>` oppure `NOT_FOUND` |
| `GETV <key>` | Legge valore e versione | `OK <value> version=<v>` oppure `NOT_FOUND` |
| `KEYS` | Elenca tutte le chiavi | `OK <key1> <key2> ...` |
| `QUIT` | Chiude la connessione | `OK BYE` |

---

## Comando di acknowledgement

### `ACK <client_id> <sequence_number>`

Il client conferma di aver ricevuto tutte le risposte fino a `sequence_number`
incluso. Il server puo' liberare le entry corrispondenti dalla tabella di
deduplicazione.

| Caso | Risposta |
| --- | --- |
| Successo | `OK` |
| Argomenti mancanti | `ERR usage: ACK <client_id> <sequence_number>` |
| sequence_number non intero | `ERR sequence_number must be an integer` |

---

## Precondizioni

1. Il `request_id` deve avere il formato `<client_id>:<sequence_number>`.
2. `sequence_number` deve essere un intero >= 0.
3. Il client deve usare `sequence_number` monotoni crescenti.
4. Il `sequence_number` deve essere entro la finestra di deduplicazione attiva
   (non deve essere gia' stato rimosso dalla garbage collection).

## Postcondizioni

1. Se la richiesta e' nuova: viene eseguita, la risposta viene salvata nella
   tabella di deduplicazione, e restituita al client.
2. Se la richiesta e' un replay (stesso `request_id` gia' visto): la risposta
   salvata viene restituita **senza rieseguire** il comando.
3. Valore e versione nello store riflettono una sola esecuzione per ogni
   `request_id`.

## Casi fuori contratto

| Caso | Comportamento |
| --- | --- |
| `request_id` con formato errato (no `:`, seq non numerica) | `ERR invalid_request_id` |
| `sequence_number` gia' scaduto dalla finestra di GC | `ERR request_id_expired` |
| Client che riusa lo stesso `client_id` da processi diversi | Non garantito — il contratto assume un `client_id` per processo |
| `sequence_number` non monotono (salti all'indietro) | La richiesta viene trattata normalmente se ancora in finestra, altrimenti `ERR request_id_expired` |

---

## Garbage Collection

Il server conserva al massimo `MAX_WINDOW` (default: 100) risposte per client.

Strategie di pulizia:

1. **Automatica**: quando la tabella di un client supera `MAX_WINDOW` entry, le
   entry con `sequence_number` piu' basso vengono rimosse.
2. **Esplicita**: il client invia `ACK <client_id> <seq>` per confermare la
   ricezione di tutte le risposte fino a `seq`. Il server rimuove le entry
   con `sequence_number <= seq`.

Dopo la pulizia, un retry su un `request_id` rimosso riceve
`ERR request_id_expired`.

---

## Safety

1. **No doppio effetto**: la stessa richiesta mutativa non deve produrre effetti
   doppi. La lookup nella `request_table` prima dell'esecuzione lo garantisce.
2. **Coerenza del replay**: la risposta replayed e' identica a quella prodotta
   dalla prima esecuzione. La risposta viene salvata atomicamente con l'effetto
   sotto lock.
3. **Non confusione**: due richieste diverse non vengono confuse — il
   `request_id` e' univoco per coppia `(client_id, sequence_number)`.

## Liveness

1. **GC non blocca il servizio**: la garbage collection avviene inline durante
   l'inserimento, senza stop-the-world.
2. **Completamento retry**: un client corretto (con numeri di sequenza monotoni
   entro la finestra) completa sempre la sua sequenza di retry.
3. **Memoria limitata**: il server non conserva per sempre tutti i `request_id`
   grazie alla sliding window e all'ACK cumulativo.
