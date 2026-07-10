# Nota Tecnica — Retry Idempotenti Con `request_id`

## Promessa introdotta

Il sistema garantisce **at-most-once semantics** per le operazioni mutative:
una richiesta identificata da `request_id` viene eseguita al piu' una volta,
indipendentemente dal numero di volte che il client la invia.

## Costo tecnico accettato

La promessa di idempotenza ha i seguenti costi:

1. **Memoria aggiuntiva**: il server mantiene una `request_table` con le
   risposte delle ultime `MAX_WINDOW` richieste per ciascun client. Con
   `MAX_WINDOW=100` e risposte medie di ~30 byte, il costo e' circa
   3 KB per client attivo.

2. **Overhead per richiesta**: ogni operazione mutativa richiede una lookup
   nella `request_table` prima dell'esecuzione e un inserimento dopo.
   Entrambe le operazioni sono O(1) su dizionario Python.

3. **Finestra di deduplicazione finita**: i retry sono garantiti solo
   entro la finestra. Un client che ritenta una richiesta dopo che
   `MAX_WINDOW` richieste successive sono state inviate ricevera'
   `ERR request_id_expired`. Questo e' un trade-off esplicito tra
   garanzia di idempotenza e consumo di memoria.

4. **Lock globale**: l'intera operazione (check dedup + esecuzione +
   salvataggio) avviene sotto un singolo lock. Questo semplifica la
   correttezza ma limita il throughput sotto carico elevato.

---

## Proprieta' di Safety

### S1: No doppio effetto

**Enunciato**: la stessa richiesta mutativa (stesso `request_id`) non deve
produrre effetti doppi sullo stato del KV store.

**Come e' garantita**: prima di eseguire qualsiasi operazione mutativa, il
server controlla se il `request_id` e' gia' presente nella `request_table`.
Se lo e', restituisce la risposta salvata senza rieseguire il comando.
Il check e l'eventuale esecuzione avvengono atomicamente sotto lo stesso lock,
impedendo race condition tra thread concorrenti.

**Conseguenza**: se un client invia `SET_REQ clientA:42 key value` tre volte,
la versione della chiave viene incrementata una sola volta.

### S2: Coerenza del replay

**Enunciato**: la risposta replayed deve essere identica a quella prodotta
dalla prima esecuzione.

**Come e' garantita**: la risposta viene calcolata una sola volta (alla prima
esecuzione) e salvata atomicamente nella `request_table` sotto lo stesso lock
che protegge la modifica allo store. Non esiste una finestra temporale in cui
l'effetto e' applicato ma la risposta non e' ancora salvata.

### S3: Non confusione tra richieste

**Enunciato**: due richieste diverse non devono essere confuse solo perche'
toccano la stessa chiave.

**Come e' garantita**: la deduplicazione avviene su `request_id`
(coppia `client_id:sequence_number`), non sulla chiave. Due richieste con
`request_id` diversi (`clientA:42` e `clientA:43`) sono sempre trattate come
operazioni distinte, anche se agiscono sulla stessa chiave.

---

## Proprieta' di Liveness

### L1: Garbage collection non blocca il servizio

**Enunciato**: la pulizia della `request_table` non deve causare
starvation o stop-the-world.

**Come e' garantita**: la garbage collection avviene inline, durante
l'inserimento di una nuova entry. Quando la tabella di un client supera
`MAX_WINDOW`, le entry piu' vecchie vengono rimosse nello stesso percorso
critico dell'inserimento. Non c'e' un thread separato di pulizia e non
c'e' bisogno di acquisire lock aggiuntivi.

**Costo**: la pulizia e' O(k log k) dove k e' il numero di entry da rimuovere
(tipicamente poche unita'). In pratica e' trascurabile.

### L2: Completamento della sequenza di retry

**Enunciato**: un client corretto deve poter completare una sequenza di retry
in tempo finito.

**Come e' garantita**: un client che usa numeri di sequenza monotoni crescenti
e ritenta entro la finestra di deduplicazione trovera' sempre la sua entry
nella `request_table`. Il server non rifiuta mai un retry valido (entro
finestra). L'unica causa di fallimento e' la scadenza della finestra, che il
contratto dichiara esplicitamente.

### L3: Memoria limitata

**Enunciato**: il server non puo' conservare per sempre tutti i `request_id`.

**Come e' garantita**: due meccanismi cooperano:
1. **Sliding window automatica**: al superamento di `MAX_WINDOW` entry per
   client, le piu' vecchie vengono rimosse.
2. **ACK cumulativo**: il comando `ACK client_id seq` permette al client di
   confermare esplicitamente la ricezione, scatenando la pulizia immediata
   delle entry fino a `seq`.

---

## Scelte di design e trade-off

### Request table in memoria vs. persistente

Abbiamo scelto una `request_table` **in memoria** (dizionario Python).

- **Pro**: semplicita', prestazioni O(1) per lookup e inserimento.
- **Contro**: al riavvio del server, la tabella viene persa. I retry
  dopo un riavvio non sono deduplicati.
- **Motivazione**: nel contesto didattico, la persistenza della tabella
  aggiungerebbe complessita' senza valore pedagogico. In un sistema
  di produzione, la tabella andrebbe persistita (es. su disco o DB).

### Lock globale vs. lock per chiave

Abbiamo scelto un **lock globale** per lo store e la `request_table`.

- **Pro**: correttezza semplice da ragionare. Non c'e' rischio di deadlock.
- **Contro**: throughput limitato sotto carico concorrente elevato. Tutte le
  operazioni sono serializzate.
- **Alternativa**: lock per chiave (o sharded lock) ridurrebbe la contesa
  ma complicherebbe la gestione della `request_table` che e' indicizzata
  per client, non per chiave.

### Sliding window vs. TTL temporale

Abbiamo scelto una **sliding window** (ultime N richieste per client)
invece di una scadenza temporale (TTL).

- **Pro**: prevedibile, non dipende dal clock. Il client sa che ha
  esattamente `MAX_WINDOW` richieste di margine.
- **Contro**: un client che invia poche richieste conserva le entry a lungo
  (nessuna scadenza temporale). Un client che ne invia molte perde le entry
  vecchie rapidamente.
- **Alternativa**: TTL (es. 60 secondi) sarebbe piu' uniforme ma introduce
  dipendenza dal tempo e complessita' nella pulizia (serve un timer o
  pulizia periodica).

### ACK cumulativo

Il comando `ACK client_id seq` conferma tutte le richieste fino a `seq`.

- **Pro**: un singolo ACK copre molte richieste. Semplice per il client.
- **Contro**: se il client perde la connessione prima di inviare l'ACK,
  le entry restano in memoria fino alla scadenza della finestra.
- **Alternativa**: ACK selettivo (per singolo `request_id`) sarebbe piu'
  preciso ma piu' verboso.

---

## Limiti rimasti

1. **Volatilita'**: al riavvio del server, sia lo store che la `request_table`
   vengono persi. Nessuna garanzia di durabilita'.

2. **Singolo nodo**: il sistema non e' distribuito. In un sistema multi-nodo,
   la `request_table` dovrebbe essere replicata o il routing dovrebbe
   garantire che le richieste di un client arrivino sempre allo stesso nodo.

3. **Client malevoli**: un client puo' esaurire la memoria del server
   creando molti `client_id` diversi. In produzione servirebbero limiti
   sul numero di client e/o autenticazione.

4. **Nessun timeout sulle entry orfane**: se un client si disconnette senza
   inviare ACK e senza superare la finestra, le sue entry restano in memoria
   indefinitamente (fino al riavvio del server).

---

## Possibili evoluzioni

1. **Persistenza della request_table** su disco (append-only log) per
   sopravvivere ai riavvii.
2. **TTL + sliding window combinati**: usare il piu' aggressivo dei due
   meccanismi di pulizia.
3. **Lock per chiave o per shard** per migliorare il throughput concorrente.
4. **Replicazione della request_table** in un sistema distribuito.
5. **Rate limiting** per client per prevenire abusi della tabella.
