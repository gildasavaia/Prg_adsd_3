# Homework 3 — Retry Idempotenti Con `request_id`

## Indice

1. [Panoramica del progetto](#panoramica-del-progetto)
2. [Problema affrontato](#problema-affrontato)
3. [Struttura del repository](#struttura-del-repository)
4. [Cosa è stato fatto — passo passo](#cosa-è-stato-fatto--passo-passo)
5. [Ruoli del gruppo](#ruoli-del-gruppo)
6. [Come eseguire](#come-eseguire)
7. [Proprietà di Safety e Liveness](#proprietà-di-safety-e-liveness)

---

## Panoramica del progetto

Questo homework estende un KV store versionato (con operazioni `GET`, `GETV`, `SET`, `CAS`, `DELETE`) aggiungendo la **semantica at-most-once** per le operazioni mutative. In un sistema distribuito, un client può perdere la risposta del server dopo aver inviato una scrittura: se ritenta "alla cieca", rischia di applicare due volte lo stesso effetto (es. incrementare due volte la versione). La soluzione consiste nell'associare a ogni richiesta mutativa un `request_id` univoco (`client_id:sequence_number`). Il server memorizza la risposta prodotta la prima volta, e se riceve la stessa richiesta una seconda volta restituisce la risposta salvata senza rieseguire il comando.

---

## Problema affrontato

```
Client                          Server
  |--- SET key value ------------>|   (il server esegue)
  |                               |
  |   <--- la risposta si perde   |
  |                               |
  |--- SET key value ------------>|   (il server riesegue → doppio effetto!)
```

Senza idempotenza, il secondo invio causa un secondo incremento di versione. Con il nostro sistema:

```
Client                          Server
  |--- SET_REQ clientA:42 key v ->|   (esegue, salva in request_table)
  |                               |
  |   <--- risposta si perde      |
  |                               |
  |--- SET_REQ clientA:42 key v ->|   (trova clientA:42 nella tabella → replay)
  |<--- OK version=0 ------------|   (stessa risposta, nessun doppio effetto)
```

---

## Struttura del repository

| File | Descrizione |
| --- | --- |
| `contratto.md` | Contratto pubblico: comandi, risposte, precondizioni, postcondizioni e casi fuori contratto. |
| `server.py` | Server TCP multi-thread con KV store versionato e tabella di deduplicazione. |
| `client.py` | Client con generazione automatica di `request_id` e retry con backoff. Usabile in modalità interattiva o programmatica. |
| `test_idempotenza.py` | Test dei casi nominali di idempotenza: SET, CAS, DELETE con replay e verifica che la versione non venga incrementata. |
| `test_gc.py` | Test della garbage collection: sliding window automatica e ACK esplicito. |
| `test_stress.py` | Stress test concorrente: più client simultanei con retry randomici, verifica di coerenza del valore finale. |
| `test_client.py` | Test end-to-end del client reale (`IdempotentClient`) con un chaos proxy TCP che simula guasti di rete e verifica il retry automatico. |
| `nota_tecnica.md` | Trade-off scelti, limiti del sistema e possibili evoluzioni. |
| `README.md` | Questo file. |

---

## Cosa è stato fatto — passo passo

### Passo 1 — Definizione del contratto pubblico (`contratto.md`)

Si è definito il protocollo esteso del KV store. Per ogni comando mutativo (`SET_REQ`, `CAS_REQ`, `DELETE_REQ`) sono stati specificati:

- Il formato del `request_id` come `client_id:sequence_number`
- La risposta per ogni caso: successo, replay, request_id scaduto, formato errato, argomenti mancanti
- Il comando `ACK` per la pulizia esplicita della tabella di deduplicazione
- Le precondizioni (formato valido, seq monotono, dentro la finestra)
- Le postcondizioni (esecuzione una sola volta, replay identico)
- I casi fuori contratto (client_id riusato, salti all'indietro)

Sono state dichiarate anche le proprietà di **safety** (no doppio effetto, coerenza del replay, non confusione tra richieste) e di **liveness** (GC non bloccante, completamento dei retry, memoria limitata).

### Passo 2 — Implementazione del server (`server.py`)

Si è implementato il server come estensione del KV store versionato dei laboratori. Le scelte architetturali principali:

1. **Classe `IdempotentVersionedStore`**: gestisce lo store (`_data`), la tabella di deduplicazione (`_request_table`) e l'ack cumulativo per client (`_client_ack`).

2. **Pattern di dispatch idempotente** (`_dispatch_idempotent`): metodo che fattorizza la logica comune a tutti i comandi mutanti:
   - Parsing del `request_id`
   - Check nella tabella (sotto lock) → se trovato, replay
   - Se nuovo → esecuzione della lambda specifica, salvataggio della risposta, garbage collection automatica

3. **Lock globale**: un singolo `threading.Lock()` protegge sia lo store che la `request_table`. Scelta di semplicità (nessun rischio di deadlock) a scapito del throughput.

4. **Garbage collection a sliding window**: quando la tabella di un client supera `MAX_WINDOW` (default: 100) entry, le più vecchie vengono rimosse automaticamente. L'ack cumulativo viene aggiornato di conseguenza.

5. **Comando `ACK`**: permette al client di confermare esplicitamente la ricezione delle risposte, scatenando la pulizia immediata delle entry con `seq <= ack`.

6. **Multi-thread**: un thread daemon per ogni connessione client (TCP, un comando per riga).

### Passo 3 — Implementazione del client (`client.py`)

Si è implementato un client in due modalità:

1. **Classe `IdempotentClient`** (per uso programmatico e nei test):
   - Genera automaticamente `request_id` con contatore `_seq` monotono crescente
   - Metodi `set_req()`, `cas_req()`, `delete_req()` con retry automatico (backoff lineare, riconnessione dopo timeout)
   - Metodo `ack()` per confermare la ricezione cumulativa
   - Context manager (`with` statement) per apertura/chiusura della connessione

2. **Modalità interattiva** (`main()`):
   - Prompt da terminale (`kv> `)
   - L'utente digita `SET_REQ key value` senza specificare il `request_id`: il client lo genera automaticamente
   - Comandi non mutanti (`GET`, `GETV`, `PING`, `KEYS`, `QUIT`) vengono inviati direttamente

### Passo 4 — Test di idempotenza (`test_idempotenza.py`)

Scenari testati:

| # | Scenario | Cosa verifica |
| --- | --- | --- |
| 1 | `SET_REQ` + replay | La risposta è identica e la versione non viene incrementata |
| 2 | `CAS_REQ` + replay | La risposta è identica e la versione non viene incrementata |
| 3 | `DELETE_REQ` + replay | Il replay restituisce `OK` (non `NOT_FOUND`) perché riporta la risposta salvata |
| 4 | Due `SET_REQ` diverse sulla stessa chiave | Le richieste con `request_id` diversi producono effetti distinti |
| 5 | `CAS_REQ` fallita + replay | Anche il replay di un errore (`ERR version_mismatch`) restituisce lo stesso errore |
| 6 | Formato errato del `request_id` | `ERR invalid_request_id` per vari formati errati (no `:`, seq non numerica, client vuoto) |

### Passo 5 — Test della garbage collection (`test_gc.py`)

Scenari testati:

| # | Scenario | Cosa verifica |
| --- | --- | --- |
| 1 | Invio di `MAX_WINDOW + 20` richieste | La sliding window funziona senza errori |
| 2 | Replay di una richiesta recente | Ancora in finestra → replay corretto |
| 3 | Replay di una richiesta scaduta (seq=0) | `ERR request_id_expired` perché la GC l'ha rimossa |
| 4 | ACK esplicito + replay pre/post ACK | Le entry con `seq <= ack` diventano `expired`, le altre restano valide |
| 5 | ACK con formato errato | Errori su argomenti mancanti e seq non numerica |

### Passo 6 — Stress test concorrente (`test_stress.py`)

Si è costruito un test con:

- **5 client concorrenti** (thread separati), ciascuno con una propria connessione TCP
- **20 operazioni `SET_REQ`** per client su una stessa chiave
- **30% di probabilità di retry** per ogni operazione (fino a 3 retry extra)
- Verifica che **ogni retry restituisca la stessa risposta** dell'invio originale
- Verifica finale: la **versione della chiave** corrisponde al numero totale di operazioni uniche (non dei retry)

Questo test dimostra che sotto carico concorrente, con retry randomici e interleaving non deterministico, la semantica at-most-once viene rispettata.

**Test aggiuntivo — collisione esatta sulla sezione critica**: dopo lo stress test principale, `test_stress.py` esegue anche `run_exact_collision_test`, uno scenario più mirato in cui **10 thread inviano contemporaneamente lo stesso identico `request_id`** sulla stessa chiave (non `request_id` diversi come nello stress test principale). Questo simula il caso peggiore realistico: più retry della stessa richiesta in volo nello stesso istante, ad esempio quando il timeout del client scatta mentre la prima risposta è ancora in transito. Il test verifica che tutte le risposte concorrenti siano identiche e che l'effetto sia applicato una sola volta, esercitando direttamente l'atomicità del blocco `with self._lock:` in `_dispatch_idempotent` nel caso di vera collisione simultanea, non solo di retry sequenziali.

### Passo 7 — Test end-to-end del client con chaos proxy (`test_client.py`)

A differenza degli altri test che usano socket raw per verificare il protocollo, questo test esercita il codice di produzione del client (`IdempotentClient` di `client.py`). Un **chaos proxy TCP** si interpone tra client e server e può selettivamente droppare le risposte, forzando il client a seguire il suo percorso di retry reale (`_send_with_retry` → `_reconnect`).

Scenari testati:

| # | Scenario | Cosa verifica |
| --- | --- | --- |
| 1 | `set_req()` senza fault | L'API client funziona end-to-end attraverso il proxy |
| 2 | `set_req()` con drop | Il retry automatico produce la stessa risposta; la versione non è incrementata dal retry |
| 3 | `cas_req()` con drop | CAS idempotente dopo reconnect |
| 4 | `delete_req()` con drop | DELETE idempotente dopo reconnect |
| 5 | `cas_req()` fallita con drop | Anche il retry di un errore restituisce lo stesso errore dopo reconnect |
| 6 | `ack()` via API client | ACK cumulativo automatico |
| 7 | Operazione post-ACK | Il contatore `_seq` prosegue correttamente dopo l'ACK |

### Passo 8 — Nota tecnica (`nota_tecnica.md`)

Si è documentato:

- La **promessa introdotta** (at-most-once semantics) e il **costo tecnico** accettato (memoria, overhead, finestra finita, lock globale)
- Le **proprietà di safety** con dimostrazione informale di come sono garantite
- Le **proprietà di liveness** con spiegazione dei meccanismi che le garantiscono
- I **trade-off** scelti: request table in memoria vs. persistente, lock globale vs. per chiave, sliding window vs. TTL, ACK cumulativo vs. selettivo
- I **limiti rimasti**: volatilità al riavvio, singolo nodo, client malevoli, entry orfane
- Le **possibili evoluzioni**: persistenza, TTL + sliding window, lock per shard, replicazione, rate limiting

---

## Ruoli del gruppo

### Protocol Owner

**Responsabilità**: definire l'interfaccia pubblica, le risposte, le precondizioni e i casi fuori contratto.

**Cosa ha fatto**:

- Ha scritto il **contratto pubblico** (`contratto.md`) definendo:
  - Il formato del `request_id` (`client_id:sequence_number`) e le regole di monotonia
  - Per ogni comando mutativo (`SET_REQ`, `CAS_REQ`, `DELETE_REQ`): la tabella completa dei casi e delle risposte
  - Il comando `ACK` per la garbage collection esplicita con semantica cumulativa
  - Le precondizioni (formato, sequenza, finestra) e postcondizioni (esecuzione unica, replay identico)
  - I casi fuori contratto (client_id riusato, salti all'indietro, formato errato)
- Ha definito le **proprietà di safety** nel contratto:
  - S1: No doppio effetto (stessa richiesta → un solo effetto sullo store)
  - S2: Coerenza del replay (risposta replay identica all'originale)
  - S3: Non confusione (deduplicazione su `request_id`, non sulla chiave)
- Ha definito le **proprietà di liveness**:
  - L1: GC non bloccante
  - L2: Completamento della sequenza di retry
  - L3: Memoria limitata

---

### Implementation Owner

**Responsabilità**: coordinare il codice, l'integrazione e la coerenza con lo stile dei laboratori.

**Cosa ha fatto**:

- Ha implementato il **server** (`server.py`):
  - Classe `IdempotentVersionedStore` con store versionato e tabella di deduplicazione
  - Pattern `_dispatch_idempotent()` che fattorizza la logica di check/execute/save per tutti i comandi mutanti
  - Parsing del `request_id` con validazione (`_parse_request_id`)
  - Logica di deduplicazione (`_check_dedup`): distingue tra replay (risposta salvata), expired (già garbage-collected) e nuovo
  - Salvataggio + garbage collection automatica (`_save_and_gc`) a sliding window
  - Handler per `ACK` con pulizia esplicita e aggiornamento dell'ack cumulativo
  - Server TCP multi-thread con logging timestampato
- Ha implementato il **client** (`client.py`):
  - Classe `IdempotentClient` con contatore di sequenza monotono
  - Retry con backoff lineare e riconnessione automatica
  - Modalità interattiva con prompt e generazione automatica dei `request_id`
  - Context manager per gestione corretta della connessione
- Ha garantito la **coerenza con lo stile dei laboratori**: protocollo testuale su TCP, un comando per riga, risposte `OK`/`ERR`/`NOT_FOUND`, naming convention e struttura coerente

---

### Fault/Test Owner

**Responsabilità**: costruire test, scenari di guasto, interleaving e stress.

**Cosa ha fatto**:

- Ha scritto **`test_idempotenza.py`** (6 scenari, 15 asserzioni):
  - Verifica del caso nominale per ogni comando mutativo (SET, CAS, DELETE)
  - Verifica che il replay restituisca la stessa risposta senza rieseguire
  - Verifica che `GETV` confermi la versione non incrementata
  - Test della non-confusione: richieste diverse sulla stessa chiave
  - Test del replay di un errore (`CAS_REQ` fallita per version mismatch)
  - Test del formato errato del `request_id`
- Ha scritto **`test_gc.py`** (5 scenari, 10 asserzioni):
  - Riempimento della finestra con `MAX_WINDOW + 20` richieste
  - Verifica del boundary: prima entry viva vs. prima entry scaduta
  - Verifica dell'ACK esplicito: entry pre-ACK scadute, entry post-ACK vive
  - Test dei casi errati del comando `ACK`
- Ha scritto **`test_stress.py`** (test concorrente):
  - 5 client concorrenti × 20 operazioni con 30% di probabilità di retry
  - Verifica runtime: ogni retry restituisce la stessa risposta dell'originale
  - Verifica post-hoc: la versione finale della chiave corrisponde al numero di operazioni uniche
  - Simulazione di interleaving non deterministico sotto carico
- Ha scritto **`test_client.py`** (test end-to-end con chaos proxy):
  - Chaos proxy TCP che droppa selettivamente le risposte del server
  - Esercita il codice reale di `IdempotentClient` (`_send_with_retry`, `_reconnect`)
  - Verifica SET, CAS, DELETE con fault injection e retry automatico
  - Verifica che l'errore di un CAS fallito sia preservato anche dopo drop + retry
  - Verifica ACK via API client e continuità dei sequence number

---

### Reviewer / Architect

**Responsabilità**: verificare coerenza tra contratto, implementazione, test e limiti dichiarati.

**Cosa ha fatto**:

- Ha verificato la **coerenza tra contratto e implementazione**:
  - Ogni caso del contratto (`contratto.md`) ha un handler corrispondente nel server
  - Le risposte prodotte dal server corrispondono esattamente a quelle dichiarate nel contratto
  - Le precondizioni sono controllate nell'ordine corretto (formato → finestra → deduplicazione)
- Ha verificato la **coerenza tra contratto e test**:
  - Ogni scenario del contratto (successo, replay, expired, formato errato) ha almeno un test corrispondente
  - I test di GC verificano sia la sliding window automatica che l'ACK esplicito
  - Lo stress test verifica la proprietà S1 (no doppio effetto) sotto interleaving concorrente
- Ha verificato la **coerenza tra safety dichiarata e implementazione**:
  - S1 (no doppio effetto): garantita dalla lookup nella `request_table` prima dell'esecuzione, sotto lock
  - S2 (coerenza replay): garantita dal salvataggio atomico di effetto + risposta sotto lo stesso lock
  - S3 (non confusione): garantita dall'indicizzazione su `request_id`, non sulla chiave
- Ha scritto la **nota tecnica** (`nota_tecnica.md`) documentando:
  - Trade-off scelti con pro/contro e alternative considerate
  - Limiti rimasti con analisi onesta (volatilità, singolo nodo, client malevoli)
  - Possibili evoluzioni ordinate per complessità crescente
- Ha verificato la **domanda da difendere**: "quale promessa nuova introduce il sistema e quale costo tecnico avete accettato per mantenerla?" → at-most-once semantics al costo di memoria aggiuntiva, finestra finita e lock globale

---

## Come eseguire

### 1. Avviare il server

```bash
python server.py
```

Il server si mette in ascolto su `127.0.0.1:6460`.

### 2. Client interattivo

```bash
python client.py --client-id mio_client
```

Comandi disponibili dal prompt `kv>`:

```
SET_REQ chiave valore       → genera automaticamente il request_id
CAS_REQ chiave versione val → genera automaticamente il request_id
DELETE_REQ chiave            → genera automaticamente il request_id
GET chiave                   → lettura diretta (no request_id)
GETV chiave                  → lettura con versione
ACK                          → conferma ricezione cumulativa
QUIT                         → chiudi
```

### 3. Eseguire i test

Con il server in esecuzione in un terminale separato:

```bash
# Test di idempotenza (casi nominali)
python test_idempotenza.py

# Test della garbage collection
python test_gc.py

# Stress test concorrente
python test_stress.py

# Test end-to-end del client con chaos proxy
python test_client.py
```

> **Nota**: tutti i test sono ripetibili senza riavviare il server. Ogni esecuzione genera identificatori unici (UUID) per client e chiavi, evitando collisioni con run precedenti.

---

## Proprietà di Safety e Liveness

### Safety

| ID | Proprietà | Come è garantita |
| --- | --- | --- |
| S1 | **No doppio effetto** — la stessa richiesta mutativa non produce effetti doppi | Lookup nella `request_table` prima dell'esecuzione, sotto lock globale |
| S2 | **Coerenza del replay** — la risposta replayed è identica all'originale | Salvataggio atomico di effetto + risposta sotto lo stesso lock |
| S3 | **Non confusione** — richieste diverse non si confondono | Deduplicazione su `request_id` (coppia `client_id:seq`), non sulla chiave |

### Liveness

| ID | Proprietà | Come è garantita |
| --- | --- | --- |
| L1 | **GC non blocca il servizio** | Pulizia inline durante l'inserimento, senza stop-the-world |
| L2 | **Completamento della sequenza di retry** | Client con seq monotoni entro la finestra trova sempre la sua entry |
| L3 | **Memoria limitata** | Sliding window (max N entry) + ACK cumulativo per pulizia esplicita |