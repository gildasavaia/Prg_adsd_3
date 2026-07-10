# Proposte di Homework: KV Store e Contratti Distribuiti

Questo documento raccoglie cinque possibili homework di approfondimento sul
percorso KV store.

Ogni homework parte dagli argomenti visti a lezione, ma chiede di fare un passo
in piu': definire un contratto, implementarlo, difenderlo con test e discuterne
le proprieta' di safety e liveness.

## Organizzazione dei gruppi

Il lavoro e' pensato per gruppi da 3-4 persone.

Ruoli suggeriti:

| Ruolo | Responsabilita' |
| --- | --- |
| Protocol owner | Definisce interfaccia, risposte, precondizioni e casi fuori contratto. |
| Implementation owner | Coordina codice, integrazione e coerenza con lo stile dei lab. |
| Fault/test owner | Costruisce test, scenari di guasto, interleaving e stress. |
| Reviewer/architect | Verifica coerenza tra contratto, implementazione, test e limiti dichiarati. |

Nei gruppi da 3, il ruolo di reviewer puo' essere condiviso.

## Deliverable comuni

Ogni gruppo deve consegnare:

| Deliverable | Contenuto atteso |
| --- | --- |
| Contratto pubblico | Comandi, risposte, precondizioni, postcondizioni e casi fuori contratto. |
| Implementazione | Codice funzionante basato sui laboratori del KV store. |
| Safety/liveness note | Almeno 2 proprieta' di safety e 1 proprieta' di liveness. |
| Test ripetibili | Script o procedura automatizzabile con casi nominali e casi critici. |
| Nota tecnica | Trade-off scelti, limiti rimasti e possibili evoluzioni. |

## Homework 3: Retry Idempotenti Con `request_id`

### Obiettivo

Rendere sicuri i retry delle operazioni mutative.

In un sistema distribuito, un client puo' inviare una scrittura, perdere la
risposta e non sapere se il server l'abbia applicata. Se ritenta alla cieca,
rischia di applicare due volte lo stesso effetto.

### Interfaccia proposta

Esempi:

```text
SET_REQ clientA:42 key value
CAS_REQ clientA:43 key 7 value
DELETE_REQ clientA:44 key
```

Il `request_id` identifica univocamente una richiesta mutativa del client.

### Requisiti minimi

- il server deve ricordare l'esito delle richieste gia' viste;
- ripetere lo stesso `request_id` deve restituire la stessa risposta senza riapplicare l'effetto;
- il contratto deve dire quando un `request_id` puo' essere dimenticato;
- i test devono simulare almeno un retry dopo timeout del client.

### Safety

Proprieta' da discutere:

- la stessa richiesta mutativa non deve produrre effetti doppi;
- due richieste diverse non devono essere confuse solo perche' toccano la stessa chiave;
- il replay della risposta deve essere coerente con l'effetto gia' applicato.

### Liveness

Proprieta' da discutere:

- il server non puo' conservare per sempre tutti i `request_id`;
- la garbage collection dei request id non deve bloccare il servizio;
- un client corretto deve poter completare una sequenza di retry.

### Hint

Una soluzione base e':

```text
request_table[client_id][sequence_number] = response
```

Il punto difficile e' la pulizia.

Possibili strategie:

- conservare solo gli ultimi `N` request id per client;
- usare numeri di sequenza monotoni e un ack cumulativo;
- usare una scadenza temporale, dichiarando che oltre quella finestra il retry non e' piu' garantito.

## Criteri di valutazione suggeriti

| Criterio | Peso indicativo |
| --- | --- |
| Chiarezza del contratto | 25% |
| Correttezza dell'implementazione | 25% |
| Qualita' dei test sui casi critici | 25% |
| Discussione safety/liveness e trade-off | 20% |
| Organizzazione del gruppo e nota tecnica | 5% |

## Indicazione finale

Il lavoro non deve limitarsi ad aggiungere codice.

La domanda principale da difendere e':

> quale promessa nuova introduce il vostro sistema e quale costo tecnico avete
> accettato per mantenerla?

