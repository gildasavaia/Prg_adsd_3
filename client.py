#!/usr/bin/env python3
"""
Client idempotente per KV store con retry automatici.

Obiettivo:
- generare request_id automaticamente (client_id:seq) per ogni operazione mutativa
- garantire retry safe in caso di timeout o disconnessione di rete
- supportare sia un'API programmatica (IdempotentClient) che una CLI interattiva (main)

Struttura del modulo:
- IdempotentClient: classe principale con gestione connessione, seq e retry
- parse_args: parsing CLI
- main: loop interattivo da terminale
"""

import argparse   # parsing degli argomenti da riga di comando
import socket     # connessioni TCP
import time       # sleep per il backoff tra retry
from datetime import datetime   # timestamp nel log


# Indirizzo del server (loopback = locale); deve corrispondere a HOST in server.py
HOST = "127.0.0.1"
# Porta TCP; deve corrispondere a PORT in server.py
PORT = 6460

# Timeout in secondi oltre cui una risposta e' considerata persa.
# Se il server non risponde entro questo tempo, scatta il retry.
DEFAULT_TIMEOUT = 2.0

# Numero massimo di tentativi retry per una singola operazione mutativa.
# Con backoff lineare 0.1*attempt, 3 tentativi = max ~0.3s di attesa totale.
DEFAULT_MAX_RETRIES = 3


def log(message: str) -> None:
    """Log semplice con timestamp per debug retry e riconnessioni."""
    # Formatta l'ora corrente con millisecondi (es. 14:05:32.123)
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    # Stampa il messaggio preceduto dal timestamp
    print(f"[{timestamp}] {message}")


class IdempotentClient:
    """
    Client TCP con supporto nativo all'idempotenza.

    Caratteristiche principali:

    1. request_id automatico
       Ogni operazione mutativa riceve un request_id nel formato
       'client_id:seq'. Il contatore _seq e' monotono crescente e non
       viene mai decrementato, nemmeno dopo un retry fallito.

    2. Retry safe
       In caso di timeout o errore di rete, il metodo _send_with_retry
       riprova con lo STESSO comando (stesso request_id). Il server usa
       il request_id per riconoscere il retry e rispondere dalla tabella
       di deduplicazione, senza rieseguire l'operazione.

    3. Separazione mutativi / letture
       - Comandi mutativi (set_req, cas_req, delete_req): usano _send_with_retry
       - Comandi di lettura (get, getv, ping, keys): usano send_raw (no retry)
       - ACK: usa send_raw (l'ACK e' idempotente per natura)

    4. Reconnect automatico
       Dopo un errore di rete, _reconnect() chiude e riapre la connessione TCP
       prima del prossimo tentativo.

    Uso programmatico (consigliato nei test):
        with IdempotentClient("clientA") as c:
            c.set_req("key", "value")
            c.getv("key")

    Uso diretto:
        c = IdempotentClient("clientA")
        c.connect()
        c.set_req("key", "value")
        c.close()
    """

    def __init__(
        self,
        client_id: str,
        host: str = HOST,
        port: int = PORT,
        timeout: float = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
    ) -> None:
        # Identificatore univoco del client; usato come prefisso dei request_id
        self.client_id = client_id
        # Indirizzo del server a cui connettersi
        self.host = host
        # Porta del server
        self.port = port
        # Secondi di attesa massima per ogni risposta del server
        self.timeout = timeout
        # Massimo numero di tentativi per le operazioni mutative
        self.max_retries = max_retries

        # Contatore sequenziale per i request_id.
        # Parte da 0 e viene incrementato PRIMA dell'invio in _next_request_id().
        # Non viene mai resettato (nemmeno dopo ACK o disconnessione).
        # Invariante: _seq e' sempre il prossimo seq da usare, non quello corrente.
        self._seq: int = 0

        # Connessione TCP e wrapper file buffered (None = non connesso)
        self._connection: socket.socket | None = None
        self._file = None

    # ------------------------------------------------------------------
    # Gestione connessione
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """
        Apre la connessione TCP verso il server.
        Imposta il timeout: dopo 'self.timeout' secondi senza risposta,
        readline() lancia socket.timeout, che _send_with_retry cattura.
        """
        # Crea e connette il socket TCP verso (host, port)
        self._connection = socket.create_connection((self.host, self.port))
        # Imposta il timeout di I/O: ogni read/write blocca al massimo timeout secondi
        self._connection.settimeout(self.timeout)
        # makefile("rwb"): wrapper buffered in lettura/scrittura binaria.
        # Permette readline() e write() con semantica file.
        self._file = self._connection.makefile("rwb")

    def close(self) -> None:
        """
        Chiude la connessione in modo sicuro.
        I try/except silenziosi evitano eccezioni se la connessione
        e' gia' stata chiusa dal server o da un errore precedente.
        """
        if self._file:
            try:
                # Chiude il wrapper file (svuota il buffer e chiude lo stream)
                self._file.close()
            except Exception:
                # Ignora errori: il file potrebbe già essere chiuso
                pass

        if self._connection:
            try:
                # Chiude il socket TCP sottostante
                self._connection.close()
            except Exception:
                # Ignora errori: il socket potrebbe già essere chiuso
                pass

        # Reset dei riferimenti: connect() potra' creare una nuova connessione
        self._connection = None
        self._file = None

    def _reconnect(self) -> None:
        """
        Chiude la connessione corrente e ne apre una nuova.
        Chiamata da _send_with_retry tra un tentativo e il successivo.
        Il nuovo socket riceve lo stesso timeout del vecchio.
        """
        # Prima chiude tutto in modo pulito
        self.close()
        # Poi riapre la connessione con i parametri originali
        self.connect()

    # ------------------------------------------------------------------
    # Generazione request_id
    # ------------------------------------------------------------------

    def _next_request_id(self) -> str:
        """
        Genera e restituisce il prossimo request_id univoco.

        Formato: 'client_id:sequence_number'
        Esempio: 'clientA:42'

        IMPORTANTE: _seq viene incrementato qui, UNA SOLA VOLTA per operazione.
        Se l'operazione viene ritentata, il chiamante usa il request_id
        gia' generato (non chiama _next_request_id di nuovo).
        """
        # Costruisce il request_id con il seq corrente prima di incrementarlo
        rid = f"{self.client_id}:{self._seq}"
        # Incrementa il contatore: il prossimo request_id avrà seq+1
        self._seq += 1
        # Restituisce il request_id appena generato
        return rid

    # ------------------------------------------------------------------
    # Invio messaggi
    # ------------------------------------------------------------------

    def send_raw(self, command: str) -> str:
        """
        Invia un comando al server senza logica di retry.

        Usato per:
        - Comandi di lettura (GET, GETV, PING, KEYS): sicuri da rieseguire
          anche senza idempotenza, perche' non hanno effetti sullo store
        - ACK: idempotente per natura (il server usa max(), non decrementa)
        - QUIT: non ha senso ritentare una chiusura connessione

        Lancia ConnectionError se la connessione non e' aperta o se il
        server chiude la connessione prima di rispondere.
        """
        # Verifica che la connessione sia aperta prima di scrivere
        if self._file is None:
            raise ConnectionError("Not connected")

        # Codifica il comando in UTF-8 e aggiunge il newline terminatore di riga
        self._file.write((command + "\n").encode("utf-8"))
        # necessario: il buffer non si svuota da solo; flush forza l'invio immediato
        self._file.flush()

        # Attende la risposta dal server (bloccante fino a timeout o EOF)
        response = self._file.readline()
        # b"" = EOF: il server ha chiuso la connessione senza rispondere
        if not response:
            raise ConnectionError("Connection closed by server")

        # rstrip("\n") rimuove il newline terminatore del protocollo
        return response.decode("utf-8", errors="replace").rstrip("\n")

    def _send_with_retry(self, command: str) -> str:
        """
        Invia un comando con retry automatico in caso di errore di rete.

        PROPRIETA' FONDAMENTALE: il parametro 'command' contiene gia' il
        request_id generato da _next_request_id(). Tra un tentativo e il
        successivo, il comando e' IDENTICO (stesso request_id). Il server
        riconosce il retry dalla request_table e risponde senza rieseguire.

        Strategia di retry:
        - Tenta max_retries volte
        - Tra un tentativo e il successivo: sleep con backoff lineare (0.1*attempt)
          e _reconnect() per aprire un nuovo socket TCP
        - Se tutti i tentativi falliscono: lancia ConnectionError

        Eccezioni catturate (triggherano il retry):
        - socket.timeout: nessuna risposta entro self.timeout secondi
        - ConnectionError: readline ha restituito b"" (connessione chiusa)
        - OSError: errore generico di rete (reset, connessione rifiutata, ecc.)

        Eccezioni NON catturate (non triggerano retry):
        - Risposte "ERR ..." dal server: sono risposte valide di protocollo,
          non errori di rete. Vengono restituite al chiamante normalmente.

        Backoff lineare (attempt parte da 1):
          Tentativo 1: invio diretto, nessun sleep
          Tentativo 2: sleep 0.1s, poi reconnect
          Tentativo 3: sleep 0.2s, poi reconnect
        """
        # Tiene traccia dell'ultimo errore per il messaggio finale
        last_error: Exception | None = None

        # Itera da 1 a max_retries incluso
        for attempt in range(1, self.max_retries + 1):
            try:
                # Connette se necessario (primo invio o dopo una reconnect)
                if self._connection is None or self._file is None:
                    self.connect()

                # Invia il comando (stesso per tutti i tentativi, stesso request_id)
                self._file.write((command + "\n").encode("utf-8"))
                # flush() forza l'invio immediato attraverso il buffer
                self._file.flush()

                # Aspetta la risposta (blocca fino a timeout o EOF)
                response = self._file.readline()
                # b"" = EOF: il server ha chiuso la connessione prematuramente
                if not response:
                    raise ConnectionError("Connection closed by server")

                # Risposta ricevuta correttamente: esce dal loop di retry
                return response.decode("utf-8", errors="replace").rstrip("\n")

            except (socket.timeout, ConnectionError, OSError) as exc:
                # Salva l'errore per il messaggio finale
                last_error = exc
                # Logga il fallimento del tentativo corrente
                log(f"retry {attempt}/{self.max_retries} failed: {exc}")

                # Prepara il prossimo tentativo (non l'ultimo: inutile dormire)
                if attempt < self.max_retries:
                    # Backoff lineare: 0.1s al primo, 0.2s al secondo, ecc.
                    time.sleep(0.1 * attempt)
                    # Riapre il socket TCP per il tentativo successivo
                    self._reconnect()

        # Tutti i tentativi esauriti: lancia eccezione con il dettaglio dell'ultimo errore
        raise ConnectionError(
            f"All retries failed. Last error: {last_error}"
        )

    # ------------------------------------------------------------------
    # API comandi mutativi (idempotenti)
    # ------------------------------------------------------------------

    def set_req(self, key: str, value: str) -> str:
        """
        SET idempotente: crea o sovrascrive la chiave col valore dato.

        Genera automaticamente il request_id ('client_id:seq') e invia
        SET_REQ con retry. Il server risponde 'OK version=<v>' o replaya
        la risposta originale se e' un retry.

        NOTA: value puo' contenere spazi (split(" ", 2) nel server limita
        a 2 split, quindi il valore arriva integro).
        """
        # Genera un request_id univoco e incrementa il contatore interno
        rid = self._next_request_id()
        # Costruisce il comando completo e lo invia con retry automatico
        return self._send_with_retry(f"SET_REQ {rid} {key} {value}")

    def cas_req(self, key: str, expected_version: int, value: str) -> str:
        """
        CAS idempotente: aggiorna la chiave solo se la versione corrente
        corrisponde a expected_version.

        Ritorna 'OK version=<v>' se riuscito, 'ERR version_mismatch current=<v>'
        altrimenti. Anche la risposta di errore viene ritornata identica in caso
        di retry (il server la salva in request_table come qualsiasi altra risposta).
        """
        # Genera un request_id univoco per questa operazione CAS
        rid = self._next_request_id()
        # Costruisce il comando con versione attesa e invia con retry
        return self._send_with_retry(
            f"CAS_REQ {rid} {key} {expected_version} {value}"
        )

    def delete_req(self, key: str) -> str:
        """
        DELETE idempotente: elimina la chiave dallo store.

        Ritorna 'OK' se la chiave esisteva, 'NOT_FOUND' se non esisteva.
        In caso di retry, viene restituita sempre la risposta della prima
        esecuzione (anche se la chiave nel frattempo e' gia' stata eliminata).
        """
        # Genera un request_id univoco per questa operazione di cancellazione
        rid = self._next_request_id()
        # Costruisce il comando con il request_id e invia con retry
        return self._send_with_retry(f"DELETE_REQ {rid} {key}")

    # ------------------------------------------------------------------
    # API comando ACK
    # ------------------------------------------------------------------

    def ack(self, up_to_seq: int | None = None) -> str:
        """
        Invia un ACK cumulativo al server: conferma la ricezione di tutte
        le risposte fino a up_to_seq incluso.

        Permette al server di liberare le entry corrispondenti dalla
        request_table (garbage collection esplicita).

        Se up_to_seq e' None (default), conferma tutte le richieste
        inviate finora: up_to_seq = self._seq - 1.

        Caso speciale: se non e' ancora stata inviata nessuna richiesta
        (_seq = 0), up_to_seq diventa -1 e il client risponde localmente
        senza contattare il server (non c'e' niente da confermare).

        NOTA: usa send_raw (non _send_with_retry) perche' l'ACK e'
        idempotente per natura: il server usa max() su _client_ack,
        quindi ripetere lo stesso ACK e' sempre sicuro.
        """
        if up_to_seq is None:
            # Conferma tutte le richieste inviate fino a questo momento
            # _seq punta alla prossima, quindi l'ultima inviata è _seq - 1
            up_to_seq = self._seq - 1

        if up_to_seq < 0:
            # Nessuna richiesta da confermare ancora (nessun mutativo inviato)
            return "OK (nothing to ack)"

        # Invia l'ACK cumulativo al server tramite send_raw (no retry necessario)
        return self.send_raw(f"ACK {self.client_id} {up_to_seq}")

    # ------------------------------------------------------------------
    # API comandi di lettura (no retry, no request_id)
    # ------------------------------------------------------------------

    def get(self, key: str) -> str:
        """Legge il valore di una chiave. Sicuro da rieseguire: non mutativo."""
        # Invia GET direttamente senza retry: non ha effetti sullo store
        return self.send_raw(f"GET {key}")

    def getv(self, key: str) -> str:
        """
        Legge valore e versione di una chiave.
        Utile per ottenere la versione da usare in una CAS_REQ successiva.
        """
        # Invia GETV direttamente senza retry: non ha effetti sullo store
        return self.send_raw(f"GETV {key}")

    def ping(self) -> str:
        """Verifica che il server sia attivo e risponda."""
        # Invia PING direttamente: comando di diagnostica, nessun effetto
        return self.send_raw("PING")

    def keys(self) -> str:
        """Lista di tutte le chiavi presenti nello store."""
        # Invia KEYS direttamente: sola lettura, sicuro senza retry
        return self.send_raw("KEYS")

    # ------------------------------------------------------------------
    # Context manager (for use with 'with' statement)
    # ------------------------------------------------------------------

    def __enter__(self):
        """Apre la connessione all'ingresso del blocco 'with'."""
        # Equivalente a chiamare connect() prima del blocco
        self.connect()
        # Restituisce se stesso per permettere 'as client'
        return self

    def __exit__(self, *args):
        """Chiude la connessione all'uscita del blocco 'with' (anche in caso di eccezione)."""
        # Garantisce la chiusura del socket anche se il blocco solleva un'eccezione
        self.close()


# ------------------------------------------------------------------
# CLI interattiva
# ------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    """Parsing dei parametri da riga di comando per la modalita' interattiva."""
    # Crea il parser con una descrizione leggibile dall'help
    parser = argparse.ArgumentParser(description="KV store client idempotente")

    # Argomento opzionale: host del server (default: 127.0.0.1)
    parser.add_argument("--host", default=HOST)
    # Argomento opzionale: porta del server (default: 6460)
    parser.add_argument("--port", type=int, default=PORT)
    # Argomento opzionale: identificatore univoco del client (default: "interactive")
    parser.add_argument("--client-id", default="interactive",
                        help="Identificatore univoco del client (default: 'interactive')")
    # Argomento opzionale: timeout in secondi per la risposta del server
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT,
                        help="Timeout in secondi per ogni risposta del server")
    # Argomento opzionale: numero massimo di retry per operazione mutativa
    parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES,
                        help="Numero massimo di retry per operazione mutativa")

    # Analizza sys.argv e restituisce il namespace con i valori parsati
    return parser.parse_args()


def main() -> None:
    """
    CLI interattiva: legge comandi da stdin e li invia al server.

    Per i comandi mutativi (SET_REQ, CAS_REQ, DELETE_REQ) l'utente NON
    specifica il request_id: il client lo genera automaticamente.
    Esempio: l'utente digita 'SET_REQ chiave valore', il client invia
             'SET_REQ interactive:0 chiave valore' al server.

    Per i comandi di lettura e diagnostica (GET, GETV, PING, KEYS, ACK)
    il comando viene inviato direttamente con send_raw.
    """
    # Parsa gli argomenti da riga di comando
    args = parse_args()

    # Usa il context manager per garantire la chiusura del socket al termine
    with IdempotentClient(
        client_id=args.client_id,
        host=args.host,
        port=args.port,
        timeout=args.timeout,
        max_retries=args.max_retries,
    ) as client:

        # Stampa le informazioni di connessione e le istruzioni di utilizzo
        print(f"Connected to {args.host}:{args.port}")
        print(f"Client ID: {args.client_id}")
        print("SET_REQ / CAS_REQ / DELETE_REQ generano request_id automatici")
        print("Comandi raw: GET, GETV, PING, KEYS, ACK, QUIT")
        print()

        while True:
            try:
                # Legge una riga dall'utente con il prompt "kv> "
                line = input("kv> ")
            except EOFError:
                # Ctrl+D: tratta come QUIT (nessun input disponibile)
                line = "QUIT"
                print()

            # Rimuove spazi e newline attorno all'input
            cmd = line.strip()
            # Riga vuota: torna al prompt senza fare nulla
            if not cmd:
                continue

            # Uscita immediata: invia QUIT al server e chiude il loop
            if cmd.upper() == "QUIT":
                try:
                    # Prova a inviare QUIT: il server risponde "OK BYE" e chiude
                    print(client.send_raw("QUIT"))
                except Exception:
                    # Se il server è già chiuso, ignora l'errore
                    pass
                break

            # Divide il comando dagli argomenti al primo spazio
            parts = cmd.split(" ", 1)
            # Normalizza il nome del comando in maiuscolo
            op = parts[0].upper()
            # args_blob contiene tutto ciò che viene dopo il comando (o "" se assente)
            args_blob = parts[1] if len(parts) > 1 else ""

            if op == "SET_REQ":
                # L'utente fornisce solo 'key value'; il request_id e' generato
                try:
                    # Divide key e value al primo spazio (value può contenere spazi)
                    k, v = args_blob.split(" ", 1)
                    # Invoca set_req che genera il request_id e gestisce i retry
                    print(client.set_req(k, v))
                except ValueError:
                    # args_blob non aveva almeno due token: formato sbagliato
                    print("Uso: SET_REQ <key> <value>")

            elif op == "CAS_REQ":
                # L'utente fornisce 'key expected_version value'
                try:
                    # Divide in tre parti: key, versione attesa, valore
                    k, ev, v = args_blob.split(" ", 2)
                    # Invoca cas_req con la versione convertita in intero
                    print(client.cas_req(k, int(ev), v))
                except ValueError:
                    # Formato sbagliato o expected_version non intero
                    print("Uso: CAS_REQ <key> <expected_version> <value>")

            elif op == "DELETE_REQ":
                # L'utente fornisce solo la chiave; il request_id e' generato
                # strip() per rimuovere spazi superflui attorno alla chiave
                print(client.delete_req(args_blob.strip()))

            elif op == "ACK":
                # Invia ACK cumulativo fino all'ultima richiesta inviata
                # ack() senza argomenti usa self._seq - 1 come limite
                print(client.ack())

            else:
                # Tutti gli altri comandi (GET, GETV, PING, KEYS) vengono
                # inviati direttamente senza modifiche tramite send_raw
                try:
                    # Invia il comando testuale al server e stampa la risposta
                    print(client.send_raw(cmd))
                except Exception as exc:
                    # Cattura errori di rete o connessione e li mostra all'utente
                    print(f"Errore: {exc}")


if __name__ == "__main__":
    # Punto di ingresso: avvia la CLI solo se eseguito direttamente
    main()