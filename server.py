#!/usr/bin/env python3
"""
KV Store idempotente con retry safety.

Obiettivo:
- rendere le operazioni mutative safe anche con retry client
- garantire deduplicazione tramite request_id (client_id:seq)
- mantenere consistenza della versione nel KV store
- supportare concorrenza multi-thread
"""

import socket                    # modulo stdlib per connessioni TCP
import threading                 # modulo stdlib per thread e Lock
from datetime import datetime    # usato per i timestamp nel log
from typing import Callable      # annotazione di tipo per gli handler


# Indirizzo su cui il server ascolta (loopback = solo locale)
HOST = "127.0.0.1"
# Porta TCP del server; deve corrispondere a PORT in client.py
PORT = 6460

# Numero massimo di request_id memorizzati per client.
# Serve a limitare memoria e simulare una sliding window.
# Dopo questo limite, le richieste più vecchie vengono eliminate.
MAX_WINDOW = 100

# Alias di tipo: un handler riceve gli argomenti testuali del comando
# e restituisce (risposta_testuale, flag_chiudi_connessione)
CommandHandler = Callable[[str], tuple[str, bool]]


def log(message: str) -> None:
    """
    Logging di debug con:
    - timestamp preciso (utile per concorrenza)
    - nome del thread (utile per race condition)
    """
    # Formatta l'ora corrente con millisecondi (es. 14:05:32.123)
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    # Recupera il nome del thread corrente (es. Thread-1, MainThread)
    thread_name = threading.current_thread().name
    # Stampa il messaggio con timestamp e nome thread come prefisso
    print(f"[{timestamp}] [{thread_name}] {message}")


class IdempotentVersionedStore:
    """
    Store KV con supporto a:

    1. Versioning
       Ogni chiave ha una versione incrementale.

    2. Idempotenza
       Ogni richiesta mutativa ha un request_id univoco:
       client_id:sequence_number

       Questo permette di:
       - ignorare duplicati (retry)
       - restituire la stessa risposta

    3. Concorrenza
       Tutto è protetto da un lock globale per evitare race condition.

    4. Garbage collection
       La request_table non cresce indefinitamente.
    """

    def __init__(self, max_window: int = MAX_WINDOW) -> None:
        # Lock rientrante per proteggere tutti gli accessi alle strutture dati
        self._lock = threading.Lock()

        # Stato del KV store:
        # key -> (valore, versione)
        # versione parte da 0 al primo SET e si incrementa a ogni modifica
        self._data: dict[str, tuple[str, int]] = {}

        # Tabella dedup:
        # client_id -> {seq_number -> response già calcolata}
        # serve per rispondere identico ai retry
        self._request_table: dict[str, dict[int, str]] = {}

        # Ultimo request_id confermato per client.
        # Tutto <= ack può essere eliminato definitivamente.
        self._client_ack: dict[str, int] = {}

        # Dimensione massima della sliding window di deduplicazione per client
        self._max_window = max_window

        # Dispatch dei comandi testuali.
        # Ogni comando viene associato a una funzione handler.
        # Il dizionario viene costruito qui una sola volta per evitare
        # di ricostruirlo a ogni chiamata di execute().
        self._handlers: dict[str, CommandHandler] = {
            "PING":       self._handle_ping,       # test connessione
            "GET":        self._handle_get,         # lettura senza versione
            "GETV":       self._handle_getv,        # lettura con versione
            "SET_REQ":    self._handle_set_req,     # scrittura idempotente
            "CAS_REQ":    self._handle_cas_req,     # CAS idempotente
            "DELETE_REQ": self._handle_delete_req,  # cancellazione idempotente
            "ACK":        self._handle_ack,         # conferma ricevuta lato client
            "KEYS":       self._handle_keys,        # lista chiavi attive
            "QUIT":       self._handle_quit,        # chiusura connessione
        }

    def execute(self, line: str) -> tuple[str, bool]:
        """
        Punto di ingresso per ogni comando.

        Parsing:
        - separa comando e argomenti
        - dispatch verso handler

        Ritorna:
        - risposta testuale
        - flag se chiudere connessione
        """
        # Rimuove spazi e newline attorno alla riga ricevuta
        line = line.strip()
        # Se la riga è vuota dopo strip, restituisce errore senza crashare
        if not line:
            return "ERR empty command", False

        # Divide al primo spazio: "SET_REQ client:0 k v" -> ("SET_REQ", "client:0 k v")
        command, *rest = line.split(" ", 1)
        # Normalizza in maiuscolo per confronto case-insensitive
        command = command.upper()
        # rest è una lista vuota se non ci sono argomenti; prendiamo il primo elemento
        args = rest[0] if rest else ""

        # Cerca il comando nella mappa degli handler
        handler = self._handlers.get(command)
        # Comando sconosciuto: risponde con errore standard di protocollo
        if handler is None:
            return "ERR unknown command", False

        # Delega l'esecuzione all'handler specifico
        return handler(args)

    def _parse_request_id(self, token: str) -> tuple[str, int] | None:
        """
        Converte request_id nel formato:
            client_id:sequence_number

        Questo è fondamentale perché:
        - identifica univocamente ogni operazione mutativa
        - permette retry safe
        """
        # Il separatore ":" deve essere presente; senza è formato invalido
        if ":" not in token:
            return None

        # rsplit con maxsplit=1: separa l'ultimo ":" per gestire client_id
        # che potrebbero contenere ":" (es. "host:port:clientA" non accade qui
        # ma rsplit è più robusto di split per futuri formati)
        client_id, seq_text = token.rsplit(":", 1)

        # client_id non può essere la stringa vuota (es. ":5" è invalido)
        if not client_id:
            return None

        try:
            # Converte la parte numerica in intero
            seq = int(seq_text)
        except ValueError:
            # seq_text non era un intero (es. "clientA:abc")
            return None

        # I numeri di sequenza negativi non sono ammessi dal protocollo
        if seq < 0:
            return None

        # Restituisce la coppia (client_id, numero_di_sequenza)
        return client_id, seq

    def _check_dedup(self, client_id: str, seq: int) -> str | None:
        """
        Fase di deduplicazione.

        Tre casi:

        1. Richiesta già confermata (<= ack)
           -> considerata scaduta

        2. Richiesta già eseguita (presente in request_table)
           -> replay della risposta originale

        3. Richiesta nuova
           -> deve essere eseguita

        Questo è il cuore dell'idempotenza.
        """
        # Recupera l'ultimo seq confermato; -1 se il client non ha mai mandato ACK
        ack = self._client_ack.get(client_id, -1)

        # Caso 1: seq <= ack significa che questa richiesta è già stata
        # confermata dal client come "non verrà più ritentata"; è scaduta
        if seq <= ack:
            return "ERR request_id_expired"

        # Recupera la sotto-tabella di questo client (dizionario vuoto se assente)
        table = self._request_table.get(client_id, {})

        # Caso 2: il seq è già presente -> è un retry; restituisce la risposta
        # originale senza rieseguire l'operazione (idempotenza)
        if seq in table:
            return table[seq]

        # Caso 3: richiesta nuova, nessuna risposta in cache
        return None

    def _save_and_gc(self, client_id: str, seq: int, response: str) -> None:
        """
        Salva la risposta per eventuali retry futuri.

        Inoltre gestisce garbage collection:

        - ogni client ha una finestra MAX_WINDOW
        - quando la supera, si eliminano le richieste più vecchie
        - si aggiorna ACK per riflettere ciò che è stato eliminato

        Questo evita crescita infinita della request_table.
        """
        # Crea la sotto-tabella per il client se non esiste ancora
        if client_id not in self._request_table:
            self._request_table[client_id] = {}

        # Memorizza la risposta associata a questo numero di sequenza
        self._request_table[client_id][seq] = response

        # Alias locale per leggibilità
        table = self._request_table[client_id]

        # Verifica se la finestra è stata superata
        if len(table) > self._max_window:
            # Ordina i seq numericamente (crescente) per trovare i più vecchi
            sorted_seqs = sorted(table.keys())

            # Numero di entry da rimuovere per rientrare nella finestra
            to_remove = len(table) - self._max_window

            # rimuove le richieste più vecchie (quelle con seq più bassi)
            for s in sorted_seqs[:to_remove]:
                del table[s]

            # aggiorna ACK: tutto ciò che è stato rimosso
            # è considerato definitivamente confermato
            # sorted_seqs[to_remove - 1] è l'ultimo seq rimosso
            new_ack = sorted_seqs[to_remove - 1]
            # Legge l'ack corrente (default -1 se mai settato)
            old_ack = self._client_ack.get(client_id, -1)

            # Aggiorna solo se il nuovo valore è più alto (ack è monotono)
            if new_ack > old_ack:
                self._client_ack[client_id] = new_ack

    def _dispatch_idempotent(
        self,
        request_id: str,
        fn: Callable[[], str]
    ) -> tuple[str, bool]:
        """
        Pipeline principale dell'idempotenza.

        Ordine delle operazioni:

        1. parsing request_id
        2. lock globale (protezione concorrenza)
        3. check deduplicazione
        4. esecuzione operazione se nuova
        5. salvataggio risposta per retry futuri

        Questo garantisce:
        - nessuna doppia esecuzione
        - risposta consistente tra retry
        - atomicità logica
        """
        # Fase 1: valida e decompone il request_id in (client_id, seq)
        parsed = self._parse_request_id(request_id)
        # request_id malformato: risponde subito senza acquisire il lock
        if parsed is None:
            return "ERR invalid_request_id", False

        # Estrae le componenti del request_id
        client_id, seq = parsed

        # Fase 2: acquisisce il lock globale per garantire atomicità
        # (nessun altro thread può modificare _data o _request_table)
        with self._lock:

            # Fase 3: controlla se la richiesta è un duplicato o è scaduta
            # caso retry o expired
            cached = self._check_dedup(client_id, seq)
            if cached is not None:
                # Log del cache hit per debug e tracciabilità
                log(f"dedup hit {request_id} -> {cached}")
                # Restituisce la risposta già calcolata senza rieseguire fn()
                return cached, False

            # Fase 4: richiesta nuova -> esegue la modifica reale allo store
            # fn() è una closure che cattura key/value dalla chiamata originale
            response = fn()

            # Fase 5: salva la risposta per futuri retry e applica GC
            self._save_and_gc(client_id, seq, response)

        # Il lock è rilasciato; restituisce la risposta appena calcolata
        return response, False

    def _handle_ping(self, args: str) -> tuple[str, bool]:
        """Test base connessione."""
        # PING non accetta argomenti: se args non è vuoto, è un errore di sintassi
        if args.strip():
            return "ERR usage: PING", False
        # Risponde con PONG: conferma che il server è vivo
        return "OK PONG", False

    def _handle_get(self, args: str) -> tuple[str, bool]:
        """Lettura semplice senza versione."""
        # Estrae la chiave rimuovendo spazi superflui
        key = args.strip()
        # La chiave è obbligatoria
        if not key:
            return "ERR usage: GET <key>", False

        # Legge il valore sotto lock per evitare letture parziali
        with self._lock:
            value = self._data.get(key)

        # Chiave inesistente: risponde NOT_FOUND (non è un errore di protocollo)
        if value is None:
            return "NOT_FOUND", False

        # value è la tupla (valore, versione); restituisce solo il valore
        return f"OK {value[0]}", False

    def _handle_getv(self, args: str) -> tuple[str, bool]:
        """Lettura con versione (utile per test consistenza)."""
        # Estrae la chiave rimuovendo spazi superflui
        key = args.strip()
        # La chiave è obbligatoria
        if not key:
            return "ERR usage: GETV <key>", False

        # Legge sotto lock per consistenza snapshot
        with self._lock:
            value = self._data.get(key)

        # Chiave inesistente
        if value is None:
            return "NOT_FOUND", False

        # Restituisce valore e versione: utile al client per una CAS successiva
        return f"OK {value[0]} version={value[1]}", False

    def _handle_keys(self, args: str) -> tuple[str, bool]:
        """Lista chiavi attive."""
        # KEYS non accetta argomenti
        if args.strip():
            return "ERR usage: KEYS", False

        # Acquisisce lock e legge le chiavi in modo atomico
        with self._lock:
            # Ordina le chiavi per output deterministico
            keys = " ".join(sorted(self._data.keys()))

        # rstrip() rimuove lo spazio finale se lo store è vuoto
        return f"OK {keys}".rstrip(), False

    def _handle_quit(self, args: str) -> tuple[str, bool]:
        """Chiude connessione client."""
        # Secondo elemento True: segnala a handle_client di chiudere il socket
        return "OK BYE", True

    def _handle_set_req(self, args: str) -> tuple[str, bool]:
        """Operazione SET idempotente."""
        # Divide in al massimo 3 parti: request_id, key, value
        # (value può contenere spazi, split limita a 2 separazioni)
        parts = args.split(" ", 2)
        if len(parts) != 3:
            return "ERR usage: SET_REQ <request_id> <key> <value>", False
        # Destruttura le tre parti
        request_id, key, value = parts

        def do_set() -> str:
            # Legge la versione corrente della chiave (default -1 se assente)
            _, v = self._data.get(key, ("", -1))
            # Incrementa la versione (0 al primo SET, poi 1, 2, ...)
            v += 1
            # Scrive il nuovo valore con la versione aggiornata
            self._data[key] = (value, v)
            # Restituisce la versione assegnata al client
            return f"OK version={v}"

        # Delega a _dispatch_idempotent per dedup + lock + GC
        return self._dispatch_idempotent(request_id, do_set)

    def _handle_cas_req(self, args: str) -> tuple[str, bool]:
        """CAS idempotente."""
        # Divide in al massimo 4 parti: request_id, key, expected_version, value
        parts = args.split(" ", 3)
        if len(parts) != 4:
            return "ERR usage: CAS_REQ <request_id> <key> <expected_version> <value>", False
        # Destruttura le quattro parti
        request_id, key, exp, value = parts
        try:
            # Converte la versione attesa in intero
            expected = int(exp)
        except ValueError:
            # expected_version non numerico: errore di formato
            return "ERR expected_version must be an integer", False

        def do_cas() -> str:
            # Legge lo stato corrente della chiave
            cur = self._data.get(key)
            # Se la chiave non esiste, la versione corrente è -1 (convenzione)
            cur_v = -1 if cur is None else cur[1]

            # Verifica che la versione attesa corrisponda a quella reale
            if cur_v != expected:
                # Fallimento CAS: restituisce la versione effettiva per debug
                return f"ERR version_mismatch current={cur_v}"

            # Versione corrispondente: procede con l'aggiornamento
            new_v = cur_v + 1
            # Scrive il nuovo valore con versione incrementata
            self._data[key] = (value, new_v)
            # Restituisce la nuova versione assegnata
            return f"OK version={new_v}"

        # Delega a _dispatch_idempotent per dedup + lock + GC
        return self._dispatch_idempotent(request_id, do_cas)

    def _handle_delete_req(self, args: str) -> tuple[str, bool]:
        """Delete idempotente."""
        # Divide in al massimo 2 parti: request_id e key
        parts = args.split(" ", 1)
        if len(parts) != 2:
            return "ERR usage: DELETE_REQ <request_id> <key>", False
        # Destruttura le due parti
        request_id, key = parts

        def do_delete() -> str:
            # Se la chiave non esiste, risponde NOT_FOUND
            # (idempotente: la risposta sarà la stessa in caso di retry)
            if key not in self._data:
                return "NOT_FOUND"
            # Rimuove la chiave dal dizionario
            del self._data[key]
            # Conferma la cancellazione
            return "OK"

        # Delega a _dispatch_idempotent per dedup + lock + GC
        return self._dispatch_idempotent(request_id, do_delete)

    def _handle_ack(self, args: str) -> tuple[str, bool]:
        """
        ACK manuale dal client.

        Serve a:
        - dire al server che certe richieste non saranno più ritentate
        - permettere garbage collection aggressiva

        A differenza della sliding window (che rimuove le entry solo
        quando la tabella supera MAX_WINDOW in seguito a una nuova
        richiesta), l'ACK esplicito libera la memoria immediatamente:
        appena il client conferma seq, tutte le entry con
        sequence_number <= seq vengono cancellate da _request_table,
        come dichiarato nel contratto ("Il server rimuove le entry con
        sequence_number <= seq").
        """
        # Divide il payload: deve contenere esattamente client_id e seq
        parts = args.split()
        if len(parts) != 2:
            return "ERR usage: ACK <client_id> <sequence_number>", False
        # Destruttura client_id e testo del sequence number
        client_id, seq_text = parts
        try:
            # Converte il sequence number in intero
            seq = int(seq_text)
        except ValueError:
            # sequence_number non numerico
            return "ERR sequence_number must be an integer", False

        with self._lock:
            # Aggiorna _client_ack in modo monotono: non può tornare indietro.
            # max() garantisce che ACK duplicati o fuori ordine siano sicuri
            self._client_ack[client_id] = max(
                seq,
                self._client_ack.get(client_id, -1)
            )

            # Pulizia esplicita: rimozione fisica delle entry confermate.
            # Si costruisce prima la lista delle chiavi da rimuovere per
            # non modificare il dizionario mentre lo si itera.
            table = self._request_table.get(client_id)
            if table:
                # Raccoglie tutti i seq <= ack che possono essere eliminati
                expired_seqs = [s for s in table if s <= seq]
                # Rimuove le entry scadute dalla request_table
                for s in expired_seqs:
                    del table[s]

        # Risponde OK: il client sa che il GC è avvenuto
        return "OK", False


def handle_client(conn, addr, store):
    """Thread per singolo client TCP."""
    # Logga l'apertura della connessione con l'indirizzo del client
    log(f"connection {addr}")

    # Il blocco 'with conn' chiude il socket automaticamente all'uscita
    with conn:
        # Crea un wrapper file buffered bidirezionale sul socket
        # "rwb" = lettura + scrittura + binario
        f = conn.makefile("rwb")

        while True:
            # Legge una riga dal client (bloccante; ritorna b"" se il client chiude)
            raw = f.readline()
            # b"" indica EOF: il client ha chiuso la connessione
            if not raw:
                break

            # Decodifica da bytes a stringa (errors="replace" evita crash su byte non-UTF8)
            line = raw.decode("utf-8", errors="replace")
            # Logga la richiesta ricevuta (strip per rimuovere il newline dal log)
            log(f"request {line.strip()}")

            # Passa la riga allo store che la esegue e restituisce (risposta, chiudi)
            resp, close = store.execute(line)

            # Invia la risposta terminata da newline (protocollo line-based)
            f.write((resp + "\n").encode())
            # flush() è necessario: il buffer non si svuota automaticamente
            f.flush()

            # Logga la risposta inviata
            log(f"response {resp}")

            # Se lo store ha segnalato chiusura (es. QUIT), esce dal loop
            if close:
                break


def serve():
    """Avvia server TCP multi-thread."""
    # Crea l'unica istanza condivisa dello store (tutti i thread la condividono)
    store = IdempotentVersionedStore()

    # socket.AF_INET = IPv4; socket.SOCK_STREAM = TCP
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        # SO_REUSEADDR: permette di riavviare il server subito dopo uno stop
        # senza attendere il timeout del kernel sul socket in TIME_WAIT
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Associa il socket all'indirizzo e alla porta configurati
        s.bind((HOST, PORT))
        # Mette il socket in ascolto; il kernel accetta connessioni in coda
        s.listen()

        log(f"server listening on {HOST}:{PORT}")

        while True:
            # Blocca fino all'arrivo di una nuova connessione TCP
            conn, addr = s.accept()
            # Crea un thread daemon per gestire questo client
            # daemon=True: il thread viene terminato automaticamente all'uscita del main
            t = threading.Thread(
                target=handle_client,
                args=(conn, addr, store),
                daemon=True
            )
            # Avvia il thread; il main torna immediatamente ad accept()
            t.start()


if __name__ == "__main__":
    # Punto di ingresso: avvia il server solo se eseguito direttamente
    serve()