import argparse
import socket
import sys
import threading
import time
import uuid

from client import IdempotentClient

"""
Test end-to-end del client reale.

A differenza degli altri test che usano socket raw per verificare il
protocollo, questo test esercita il codice di produzione del client
(client.py) attraverso un "chaos proxy" TCP che simula guasti di rete.

Il proxy si interpone tra IdempotentClient e il server: quando riceve
l'istruzione di "droppare", inghiotte la risposta del server e chiude
la connessione verso il client, forzandolo a seguire il suo percorso
di retry (_send_with_retry -> _reconnect -> stessa richiesta con lo
stesso request_id).

Scenari testati:
1. Operazione normale tramite API client (senza fault)
2. SET_REQ con drop: il retry automatico produce la stessa risposta
3. CAS_REQ con drop: versione non incrementata dal retry
4. DELETE_REQ con drop: retry della cancellazione idempotente
5. CAS_REQ fallita con drop: anche il retry restituisce l'errore originale
6. ACK via client.ack()
7. Operazione post-ACK (continuita' dei sequence number)

Il server deve essere in esecuzione su 127.0.0.1:6460 prima di lanciare
questo script.
"""

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Test end-to-end del client con chaos proxy."
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6460,
                        help="Porta del server")
    parser.add_argument("--proxy-port", type=int, default=6461,
                        help="Porta locale del chaos proxy")
    return parser.parse_args()


# =====================================================================
# Chaos Proxy
# =====================================================================

class ChaosProxy:
    """
    Proxy TCP trasparente con fault injection.

    Si interpone tra client e server. Per default inoltra tutto.
    Quando viene chiamato schedule_drop(), la prossima risposta del
    server viene inghiottita e la connessione client chiusa, simulando
    una perdita di rete.

    Flusso di un drop:
      1. Il client invia la richiesta -> il proxy la inoltra al server.
      2. Il server elabora, salva nella request_table, risponde.
      3. Il proxy legge la risposta ma NON la inoltra al client.
      4. Il proxy chiude la connessione lato client.
      5. Il client riceve ConnectionError, esegue _reconnect e ritenta.
      6. Il retry arriva su una nuova connessione (nuovo handler del proxy,
         nuova connessione al server). Il server riconosce il request_id
         duplicato e restituisce la risposta salvata.
    """

    def __init__(self, listen_port: int, server_host: str, server_port: int) -> None:
        self.listen_port = listen_port
        self.server_host = server_host
        self.server_port = server_port
        self._drop_next = threading.Event()
        self.drop_count: int = 0
        self.forward_count: int = 0
        self._server_socket: socket.socket | None = None
        self._running = False
        self._lock = threading.Lock()

    def start(self) -> None:
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind(("127.0.0.1", self.listen_port))
        self._server_socket.listen(5)
        self._server_socket.settimeout(1.0)
        self._running = True
        self._thread = threading.Thread(
            target=self._accept_loop, daemon=True, name="proxy-accept"
        )
        self._thread.start()

    def _accept_loop(self) -> None:
        while self._running:
            try:
                client_conn, _ = self._server_socket.accept()
                handler = threading.Thread(
                    target=self._handle,
                    args=(client_conn,),
                    daemon=True,
                    name="proxy-handler",
                )
                handler.start()
            except socket.timeout:
                continue
            except OSError:
                break

    def _handle(self, client_conn: socket.socket) -> None:
        try:
            server_conn = socket.create_connection(
                (self.server_host, self.server_port)
            )
        except Exception:
            client_conn.close()
            return

        client_file = client_conn.makefile("rwb")
        server_file = server_conn.makefile("rwb")

        try:
            while True:
                # Legge la richiesta dal client
                request = client_file.readline()
                if not request:
                    break

                # Inoltra al server
                server_file.write(request)
                server_file.flush()

                # Legge la risposta dal server
                response = server_file.readline()
                if not response:
                    break

                # === Fault injection ===
                if self._drop_next.is_set():
                    self._drop_next.clear()
                    with self._lock:
                        self.drop_count += 1
                    # Chiude la connessione senza inoltrare la risposta.
                    # Il server ha gia' processato e salvato nella
                    # request_table; il client ricevera' ConnectionError
                    # e fara' retry con lo stesso request_id.
                    client_conn.close()
                    server_conn.close()
                    return

                # Inoltro normale
                with self._lock:
                    self.forward_count += 1
                client_file.write(response)
                client_file.flush()
        except Exception:
            pass
        finally:
            for f in (client_file, server_file):
                try:
                    f.close()
                except Exception:
                    pass
            for s in (client_conn, server_conn):
                try:
                    s.close()
                except Exception:
                    pass

    def schedule_drop(self) -> None:
        """Schedula il drop della prossima risposta dal server."""
        self._drop_next.set()

    def stop(self) -> None:
        """Ferma il proxy."""
        self._running = False
        if self._server_socket:
            try:
                self._server_socket.close()
            except Exception:
                pass


# =====================================================================
# Asserzioni
# =====================================================================

def assert_eq(test_name: str, actual: str, expected: str) -> bool:
    if actual == expected:
        print(f"  PASS  {test_name}")
        return True
    else:
        print(f"  FAIL  {test_name}")
        print(f"        expected: {expected}")
        print(f"        actual:   {actual}")
        return False


def assert_starts(test_name: str, actual: str, prefix: str) -> bool:
    if actual.startswith(prefix):
        print(f"  PASS  {test_name}")
        return True
    else:
        print(f"  FAIL  {test_name}")
        print(f"        expected prefix: {prefix}")
        print(f"        actual:          {actual}")
        return False


# =====================================================================
# Test principale
# =====================================================================

def main() -> None:
    args = parse_args()
    passed = 0
    failed = 0

    def check(result: bool) -> None:
        nonlocal passed, failed
        if result:
            passed += 1
        else:
            failed += 1

    run_id = uuid.uuid4().hex[:8]
    client_id = f"cli_{run_id}"
    key_a = f"ka_{run_id}"
    key_b = f"kb_{run_id}"
    print(f"Run ID: {run_id}  (client_id={client_id})")

    # === Avvio chaos proxy ===
    proxy = ChaosProxy(args.proxy_port, args.host, args.port)
    proxy.start()
    time.sleep(0.3)

    # === Client connesso al PROXY (non al server direttamente) ===
    client = IdempotentClient(
        client_id=client_id,
        host="127.0.0.1",
        port=args.proxy_port,
        timeout=0.5,       # timeout basso per test veloci
        max_retries=3,
    )
    client.connect()

    try:
        # ====================================================================================
        # SCENARIO 1: Operazione normale (Nessun fault)
        # Verifica semplicemente che l'architettura col Proxy in mezzo funzioni per il traffico pulito.
        # ====================================================================================
        print("\n-- Scenario 1: SET_REQ via IdempotentClient (nessun fault) --")

        r1 = client.set_req(key_a, "hello")
        # Usiamo il metodo ad alto livello 'set_req' (il client genera il request_id da solo).
        check(assert_eq("set_req normale", r1, "OK version=0"))

        r2 = client.getv(key_a)
        check(assert_eq("getv dopo set", r2, "OK hello version=0"))

        # ====================================================================================
        # SCENARIO 2: SET_REQ con DROP
        # Carichiamo la trappola. Il proxy farà cadere la connessione proprio mentre il server
        # sta confermando l'aggiornamento a version=1. Il client dovrà ritentare da solo.
        # ====================================================================================
        print("\n-- Scenario 2: SET_REQ con drop -> retry automatico --")

        proxy.schedule_drop()
        # Alziamo la bandierina del sabotaggio!
        r3 = client.set_req(key_a, "world")
        # Il test si blocca qui per un secondo: il client invia, la linea cade, il client cattura
        # l'errore, si riconnette, reinvia con lo stesso ID e alla fine ottiene l'OK.
        check(assert_eq("set_req dopo drop+retry", r3, "OK version=1"))

        r4 = client.getv(key_a)
        # Il DB è avanzato a version=1 e NON a version=2. Il retry ha funzionato in modo idempotente!
        check(assert_eq("versione non doppia dopo retry", r4, "OK world version=1"))

        # ====================================================================================
        # SCENARIO 3: CAS_REQ con drop
        # Stessa cosa, ma testiamo il comportamento automatico sulla funzione Compare-And-Set.
        # ====================================================================================
        print("\n-- Scenario 3: CAS_REQ con drop -> retry automatico --")

        proxy.schedule_drop()
        r5 = client.cas_req(key_a, 1, "updated")
        check(assert_eq("cas_req dopo drop+retry", r5, "OK version=2"))

        r6 = client.getv(key_a)
        check(assert_eq("versione dopo CAS retry", r6, "OK updated version=2"))

        # ====================================================================================
        # SCENARIO 4: DELETE_REQ con drop
        # Testiamo se il client sa fare il retry automatico anche per le cancellazioni.
        # ====================================================================================
        print("\n-- Scenario 4: DELETE_REQ con drop -> retry automatico --")

        proxy.schedule_drop()
        r7 = client.delete_req(key_a)
        # Il proxy fa cadere la linea, il client riprova e riceve l'OK preso dalla cache.
        check(assert_eq("delete_req dopo drop+retry", r7, "OK"))

        r8 = client.get(key_a)
        check(assert_eq("chiave cancellata", r8, "NOT_FOUND"))

        # ====================================================================================
        # SCENARIO 5: CAS fallita + drop
        # Cosa succede se il server boccia la nostra richiesta, e per giunta la risposta
        # bocciata si perde nella rete? Il client deve recuperare l'errore intatto.
        # ====================================================================================
        print("\n-- Scenario 5: CAS_REQ fallita con drop -> retry errore --")

        r9 = client.set_req(key_b, "base")
        check(assert_eq("setup key_b", r9, "OK version=0"))

        proxy.schedule_drop()
        # Inneschiamo la caduta della rete.
        r10 = client.cas_req(key_b, 99, "nope")
        # Iniziamo una CAS illegale (version 99). Il server dice ERR, il proxy taglia il cavo.
        # Il client riprova, il server ripesca l'ERR in memoria e lo restituisce.
        check(assert_eq(
            "CAS fallita dopo drop+retry", r10,
            "ERR version_mismatch current=0"
        ))

        # ====================================================================================
        # SCENARIO 6 & 7: ACK via API e continuità
        # Dimostriamo che il client reale sa inviare gli ACK per fare pulizia, e che subito
        # dopo il suo contatore interno della sequenza (`_seq`) non si è rotto ma va avanti.
        # ====================================================================================
        print("\n-- Scenario 6: ACK via client.ack() --")

        r11 = client.ack()
        # Testiamo il metodo 'ack()' integrato nella classe.
        check(assert_eq("ack()", r11, "OK"))

        print("\n-- Scenario 7: Operazione dopo ACK (continuita' seq) --")

        r12 = client.set_req(key_b, "post_ack")
        # Inviamo una nuova operazione per assicurarci che il contatore delle sequenze non sia azzerato.
        check(assert_starts("set_req post-ACK", r12, "OK"))

    finally:
        # Quando il test finisce (che vada bene o in errore), chiudiamo in modo pulito le risorse.
        client.close()
        proxy.stop()

    # === Riepilogo ===
    print(f"\n{'='*50}")
    print(f"Proxy: {proxy.forward_count} inoltrate, {proxy.drop_count} droppate")
    print(f"Risultati: {passed} PASS, {failed} FAIL")
    print(f"{'='*50}")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
