import argparse
import socket
import sys
import uuid

"""
Verifica che il server gestisca correttamente la pulizia della
request_table e che i retry su request_id scaduti restituiscano
ERR request_id_expired.

Il server deve essere in esecuzione su 127.0.0.1:6460 prima di lanciare
questo script.

NOTA: il server usa MAX_WINDOW=100 per default. Per testare la GC
automatica, inviamo > 100 richieste per un singolo client.
"""

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Test garbage collection.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6460)
    parser.add_argument("--window", type=int, default=100,
                        help="MAX_WINDOW del server (per calcolare le attese)")
    return parser.parse_args()


class TestConnection:
    def __init__(self, host: str, port: int) -> None:
        self._socket = socket.create_connection((host, port))
        self._socket.settimeout(10.0)
        self._file = self._socket.makefile("rwb")

    def send(self, command: str) -> str:
        self._file.write((command + "\n").encode("utf-8"))
        self._file.flush()
        response = self._file.readline()
        if not response:
            raise ConnectionError("Connection closed")
        return response.decode("utf-8", errors="replace").strip()

    def close(self) -> None:
        try:
            self.send("QUIT")
        except Exception:
            pass
        self._file.close()
        self._socket.close()


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

    conn = TestConnection(args.host, args.port)
    run_id = uuid.uuid4().hex[:8]
    client_id = f"gc_{run_id}"
    window = args.window
    print(f"Run ID: {run_id}  (client_id={client_id})")


    # Scenario 1: Riempimento della finestra.
    # L'obiettivo è superare la MAX_WINDOW del server per forzare l'attivazione della GC automatica.
    # Inviando 120 richieste su una finestra di 100, il server sarà costretto a espellere le prime 20.
    print(f"\n-- Scenario 1: Invio di {window + 20} richieste (finestra = {window}) --")

    total_ops = window + 20
    for seq in range(total_ops):
        response = conn.send(f"SET_REQ {client_id}:{seq} gckey_{run_id} val_{seq}")
        if not response.startswith("OK"):
            print(f"  WARN  Unexpected response at seq {seq}: {response}")

    print(f"  INFO  Inviate {total_ops} richieste")

    # SCENARIO 2: Replay di una richiesta recente ancora in finestra.
    # Verifica che le richieste recenti, non ancora espulse, mantengano la loro idempotenza.
    print(f"\n-- Scenario 2: Replay di richiesta recente (seq={total_ops - 1}) --")

    recent_seq = total_ops - 1
    r1 = conn.send(f"SET_REQ {client_id}:{recent_seq} gckey_{run_id} val_{recent_seq}")
    check(assert_starts("Replay recente OK", r1, "OK"))

    # SCENARIO 3: Replay di una richiesta scaduta (BOUNDARY TESTING)
    # Si testa la primissima richiesta inviata, sicuramente scartata.
    # poi si testano  i due bordi della finestra, ci aspettiamo che la 20 sia valida e la 19 espulsa.
    print(f"\n-- Scenario 3: Replay di richiesta scaduta (seq=0) --")

    # Si invia un retry per la primissima sequenza assoluta, ovvero la 0.
    r2 = conn.send(f"SET_REQ {client_id}:0 gckey_{run_id} val_0")
    # Il server restituisce l'eccezione corretta: non ce l'ha più in memoria.
    check(assert_eq("Replay scaduto", r2, "ERR request_id_expired"))

    # Prima entry sopravvissuta dovrebbe essere seq = total_ops - window
    first_alive = total_ops - window

    # Tenta il replay della sequenza 20
    r3 = conn.send(f"SET_REQ {client_id}:{first_alive} gckey_{run_id} val_{first_alive}")
    # La 20 è la più vecchia entry ancora sopravvissuta nel dizionario.
    check(assert_starts(f"Replay prima entry viva (seq={first_alive})", r3, "OK"))

    # L'entry appena prima della prima viva dovrebbe essere scaduta.
    # Calcola la sequenza immediatamente precedente: 20 - 1 = 19.
    just_expired = first_alive - 1
    # Tenta il replay della sequenza 19.
    r4 = conn.send(f"SET_REQ {client_id}:{just_expired} gckey_{run_id} val_{just_expired}")
    # Fallisce con expired! È la conferma finale del Boundary Testing perfetto.
    check(assert_eq(f"Replay entry appena scaduta (seq={just_expired})", r4, "ERR request_id_expired"))


    # Scenario 4: ACK esplicito
    # Dimostra l'ACK cumulativo funziona istantaneamente senza dover aspettare il riempimento della finestra di 100.
    print(f"\n-- Scenario 4: ACK esplicito --")

    # Creiamo un nuovo client per ACK
    ack_client = f"ack_{run_id}"
    # Si crea un nuovo client con zero storia sul server per evitare interferenze.
    ack_key = f"ackkey_{run_id}"
    # Invia 10 richieste sequenziali (da 0 a 9).
    for seq in range(10):
        conn.send(f"SET_REQ {ack_client}:{seq} {ack_key} val_{seq}")

    # Tutti i seq 0-9 dovrebbero essere replayabili.
    # Tenta un retry a metà strada prima di fare ACK.
    r5 = conn.send(f"SET_REQ {ack_client}:5 {ack_key} val_5")
    # Funziona regolarmente perché sono state inviate solo 10 richieste (finestra non piena).
    check(assert_starts("Pre-ACK replay seq=5 OK", r5, "OK"))

    # Inviamo ACK fino a seq 7
    r6 = conn.send(f"ACK {ack_client} 7")
    # Il server conferma l'eliminazione.
    check(assert_eq("ACK risposta", r6, "OK"))

    # Ora seq 0-7 dovrebbero essere scaduti. Tenta nuovamente un retry sulla sequenza 5.
    r7 = conn.send(f"SET_REQ {ack_client}:5 {ack_key} val_5")
    # Questa volta scatta l'errore perché la pulizia manuale l'ha spazzata via.
    check(assert_eq("Post-ACK replay seq=5 scaduto", r7, "ERR request_id_expired"))

    # Ma seq 8 e 9 dovrebbero essere ancora replayabili. # Tenta un retry per la 8.
    r8 = conn.send(f"SET_REQ {ack_client}:8 {ack_key} val_8")
    # Funziona regolarmente.
    check(assert_starts("Post-ACK replay seq=8 OK", r8, "OK"))

    r9 = conn.send(f"SET_REQ {ack_client}:9 {ack_key} val_9")
    check(assert_starts("Post-ACK replay seq=9 OK", r9, "OK"))

    # SCENARIO 5: ACK con formato errato
    # Esattamente come in test_idempotenza, si verifica la robustezza del parser
    # contro stringhe ACK inviate in modo malevolo o scorretto.
    print(f"\n-- Scenario 5: ACK con formato errato --")

    # Invia comando senza parametri.
    r10 = conn.send("ACK")
    check(assert_starts("ACK senza argomenti", r10, "ERR"))

    # Invia comando omettendo il numero di sequenza.
    r11 = conn.send("ACK solo_client_id")
    check(assert_starts("ACK senza seq", r11, "ERR"))

    # Invia comando con una sequenza non numerica.
    r12 = conn.send("ACK client abc")
    check(assert_eq("ACK seq non numerica", r12, "ERR sequence_number must be an integer"))


    # Riepilogo
    conn.close()
    print(f"\n{'='*50}")
    print(f"Risultati: {passed} PASS, {failed} FAIL")
    print(f"{'='*50}")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
