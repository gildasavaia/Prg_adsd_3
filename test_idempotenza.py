import argparse
import socket
import sys
import uuid

""" Test di idempotenza -> Verifica che i retry delle operazioni di riscrittura non producano
effetti doppi.

Il server deve essere in esecuzione su 127.0.0.1:6460 prima di lanciare questo script.

Scenari testati:
1. SET_REQ: primo invio + replay -> stessa risposta, versione non incrementata
2. CAS_REQ: primo invio + replay -> stessa risposta, versione non incrementata
3. DELETE_REQ: primo invio + replay -> stessa risposta, chiave resta cancellata
4. Richieste diverse con stessa chiave -> effetti distinti
5. CAS_REQ fallita + replay -> anche il replay restituisce l'errore originale
"""

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Test idempotenza per KV store.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6460)
    return parser.parse_args()

class TestConnection:
    """Connessione raw TCP per test a basso livello."""

    def __init__(self, host: str, port: int) -> None:
        self._socket = socket.create_connection((host, port))
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
    cid = uuid.uuid4().hex[:8]  # client_id unico per ogni run.
    # Chiavi uniche per ogni run, cosi' il test e' ripetibile.
    ka, kb, kc, kd, ke = (
        f"alpha_{cid}", f"beta_{cid}", f"gamma_{cid}", f"delta_{cid}", f"epsilon_{cid}"
    )
    print(f"Client ID per questo run: {cid}")


    # Scenario 1: SET_REQ idempotente.
    print("\n-- Scenario 1: SET_REQ idempotente --")

    # Prima esecuzione.
    r1 = conn.send(f"SET_REQ {cid}:0 {ka} hello")
    check(assert_eq("SET_REQ prima esecuzione", r1, "OK version=0"))

    # Replay della stessa richiesta.
    r2 = conn.send(f"SET_REQ {cid}:0 {ka} hello")
    check(assert_eq("SET_REQ replay stessa risposta", r2, r1))

    # Verifica che la versione non sia stata incrementata (ancora 0, non 1).
    r3 = conn.send(f"GETV {ka}")
    check(assert_eq("GETV versione non incrementata", r3, "OK hello version=0"))


    # Scenario 2: CAS_REQ idempotente.
    print("\n-- Scenario 2: CAS_REQ idempotente --")

    # Prima esecuzione.
    r4 = conn.send(f"CAS_REQ {cid}:1 {ka} 0 world")
    check(assert_eq("CAS_REQ prima esecuzione", r4, "OK version=1"))

    # Replay della stessa richiesta.
    r5 = conn.send(f"CAS_REQ {cid}:1 {ka} 0 world")
    check(assert_eq("CAS_REQ replay stessa risposta", r5, r4))

    # Verifica che la versione non sia stata incrementata.
    r6 = conn.send(f"GETV {ka}")
    check(assert_eq("GETV dopo CAS_REQ", r6, "OK world version=1"))


    # Scenario 3: DELETE_REQ idempotente.
    print("\n-- Scenario 3: DELETE_REQ idempotente --")

    # Prima esecuzione.
    r7 = conn.send(f"DELETE_REQ {cid}:2 {ka}")
    check(assert_eq("DELETE_REQ prima esecuzione", r7, "OK"))

    # Replay della stessa richiesta.
    r8 = conn.send(f"DELETE_REQ {cid}:2 {ka}")
    check(assert_eq("DELETE_REQ replay stessa risposta", r8, "OK"))

    # Verifica che la chiave sia effettivamente cancellata.
    r9 = conn.send(f"GET {ka}")
    check(assert_eq("GET dopo DELETE_REQ", r9, "NOT_FOUND"))


    # Scenario 4: Richieste diverse sulla stessa chiave.
    print("\n-- Scenario 4: Richieste diverse, stessa chiave --")

    r10 = conn.send(f"SET_REQ {cid}:3 {kb} uno")
    check(assert_eq("SET_REQ beta primo", r10, "OK version=0"))

    r11 = conn.send(f"SET_REQ {cid}:4 {kb} due")
    check(assert_eq("SET_REQ beta secondo", r11, "OK version=1"))

    # Le due richieste hanno request_id diversi, per questo motivo non si confondono.
    r12 = conn.send(f"GETV {kb}")
    check(assert_eq("GETV beta valore finale", r12, "OK due version=1"))


    # Scenario 5: CAS_REQ fallita + replay.
    print("\n-- Scenario 5: CAS_REQ fallita + replay --")

    # CAS con versione sbagliata (beta e' a versione 1, non 99).
    r13 = conn.send(f"CAS_REQ {cid}:5 {kb} 99 tre")
    check(assert_eq("CAS_REQ fallita", r13, "ERR version_mismatch current=1"))

    # Replay della CAS fallita: deve restituire lo stesso errore.
    r14 = conn.send(f"CAS_REQ {cid}:5 {kb} 99 tre")
    check(assert_eq("CAS_REQ fallita replay", r14, r13))


    # Scenario 6: Formato request_id errato.
    print("\n-- Scenario 6: Formato request_id errato --")

    r15 = conn.send(f"SET_REQ badformat {kc} hello")
    check(assert_eq("SET_REQ senza ':'", r15, "ERR invalid_request_id"))

    r16 = conn.send(f"SET_REQ test:abc {kc} hello")
    check(assert_eq("SET_REQ seq non numerica", r16, "ERR invalid_request_id"))

    r17 = conn.send(f"SET_REQ :5 {kc} hello")
    check(assert_eq("SET_REQ client_id vuoto", r17, "ERR invalid_request_id"))


    # Scenario 7: retry con lo stesso request_id ma con argomenti diversi.
    """Il server deve ignorare i nuovi argomenti e restituire la risposta memorizzata dalla prima
    esecuzione, senza applicare il nuovo valore."""
    print("\n-- Scenario 7: retry con argomenti diversi --")

    r18 = conn.send(f"SET_REQ {cid}:6 {kd} originale")
    check(assert_eq("SET_REQ prima esecuzione (delta)", r18, "OK version=0"))

    # Stesso request_id, ma valore diverso: deve valere il replay, non il nuovo valore.
    r19 = conn.send(f"SET_REQ {cid}:6 {kd} valore_diverso")
    check(assert_eq("SET_REQ retry con argomenti diversi -> replay originale", r19, r18))

    r20 = conn.send(f"GETV {kd}")
    check(assert_eq("GETV valore non modificato dal retry", r20, "OK originale version=0"))


    # Scenario 8: DELETE_REQ su chiave mai esistita, ripetuto.
    """Anche una risposta NOT_FOUND deve essere memorizzata e ripetuta in modo idempotente,
    non solo le risposte di successo."""
    print("\n-- Scenario 8: DELETE_REQ NOT_FOUND idempotente --")

    r21 = conn.send(f"DELETE_REQ {cid}:7 {ke}")
    check(assert_eq("DELETE_REQ su chiave mai esistita", r21, "NOT_FOUND"))

    r22 = conn.send(f"DELETE_REQ {cid}:7 {ke}")
    check(assert_eq("DELETE_REQ replay di NOT_FOUND", r22, "NOT_FOUND"))


    # Riepilogo.
    conn.close()
    print(f"\n{'='*50}")
    print(f"Risultati: {passed} PASS, {failed} FAIL")
    print(f"{'='*50}")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()