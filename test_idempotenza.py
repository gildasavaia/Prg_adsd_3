import argparse
import socket
import sys
import uuid

""" Verifica che i retry delle operazioni di riscrittura non producano effetti doppi.
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
    # Dimostra la garanzia principale: se un client invia una richiesta mutativa SET_REQ
    # e poi la ripete alla cieca simulando un retry, il server riconosce il duplicato.
    # Il server restituisce la stessa risposta ma non riesegue la scrittura sul database,
    # mantenendo la versione del dato invariata.
    print("\n-- Scenario 1: SET_REQ idempotente --")

    # Prima esecuzione. Invia il comando SET_REQ originale con sequenza 0 e salva la risposta in r1.
    r1 = conn.send(f"SET_REQ {cid}:0 {ka} hello")
    # Controlla che la risposta sia "OK version=0" e aggiorna il contatore tramite check().
    check(assert_eq("SET_REQ prima esecuzione", r1, "OK version=0"))

    # Replay della stessa richiesta. Invia esattamente la stessa stringa (stesso request_id, stessa chiave, stesso valore).
    r2 = conn.send(f"SET_REQ {cid}:0 {ka} hello")
    # Controlla che la seconda risposta (r2) sia un replay identico alla prima (r1).
    check(assert_eq("SET_REQ replay stessa risposta", r2, r1))

    # Invia una lettura con GETV sulla chiave appena scritta per controllarne lo stato interno.
    r3 = conn.send(f"GETV {ka}")
    # Il test passa solo se la versione letta è ancora 0. Se fosse 1 significherebbe doppio effetto.
    check(assert_eq("GETV versione non incrementata", r3, "OK hello version=0"))


    # Scenario 2: CAS_REQ idempotente.
    # Come il primo scenario, ma per l'operazione CAS.
    # Verifica che anche in caso di retry di una CAS, l'effetto avvenga una sola volta.
    print("\n-- Scenario 2: CAS_REQ idempotente --")

    # Prima esecuzione. Invia un CAS_REQ (sequenza 1) che aggiorna ka solo se attualmente è a versione 0.
    r4 = conn.send(f"CAS_REQ {cid}:1 {ka} 0 world")
    # La CAS ha successo, la versione sale a 1, controlliamo che la risposta sia corretta.
    check(assert_eq("CAS_REQ prima esecuzione", r4, "OK version=1"))

    # Replay della stessa richiesta. Il client riprova inviando di nuovo la stessa identica CAS_REQ.
    r5 = conn.send(f"CAS_REQ {cid}:1 {ka} 0 world")
    # Il server prende la risposta in cache (r4) e la restituisce.
    check(assert_eq("CAS_REQ replay stessa risposta", r5, r4))

    # Interroga il server con GETV per vedere come stanno i dati.
    r6 = conn.send(f"GETV {ka}")
    # Verifica che il dato è "world" e la versione è ancora a 1 e non è saltata a 2.
    check(assert_eq("GETV dopo CAS_REQ", r6, "OK world version=1"))


    # Scenario 3: DELETE_REQ idempotente.
    # Testa l'idempotenza sull'operazione di cancellazione. Se un client ripete una DELETE,
    # il server deve rispondere OK (la risposta originaria) e NON "NOT_FOUND".
    print("\n-- Scenario 3: DELETE_REQ idempotente --")

    # Prima esecuzione. Invia il comando di cancellazione per la chiave "ka".
    r7 = conn.send(f"DELETE_REQ {cid}:2 {ka}")
    # Controlla che il server confermi l'eliminazione con "OK".
    check(assert_eq("DELETE_REQ prima esecuzione", r7, "OK"))

    # Replay della stessa richiesta. Rinviamo la cancellazione. La chiave non c'è più, ma avendo lo stesso ID, il server deduplica.
    r8 = conn.send(f"DELETE_REQ {cid}:2 {ka}")
    # Verifica che la risposta sia ancora "OK" per la logica del replay.
    check(assert_eq("DELETE_REQ replay stessa risposta", r8, "OK"))

    # Proviamo a leggere la chiave appena cancellata.
    r9 = conn.send(f"GET {ka}")
    # Verifichiamo che il server ci risponda che non esiste.
    check(assert_eq("GET dopo DELETE_REQ", r9, "NOT_FOUND"))


    # Scenario 4: Richieste diverse sulla stessa chiave.
    # Dimostra che la deduplicazione si basa rigorosamente sul request_id e non sulla chiave.
    # Due scritture diverse sulla stessa chiave da parte dello stesso client non devono
    # bloccare la seconda, ma applicarle in sequenza.
    print("\n-- Scenario 4: Richieste diverse, stessa chiave --")

    # Invia una SET_REQ sulla nuova chiave "kb".
    r10 = conn.send(f"SET_REQ {cid}:3 {kb} uno")
    # Verifica la corretta creazione a versione 0.
    check(assert_eq("SET_REQ beta primo", r10, "OK version=0"))

    # Invia una nuova SET_REQ sulla stessa identica chiave "kb".
    r11 = conn.send(f"SET_REQ {cid}:4 {kb} due")
    # Il server non si fa ingannare dalla chiave, vede seq 4, la esegue e la versione sale a 1.
    check(assert_eq("SET_REQ beta secondo", r11, "OK version=1"))

    # Le due richieste hanno request_id diversi, per questo motivo non si confondono. Leggiamo lo stato finale di kb.
    r12 = conn.send(f"GETV {kb}")
    # Verifichiamo che l'ultima scrittura abbia effettivamente vinto e sia consolidata.
    check(assert_eq("GETV beta valore finale", r12, "OK due version=1"))


    # Scenario 5: CAS_REQ fallita + replay.
    # Dimostra che il sistema memorizza e ripete anche gli errori semantici.
    # La risposta memorizzata nella cache vince sempre sul ricalcolo in tempo reale.
    print("\n-- Scenario 5: CAS_REQ fallita + replay --")

    # Tenta una CAS con una precondizione falsa, perchè beta è a 1 e non a 99.
    r13 = conn.send(f"CAS_REQ {cid}:5 {kb} 99 tre")
    # Il server se ne accorge, fallisce l'operazione e restituisce un errore.
    check(assert_eq("CAS_REQ fallita", r13, "ERR version_mismatch current=1"))

    # Replay della CAS fallita: deve restituire lo stesso errore.
    r14 = conn.send(f"CAS_REQ {cid}:5 {kb} 99 tre")
    # Il server non la ricalcola, ci dà indietro direttamente lo stesso errore di prima.
    check(assert_eq("CAS_REQ fallita replay", r14, r13))


    # Scenario 6: Formato request_id errato.
    # Testa la solidità del parser del server, ovvero la robustezza ai formati errati.
    # In tutti i casi di sintassi non corretta per il request_id, il server deve
    # sollevare un errore controllato di tipo ERR invalid_request_id.
    print("\n-- Scenario 6: Formato request_id errato --")

    # Manca il separatore ":" nell'ID.
    r15 = conn.send(f"SET_REQ badformat {kc} hello")
    # Il test verifica la risposta di errore.
    check(assert_eq("SET_REQ senza ':'", r15, "ERR invalid_request_id"))

    # Il numero di sequenza è una stringa alfanumerica e non un intero.
    r16 = conn.send(f"SET_REQ test:abc {kc} hello")
    # Il test verifica la risposta di errore.
    check(assert_eq("SET_REQ seq non numerica", r16, "ERR invalid_request_id"))

    # Manca la parte stringa del client_id prima dei due punti.
    r17 = conn.send(f"SET_REQ :5 {kc} hello")
    # Il test verifica la risposta di errore.
    check(assert_eq("SET_REQ client_id vuoto", r17, "ERR invalid_request_id"))


    # Scenario 7: retry con lo stesso request_id ma con argomenti diversi.
    # Se un client malevolo tenta un replay cambiando i parametri del payload, il server deve ignorarli.
    # L'effetto sul DB resta inalterato e viene restituita semplicemente la vecchia risposta memorizzata.
    print("\n-- Scenario 7: retry con argomenti diversi --")

    # Esegue una scrittura normale su kd.
    r18 = conn.send(f"SET_REQ {cid}:6 {kd} originale")
    # Controlla la corretta esecuzione iniziale.
    check(assert_eq("SET_REQ prima esecuzione (delta)", r18, "OK version=0"))

    # Invia un retry, stesso ID, stessa chiave, ma prova a iniettare "valore_diverso".
    r19 = conn.send(f"SET_REQ {cid}:6 {kd} valore_diverso")
    # Il server restituisce l'OK originale. Ha ignorato il nuovo payload.
    check(assert_eq("SET_REQ retry con argomenti diversi -> replay originale", r19, r18))

    # Leggiamo il valore della chiave sul database per avere la conferma empirica.
    r20 = conn.send(f"GETV {kd}")
    # Verifichiamo che il valore sia effettivamente rimasto "originale".
    check(assert_eq("GETV valore non modificato dal retry", r20, "OK originale version=0"))


    # Scenario 8: DELETE_REQ su chiave mai esistita, ripetuto.
    # Dimostra che l'idempotenza sui fallimenti si applica anche a comandi che
    # non partono da una versione numerica, come le cancellazioni a vuoto.
    print("\n-- Scenario 8: DELETE_REQ NOT_FOUND idempotente --")

    # Tenta di cancellare la chiave "ke" che non abbiamo mai creato in tutto lo script.
    r21 = conn.send(f"DELETE_REQ {cid}:7 {ke}")
    # Il server risponde giustamente NOT_FOUND.
    check(assert_eq("DELETE_REQ su chiave mai esistita", r21, "NOT_FOUND"))

    # Ritentiamo lo stesso comando di cancellazione.
    r22 = conn.send(f"DELETE_REQ {cid}:7 {ke}")
    # Il server usa la tabella di deduplicazione e fa il replay del NOT_FOUND originario.
    check(assert_eq("DELETE_REQ replay di NOT_FOUND", r22, "NOT_FOUND"))


    # Riepilogo.
    conn.close()
    print(f"\n{'='*50}")
    print(f"Risultati: {passed} PASS, {failed} FAIL")
    print(f"{'='*50}")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()