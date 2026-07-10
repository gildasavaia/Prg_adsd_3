import argparse
import random
import socket
import sys
import threading
import time
import uuid
from queue import Queue

"""
Simula piu' client concorrenti che inviano operazioni mutative con retry
randomici. Verifica che il valore finale sia coerente e che nessun
doppio effetto si sia verificato.

Il server deve essere in esecuzione su 127.0.0.1:6460 prima di lanciare
questo script.
"""

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stress test concorrente.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6460)
    parser.add_argument("--clients", type=int, default=5,
                        help="Numero di client concorrenti")
    parser.add_argument("--ops-per-client", type=int, default=20,
                        help="Operazioni SET_REQ per client")
    parser.add_argument("--retry-prob", type=float, default=0.3,
                        help="Probabilita' di retry per ogni operazione")
    parser.add_argument("--max-extra-retries", type=int, default=3,
                        help="Numero massimo di retry extra per operazione")
    parser.add_argument("--window", type=int, default=100,
                        help="MAX_WINDOW del server (per validare i parametri)")
    return parser.parse_args()


class RawClient:
    """Client TCP minimale per test."""

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

# Questa funzione verrà lanciata in parallelo da ogni thread. Rappresenta la vita
# autonoma di un client che invia comandi e, casualmente, "impazzisce" inviando retry.
def worker(
    client_id: str,
    host: str,
    port: int,
    num_ops: int,
    retry_prob: float,
    max_extra_retries: int,
    key: str,
    results: Queue,
) -> None:
    """ Worker thread: invia num_ops SET_REQ su key, ciascuna con un valore diverso. Dopo ogni invio,
    con probabilita' retry_prob rimanda la stessa richiesta (simulando un retry dopo timeout)."""
    client = RawClient(host, port)
    errors = []
    total_sent = 0
    total_retries = 0

    try:
        for seq in range(num_ops):
            request_id = f"{client_id}:{seq}"
            value = f"{client_id}_v{seq}"
            command = f"SET_REQ {request_id} {key} {value}"

            # Invio principale
            response = client.send(command)
            total_sent += 1

            if not response.startswith("OK"):
                errors.append(f"Unexpected response for {request_id}: {response}")

            # Retry simulati
            if random.random() < retry_prob:
                extra = random.randint(1, max_extra_retries)
                for _ in range(extra):
                    retry_response = client.send(command)
                    total_retries += 1

                    # Il retry DEVE restituire la stessa risposta
                    if retry_response != response:
                        errors.append(
                            f"Retry mismatch for {request_id}: "
                            f"original={response}, retry={retry_response}"
                        )

    except Exception as exc:
        errors.append(f"Exception in {client_id}: {exc}")
    finally:
        client.close()

    results.put({
        "client_id": client_id,
        "total_sent": total_sent,
        "total_retries": total_retries,
        "errors": errors,
    })


# In questo scenario 10 client fanno retry simultaneo della stessa richiesta.
def run_exact_collision_test(host: str, port: int, run_id: str, num_threads: int = 10) -> list[str]:
    """
    Test di collisione esatta sulla sezione critica.

    A differenza dello stress test principale (client diversi, seq_num diversi),
    qui N thread inviano contemporaneamente lo stesso identico request_id sulla
    stessa chiave, simulando il caso peggiore: piu' retry della stessa richiesta
    in volo nello stesso istante (es. timeout del client scattato mentre la
    prima risposta era gia' in transito).

    Verifica che:
    - tutte le risposte siano identiche (nessun thread osserva uno stato
      intermedio diverso dagli altri);
    - l'effetto sia applicato una sola volta sul KV store, non num_threads volte.
    """
    client_id = f"collision_{run_id}"
    key = f"collision_{run_id}"

    # Baseline: crea la chiave con un valore iniziale (seq=1)
    setup_client = RawClient(host, port)
    setup_client.send(f"SET_REQ {client_id}:1 {key} initial_val")
    setup_client.close()

    # Tutti i thread inviano esattamente lo stesso comando (stesso request_id)
    command = f"SET_REQ {client_id}:999 {key} conc_val"
    results: list[str] = []
    errors: list[str] = []
    lock = threading.Lock()

    def worker() -> None:
        try:
            client = RawClient(host, port)
            try:
                response = client.send(command)
                with lock:
                    results.append(response)
            finally:
                client.close()
        except Exception as exc:
            with lock:
                errors.append(f"Exception durante la collisione: {exc}")

    threads = [threading.Thread(target=worker) for _ in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    print(f"\n{'='*50}")
    print(f"Test di collisione esatta: {num_threads} thread, stesso request_id")
    print(f"{'='*50}")

    if errors:
        print(f"  FAIL  {len(errors)} errori di connessione")
        return errors

    if len(results) != num_threads:
        errors.append(
            f"Attese {num_threads} risposte alla collisione, ricevute {len(results)}"
        )
        print(f"  FAIL  numero di risposte inatteso ({len(results)}/{num_threads})")
        return errors

    first_response = results[0]
    if not first_response.startswith("OK version="):
        errors.append(f"Risposta inattesa alla collisione: {first_response}")
        print(f"  FAIL  risposta inattesa: {first_response}")
        return errors

    if all(r == first_response for r in results):
        print(f"  PASS  tutte le {num_threads} risposte sono identiche ({first_response})")
    else:
        errors.append(f"Risposte diverse tra thread concorrenti: {set(results)}")
        print(f"  FAIL  risposte diverse tra thread concorrenti: {set(results)}")

    # Verifica finale: l'effetto deve essere stato applicato una sola volta.
    # version=0 dal setup, version=1 dalla collisione (se applicata una sola volta).
    check_client = RawClient(host, port)
    final_response = check_client.send(f"GETV {key}")
    check_client.close()

    expected = "OK conc_val version=1"
    if final_response == expected:
        print(f"  PASS  effetto applicato una sola volta (GETV: {final_response})")
    else:
        errors.append(
            f"Possibile doppio effetto: GETV={final_response}, atteso={expected}"
        )
        print(f"  FAIL  possibile doppio effetto: GETV={final_response}, atteso={expected}")

    return errors


def main() -> None:
    args = parse_args()
    results: Queue = Queue()
    run_id = uuid.uuid4().hex[:8]
    key = f"stress_{run_id}"
    print(f"Run ID: {run_id}  (chiave={key})")

    # ── Guardia: verifica che nessun client sfori la propria finestra GC ──
    # MAX_WINDOW e' una finestra per-client (request_table[client_id]), non
    # globale: ogni client ha una propria tabella di deduplicazione. Il
    # controllo va quindi fatto su ops_per_client, non sulla somma di tutti
    # i client.
    if args.ops_per_client > args.window:
        print(f"\n  ERRORE: ops-per-client ({args.ops_per_client}) supera la")
        print(f"          MAX_WINDOW del server ({args.window}).")
        print(f"          Ogni client ha una finestra di deduplicazione indipendente:")
        print(f"          superarla farebbe scadere request_id di quel client durante")
        print(f"          il test, rendendo il calcolo della versione attesa inaffidabile.")
        print(f"          Riduci --ops-per-client oppure alza --window")
        print(f"          (e il corrispondente MAX_WINDOW del server).")
        sys.exit(2)

    # Reset: crea la chiave con un valore iniziale
    setup_client = RawClient(args.host, args.port)
    setup_client.send(f"SET_REQ setup_{run_id}:0 {key} initial")
    setup_client.close()

    print(f"Stress test: {args.clients} client x {args.ops_per_client} ops")
    print(f"Retry probability: {args.retry_prob}, max extra retries: {args.max_extra_retries}")
    print()

    # Lancia i worker
    threads: list[threading.Thread] = []
    for i in range(args.clients):
        client_id = f"s{i}_{run_id}"
        t = threading.Thread(
            target=worker,
            args=(client_id, args.host, args.port, args.ops_per_client,
                  args.retry_prob, args.max_extra_retries, key, results),
            name=f"worker-{i}",
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    # Raccolta risultati
    total_sent = 0
    total_retries = 0
    all_errors: list[str] = []

    while not results.empty():
        r = results.get()
        total_sent += r["total_sent"]
        total_retries += r["total_retries"]
        all_errors.extend(r["errors"])
        if r["errors"]:
            print(f"  {r['client_id']}: {len(r['errors'])} errors")
        else:
            print(f"  {r['client_id']}: OK ({r['total_sent']} sent, {r['total_retries']} retries)")

    # Verifica finale: la chiave deve esistere con UNA versione coerente
    check_client = RawClient(args.host, args.port)
    getv_response = check_client.send(f"GETV {key}")
    check_client.close()

    # La versione dovrebbe essere:
    # 1 (dal setup:0) + totale SET_REQ unici da tutti i client
    # Ogni client ha fatto ops_per_client SET_REQ con request_id diversi
    expected_total_unique_ops = args.clients * args.ops_per_client + 1  # +1 per setup
    # La versione va da 0, quindi expected_version = expected_total_unique_ops - 1
    expected_version = expected_total_unique_ops - 1

    print(f"\n{'='*50}")
    print(f"Totale invii:       {total_sent}")
    print(f"Totale retries:     {total_retries}")
    print(f"Errori retry:       {len(all_errors)}")
    print(f"GETV finale:        {getv_response}")
    print(f"Versione attesa:    {expected_version}")

    # Parsing della versione dal response
    if "version=" in getv_response:
        actual_version = int(getv_response.split("version=")[1])
        if actual_version == expected_version:
            print(f"Versione corretta:  PASS")
        else:
            print(f"Versione corretta:  FAIL (attesa {expected_version}, ottenuta {actual_version})")
            all_errors.append(f"Version mismatch: expected {expected_version}, got {actual_version}")
    else:
        print(f"Versione corretta:  FAIL (response inatteso)")
        all_errors.append(f"Unexpected GETV response: {getv_response}")

    if all_errors:
        print(f"\nERRORI TROVATI:")
        for err in all_errors[:10]:
            print(f"  - {err}")
        if len(all_errors) > 10:
            print(f"  ... e altri {len(all_errors) - 10} errori")

    print(f"{'='*50}")

    # ── Test aggiuntivo: collisione esatta sulla sezione critica ──
    # N thread inviano contemporaneamente lo STESSO request_id, invece dei
    # seq_num diversi usati sopra. Verifica che il check-poi-esegui-poi-salva
    # dentro _dispatch_idempotent sia davvero atomico nel caso peggiore.
    collision_errors = run_exact_collision_test(args.host, args.port, run_id)
    all_errors.extend(collision_errors)

    if collision_errors:
        print(f"\nERRORI NEL TEST DI COLLISIONE:")
        for err in collision_errors:
            print(f"  - {err}")

    sys.exit(0 if not all_errors else 1)


if __name__ == "__main__":
    main()