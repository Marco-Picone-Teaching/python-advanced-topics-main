# Riferimenti di Studio & Esperimenti — Software Engineering in Python

---

## 1. Threading, Async, Thread Pool — concorrenza in Python

### Concetti fondamentali (GIL, concorrenza vs parallelismo)
- **Real Python — What Is the Python Global Interpreter Lock (GIL)?**
  https://realpython.com/python-gil/
- **Real Python — Speed Up Your Python Program With Concurrency**
  https://realpython.com/python-concurrency/
- **Concurrency vs Parallelism in Python: A Deep Dive Using asyncio, threading e multiprocessing** (ottimo articolo di sintesi con 5 esempi pratici comparati)
  https://medium.com/@CodeWithHannan/concurrency-vs-parallelism-in-python-a-deep-dive-using-asyncio-threading-and-multiprocessing-dee9e3429ce3
- **Async vs Threading vs Multiprocessing in Python — guida pratica** (criterio decisionale I/O-bound vs CPU-bound)
  https://peerlist.io/pravinkunnure9/articles/async-vs-threading-vs-multiprocessing-inpython

### Threading
- **Real Python — An Intro to Threading in Python**
  https://realpython.com/intro-to-python-threading/
- **Documentazione ufficiale `threading`**
  https://docs.python.org/3/library/threading.html

### Thread pool e task paralleli (alto livello)
- **Documentazione ufficiale `concurrent.futures`** — `ThreadPoolExecutor`, `ProcessPoolExecutor`, `Future` (fonte primaria: da studiare con attenzione, è l'API moderna consigliata per gestire pool di thread/processi)
  https://docs.python.org/3/library/concurrent.futures.html

### Asyncio
- **Real Python — Async IO in Python: A Complete Walkthrough**
  https://realpython.com/async-io-python/
- **Documentazione ufficiale `asyncio` — Coroutines and Tasks**
  https://docs.python.org/3/library/asyncio-task.html
- **Documentazione ufficiale `asyncio` — Event Loop**
  https://docs.python.org/3/library/asyncio-eventloop.html

#### Cosa sono le coroutine

Una **coroutine** è una funzione definita con `async def` che può **sospendere** la propria esecuzione in un punto (con `await`) e restituire il controllo all'event loop, il quale nel frattempo può eseguire altre coroutine. A differenza di un thread, non c'è preemption: il passaggio di controllo avviene solo nei punti espliciti di `await`, mai a metà di una riga di codice — questo elimina intere classi di race condition tipiche del multithreading.

Differenze chiave rispetto a una funzione normale:

| | Funzione normale | Coroutine (`async def`) |
|---|---|---|
| Chiamata (`f()`) | Esegue subito il corpo | Non esegue nulla: crea un **coroutine object** |
| Esecuzione effettiva | Immediata | Serve `await f()` oppure schedularla come `Task` nell'event loop |
| Può sospendersi? | No | Sì, nei punti `await` (es. in attesa di I/O) |
| Concorrenza | Nessuna (blocca il thread) | Cooperativa: altre coroutine girano mentre questa è sospesa |

Esempio minimo:

```python
import asyncio

async def scarica(nome, secondi):
    print(f"{nome}: inizio")
    await asyncio.sleep(secondi)   # sospende QUESTA coroutine, non blocca il thread
    print(f"{nome}: fine")
    return f"{nome} completato"

async def main():
    # eseguite in modo concorrente: mentre una è in await, girano le altre
    risultati = await asyncio.gather(
        scarica("A", 2),
        scarica("B", 1),
        scarica("C", 3),
    )
    print(risultati)

asyncio.run(main())   # crea l'event loop, esegue main(), lo chiude
```

Output atteso (l'ordine di fine riflette i tempi di `sleep`, non l'ordine di avvio):
```
A: inizio
B: inizio
C: inizio
B: fine     # dopo 1s
A: fine     # dopo 2s
C: fine     # dopo 3s
['A completato', 'B completato', 'C completato']
```

Punti da tenere a mente per lo use case:
- `asyncio.run(main())` è il punto di ingresso standard: crea l'event loop, ci esegue la coroutine e lo chiude.
- `await` funziona solo dentro un'altra coroutine (o direttamente nel corpo eseguito da `asyncio.run`).
- Una coroutine che fa lavoro CPU-bound senza mai fare `await` **blocca comunque l'intero event loop** — le coroutine aiutano solo con l'I/O-bound (rete, file, sleep), non danno parallelismo reale su CPU.
- `asyncio.Task` (creato con `asyncio.create_task(coroutine)`) è ciò che permette a una coroutine di girare "in background" mentre il resto del codice continua, invece di doverla attendere subito con `await`.

### Differenze tra versioni di Python (rilevanti per la concorrenza)
- **Python 3.13 — Free-threading (rimozione opzionale del GIL, PEP 703)**: cambiamento più importante degli ultimi anni sul fronte concorrenza. Da citare esplicitamente nella tesi come evoluzione recente.
  - Approfondimento tecnico: https://algomaster.io/learn/concurrency-interview/python-free-threading
  - PEP di riferimento: consultare direttamente **PEP 703** su peps.python.org
- Da verificare sul changelog ufficiale ("What's New") di ogni versione, sezione concorrenza:
  - `asyncio.TaskGroup` e miglior gestione delle eccezioni concorrenti (Python 3.11)
  - `Queue.shutdown()` nel modulo `queue` (Python 3.13)
  - modifiche a `max_workers` di default in `ThreadPoolExecutor` (Python 3.13)

---

## 2. Il modulo `queue` — code thread-safe in Python

Nota terminologica: in Python non esiste una classe a sé chiamata così; il modulo standard si chiama **`queue`** (minuscolo, rinominato da `Queue` nel passaggio a Python 3) e fornisce le classi `Queue`, `LifoQueue`, `PriorityQueue`, `SimpleQueue`. È l'equivalente concettuale di una `LinkedBlockingQueue` Java: coda bloccante, thread-safe, pensata per lo scambio dati tra thread produttore/consumatore.

- **Documentazione ufficiale `queue`** (fonte primaria, da leggere per prima)
  https://docs.python.org/3/library/queue.html
- **Real Python — Python's Queue: 3 Types and Their Uses** (guida pratica con esempi FIFO/LIFO/Priority e pattern producer-consumer multi-thread)
  https://realpython.com/python-queue/
- **PriorityQueue e LifoQueue in Python** (esempi minimi e diretti)
  https://medium.com/@devansheerupabheda/priorityqueue-and-lifoqueue-implementation-in-python-6c6fea90c99

### Le classi disponibili nel modulo `queue`

Il modulo `queue` fornisce quattro classi, che differiscono solo per l'**ordine di estrazione** degli elementi (tranne `SimpleQueue`, che differisce anche per funzionalità):

| Classe | Ordine di estrazione | Note |
|---|---|---|
| `queue.Queue(maxsize=0)` | **FIFO** (First In, First Out) — il primo elemento inserito è il primo estratto | La più usata; caso base per pattern producer-consumer |
| `queue.LifoQueue(maxsize=0)` | **LIFO** (Last In, First Out) — si comporta come uno stack | Utile quando l'elemento più recente va processato per primo |
| `queue.PriorityQueue(maxsize=0)` | Per **priorità** — l'elemento con valore più basso viene estratto per primo (internamente usa `heapq`) | Gli elementi si inseriscono tipicamente come tuple `(priorità, dato)` |
| `queue.SimpleQueue()` | FIFO, come `Queue` | Versione "leggera": non supporta `maxsize`, `task_done()`/`join()` né altre funzionalità di task-tracking; in cambio ha un'implementazione più semplice e alcune garanzie aggiuntive (es. è reentrant-safe) |

A queste si affiancano, per completezza, le due eccezioni sollevate dalle operazioni non bloccanti:
- `queue.Empty` — sollevata da `get_nowait()` (o `get(block=False)`) su una coda vuota
- `queue.Full` — sollevata da `put_nowait()` (o `put(block=False)`) su una coda piena

Esempi rapidi:

```python
import queue

# FIFO
q = queue.Queue()
q.put("a"); q.put("b")
q.get()   # -> "a"

# LIFO
q = queue.LifoQueue()
q.put("a"); q.put("b")
q.get()   # -> "b"

# Priorità (tuple: priorità, dato — priorità più bassa = estratta per prima)
q = queue.PriorityQueue()
q.put((3, "bassa")); q.put((1, "alta"))
q.get()   # -> (1, "alta")

# SimpleQueue
q = queue.SimpleQueue()
q.put("x")
q.get()   # -> "x"  (niente task_done/join disponibili)
```

Metodi principali della classe `Queue` (analoghi anche su `LifoQueue` e `PriorityQueue`):

| Metodo | Comportamento |
|---|---|
| `put(item, block=True, timeout=None)` | Inserisce un elemento. Se la coda è piena (`maxsize` raggiunto) **blocca** il thread chiamante finché non si libera spazio (a meno di `block=False` o `timeout`). |
| `get(block=True, timeout=None)` | Estrae un elemento. Se la coda è vuota **blocca** il thread chiamante finché non arriva un elemento. |
| `put_nowait(item)` / `get_nowait()` | Varianti non bloccanti: sollevano `queue.Full` / `queue.Empty` invece di attendere. |
| `task_done()` | Segnala che un elemento estratto con `get()` è stato elaborato. |
| `join()` | Blocca finché tutti gli elementi inseriti non sono stati elaborati (cioè finché `task_done()` è stato chiamato per ognuno). |
| `qsize()`, `empty()`, `full()` | Stato della coda (indicativo, non garantito in presenza di concorrenza). |

Esempio classico producer–consumer con thread:

```python
import threading
import queue
import time

q = queue.Queue(maxsize=10)

def producer():
    for i in range(20):
        q.put(f"item-{i}")   # si blocca se la coda è piena
        time.sleep(0.05)

def consumer():
    while True:
        item = q.get()        # si blocca se la coda è vuota
        print(f"elaboro {item}")
        time.sleep(0.1)
        q.task_done()

threading.Thread(target=producer, daemon=True).start()
for _ in range(3):             # pool "manuale" di 3 worker
    threading.Thread(target=consumer, daemon=True).start()

q.join()  # attende che tutti gli item siano stati elaborati
```

Il blocco automatico di `put()`/`get()` è il motivo per cui `queue.Queue` è **la** struttura dati di riferimento per la sincronizzazione produttore/consumatore tra thread: non servono lock espliciti, la logica di attesa/segnalazione è già incapsulata nella classe.

### È utile negli scenari sopra/sotto descritti?

- **Thread pool (sez. 1):** è il meccanismo *interno* con cui `concurrent.futures.ThreadPoolExecutor` distribuisce i task ai thread worker (collegamento diretto tra le due sezioni).
- **Architetture event-driven (sez. 3):** sì, è uno dei modi più semplici per costruire un event bus locale multi-thread: i producer fanno `put()` di eventi, uno o più worker thread fanno `get()` in loop e li dispatchano ai relativi handler. È la base concettuale anche di soluzioni più complesse (Celery, broker esterni), solo che lì la coda è distribuita/persistente invece che in-memory.
- **Scenari asincroni (`asyncio`):** `queue.Queue` **non va usata** dentro codice `async`/`await`, perché il suo blocco è a livello di thread e bloccherebbe l'intero event loop. In quel contesto va usata `asyncio.Queue`, che ha la stessa API (`put`, `get`, `task_done`, `join`) ma non bloccante rispetto al thread, bensì basata su `await`.
- **Scenari multi-processo:** se i produttori/consumatori sono processi separati (non thread), `queue.Queue` non funziona (non è condivisibile tra processi): serve `multiprocessing.Queue`, concettualmente analoga ma basata su IPC.

### Da vedere in relazione a `queue`
- `collections.deque` — alternativa non thread-safe ma molto più veloce per code semplici in singolo thread (citata anche dalla doc ufficiale come "see also")
- `multiprocessing.Queue` — l'equivalente pensato per lo scambio dati **tra processi** (da confrontare con `queue.Queue`, pensata per i thread)
- `asyncio.Queue` — versione asincrona della coda, da usare con `async/await` invece che con thread (vedi sez. 1)

---

## 3. Architetture Event-Driven in Python

Concetti di base: producer/consumer, event bus, publish/subscribe, message broker.

- **Mastering Python's Event-Driven Architecture** (guida pratica con asyncio, aiohttp, watchdog, event bus custom)
  https://medium.com/pythoneers/mastering-pythons-event-driven-architecture-building-asynchronous-systems-without-going-insane-5768b6a8cf3d
- **How to Build Event-Driven Systems with Python** (pattern con `asyncio.Event`, event bus custom, code di esempio)
  https://oneuptime.com/blog/post/2026-02-02-python-event-driven-systems/view

### Costruire un layer event-driven "in casa" con thread e `queue.Queue`

Ha senso, ed è un esercizio didattico solido: obbliga a ragionare su produttori, consumatori, disaccoppiamento tramite coda, backpressure (`maxsize`), gestione errori nei worker e shutdown pulito (sentinel value o `Queue.shutdown()` da Python 3.13). È concettualmente lo stesso pattern usato internamente da `ThreadPoolExecutor` e, a un livello più alto, da soluzioni come Celery o Kafka — solo che lì la coda è esterna e distribuita, qui è in-process e sotto il pieno controllo dello sviluppatore.

Struttura tipica di un event layer con thread + queue:

Struttura consigliata a tre classi, invece di funzioni sciolte:

- **`EventBus`** — il motore: possiede la coda, i thread worker e la mappa `tipo evento → callback`. Non viene usato direttamente dai componenti applicativi.
- **`EventHandler`** — classe base astratta che definisce il *contratto* di una callback (metodo `handle(event)`); ogni componente che vuole reagire a un evento implementa una sottoclasse.
- **`EventBusClient`** — facade esposta ai componenti applicativi: separa esplicitamente le due operazioni possibili, **scrittura** (`write`, cioè pubblicare un evento) e **lettura** (`read`, cioè iscriversi a un tipo di evento), senza esporre i dettagli interni dell'`EventBus` (coda, thread).

```python
import threading
import queue
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class Event:
    type: str
    payload: Any = None


class EventHandler(ABC):
    """Contratto di una callback: ogni handler concreto implementa handle()."""

    @abstractmethod
    def handle(self, event: Event) -> None:
        ...


class EventBus:
    """Motore dell'event layer: coda + worker pool + dispatch alle callback registrate."""

    def __init__(self, num_workers: int = 3):
        self._queue: queue.Queue[Event] = queue.Queue()
        self._handlers: dict[str, list[EventHandler]] = {}
        self._lock = threading.Lock()
        self._workers = [
            threading.Thread(target=self._run_worker, daemon=True, name=f"eventbus-worker-{i}")
            for i in range(num_workers)
        ]

    def start(self) -> None:
        for w in self._workers:
            w.start()

    def publish(self, event: Event) -> None:
        self._queue.put(event)

    def subscribe(self, event_type: str, handler: EventHandler) -> None:
        with self._lock:
            self._handlers.setdefault(event_type, []).append(handler)

    def _run_worker(self) -> None:
        while True:
            event = self._queue.get()
            for handler in self._handlers.get(event.type, []):
                try:
                    handler.handle(event)
                except Exception as exc:
                    print(f"Errore nell'handler {handler}: {exc}")
            self._queue.task_done()


class EventBusClient:
    """Interfaccia usata dai componenti applicativi: solo write (publish) e read (subscribe)."""

    def __init__(self, bus: EventBus):
        self._bus = bus

    def write(self, event_type: str, payload: Any = None) -> None:
        self._bus.publish(Event(type=event_type, payload=payload))

    def read(self, event_type: str, handler: EventHandler) -> None:
        self._bus.subscribe(event_type, handler)


# Esempio di callback concreta
class LoggingHandler(EventHandler):
    def handle(self, event: Event) -> None:
        print(f"[LOG] evento ricevuto: {event.type} -> {event.payload}")


# Uso
bus = EventBus(num_workers=2)
bus.start()

client = EventBusClient(bus)
client.read("user.created", LoggingHandler())
client.write("user.created", {"id": 42, "name": "Marco"})
```

Questa separazione ha un vantaggio didattico preciso: i componenti applicativi dipendono solo da `EventBusClient` ed `EventHandler` (interfacce stabili), non dall'`EventBus` concreto — è più facile da testare (si può mockare l'`EventBusClient`) e rende esplicito, leggendo il codice di un componente, se sta solo pubblicando eventi, solo ascoltando, o entrambe le cose.

**Limiti da conoscere (importanti per la tesi):**

- **GIL**: con thread puri, il vantaggio reale c'è solo per handler I/O-bound (rilasciano il GIL). Handler CPU-bound non traggono beneficio dal multithreading — servirebbe `multiprocessing`/`ProcessPoolExecutor`, e in quel caso `queue.Queue` va sostituita con `multiprocessing.Queue`.
- **Scala solo in un processo**: la coda vive in memoria in un unico processo — non è distribuita né persistente; se il processo termina, gli eventi in coda si perdono. Per scenari multi-servizio/multi-processo servono broker esterni.
- **Alternativa per carichi I/O-bound**: un event bus basato su `asyncio.Queue` + coroutine spesso è più efficiente dei thread per questo tipo di carico, perché evita l'overhead di context-switching e sfrutta meglio un singolo thread tramite l'event loop.

**Esperimento consigliato:** implementare lo stesso event layer due volte — una con `threading` + `queue.Queue`, una con `asyncio` + `asyncio.Queue` — e confrontare prestazioni, leggibilità e complessità del codice a parità di scenario (es. dispatch di N eventi verso M handler). Questo confronto è già richiamato nella nota metodologica finale (punti 1 e 2).

---

## 4. Testing, Test Suite e Unit Test in Python

### Concetti di base e documentazione ufficiale
- **Documentazione ufficiale `unittest`**
  https://docs.python.org/3/library/unittest.html
- **Real Python — Python's unittest: Writing Unit Tests for Your Code** (test case, fixture, test suite: guida completa)
  https://realpython.com/ref/stdlib/unittest/

### pytest (framework di terze parti, standard de facto nell'ecosistema Python)
- Documentazione ufficiale: **docs.pytest.org** (da consultare come fonte primaria)
- **pytest vs unittest: Which Should You Use?** (confronto diretto con esempi di codice, utile come primo confronto pratico)
  https://pydevtools.com/handbook/explanation/pytest-vs-unittest/
- **Unittest vs pytest in Python: Key Differences and When to Use** (tabella comparativa sintetica)
  https://leyaa.ai/codefly/learn/python/qna/unittest-vs-pytest-in-python

### Temi da approfondire nella parte di testing
- Fixture e dependency injection (pytest) vs `setUp`/`tearDown` (unittest)
- Parametrizzazione dei test (`@pytest.mark.parametrize`)
- Mock e patching: modulo ufficiale `unittest.mock` — https://docs.python.org/3/library/unittest.mock.html
- Test asincroni: `pytest-asyncio` per testare codice basato su `asyncio`
- Test di codice concorrente/multi-thread: tecniche di sincronizzazione nei test, uso di `Event`/`Queue` per rendere deterministici test su thread

