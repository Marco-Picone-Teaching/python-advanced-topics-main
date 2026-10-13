"""
Section D - The queue Module
Section D.2.1 - The four queue classes: they differ in the ORDER items come out.
"""

import queue

# The same readings are put into every queue, in this order
READINGS = ["temp-01=21.5", "temp-01=21.7", "temp-01=22.0"]


def drain(q: queue.Queue | queue.SimpleQueue) -> list[str]:
    """Take every item out of the queue (non-blocking) and return them in extraction order."""
    items = []
    while True:
        try:
            items.append(q.get_nowait())
        except queue.Empty:
            return items


def main() -> None:
    fifo: queue.Queue[str] = queue.Queue()
    lifo: queue.LifoQueue[str] = queue.LifoQueue()
    simple: queue.SimpleQueue[str] = queue.SimpleQueue()
    for q in (fifo, lifo, simple):
        for r in READINGS:
            q.put(r)

    print(f"Queue       (FIFO): {drain(fifo)}")      # oldest first: a telemetry stream
    print(f"LifoQueue   (LIFO): {drain(lifo)}")      # newest first: 'latest value wins'
    print(f"SimpleQueue (FIFO): {drain(simple)}")

    # PriorityQueue: the SMALLEST item comes out first -> (priority, payload) tuples
    prio: queue.PriorityQueue[tuple[int, str]] = queue.PriorityQueue()
    prio.put((5, "telemetry: temp-01=21.5"))
    prio.put((1, "ALARM: smoke detected in room-104"))
    prio.put((3, "command: light-02 OFF"))
    prio.put((1, "ALARM: CO2 above 1500 ppm in room-101"))
    print("PriorityQueue:")
    for item in drain(prio):
        print(f"   {item}")


if __name__ == "__main__":
    main()
