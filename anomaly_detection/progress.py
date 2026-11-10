"""Nonblocking query progress collection with explicit dropped-event accounting."""
import json
import queue
import threading
from datetime import datetime, timezone
from pathlib import Path


def progress_listener(path, checkpoint):
    from pyspark.sql.streaming import StreamingQueryListener
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    messages = queue.Queue(maxsize=10000)
    dropped = [0]
    errors = []
    terminated = threading.Event()

    def writer():
        try:
            with target.open('a', encoding='utf-8') as stream:
                while True:
                    item = messages.get()
                    if item is None:
                        return
                    stream.write(json.dumps(item) + '\n')
                    stream.flush()
        except Exception as error:
            errors.append(str(error))

    thread = threading.Thread(target=writer, daemon=True)
    thread.start()

    class Listener(StreamingQueryListener):
        def onQueryStarted(self, event):
            pass

        def onQueryProgress(self, event):
            item = json.loads(event.progress.json)
            item['observed_completed_at'] = datetime.now(timezone.utc).isoformat()
            # The progress trigger timestamp differs from the state function's
            # batch processing time. Join through Spark's persisted offset log.
            offset = Path(checkpoint) / 'offsets' / str(item['batchId'])
            try:
                item['batch_started_ms'] = json.loads(offset.read_text().splitlines()[1])['batchTimestampMs']
            except (OSError, ValueError, KeyError, IndexError) as error:
                errors.append(f'Cannot map batch {item["batchId"]} to state time: {error}')
            item['listener_dropped_events'] = dropped[0]
            try:
                messages.put_nowait(item)
            except queue.Full:
                dropped[0] += 1

        def onQueryTerminated(self, event):
            terminated.set()

        def onQueryIdle(self, event):
            pass

        def await_termination(self, timeout=15):
            if not terminated.wait(timeout):
                raise RuntimeError('Timed out waiting for final query progress callbacks')

        def close(self):
            if thread.is_alive():
                messages.put(None, timeout=10)
                thread.join(timeout=10)
            if errors:
                raise RuntimeError('Progress logging failed: ' + '; '.join(errors))
            if thread.is_alive():
                raise RuntimeError('Progress logger did not drain')
            if dropped[0]:
                raise RuntimeError(f'Progress logger dropped {dropped[0]} events')

    return Listener()
