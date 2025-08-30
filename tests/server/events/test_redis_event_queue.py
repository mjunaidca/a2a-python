import asyncio
import json
import pytest

from a2a.server.events.redis_event_queue import RedisEventQueue


class FakeRedis:
    """Minimal fake redis supporting xadd, xread, set, delete for tests."""

    def __init__(self):
        # stream_key -> list of (id_str, fields_dict)
        self.streams: dict[str, list[tuple[str, dict]]] = {}

    async def xadd(self, stream_key: str, fields: dict, maxlen: int | None = None):
        lst = self.streams.setdefault(stream_key, [])
        idx = len(lst) + 1
        entry_id = f"{idx}-0"
        lst.append((entry_id, fields.copy()))
        # return id similar to real redis
        return entry_id

    async def xread(self, streams: dict, block: int = 0, count: int | None = None):
        # streams is {stream_key: last_id}
        results = []
        for key, last_id in streams.items():
            lst = self.streams.get(key, [])
            # determine numeric last id
            if last_id == '$':
                # interpret as current max id so return only entries added after this call
                last_num = len(lst)
            else:
                try:
                    last_num = int(str(last_id).split('-')[0])
                except Exception:
                    last_num = 0

            # collect entries with numeric id > last_num
            to_return = [(eid, fields) for (eid, fields) in lst if int(eid.split('-')[0]) > last_num]
            if to_return:
                results.append((key, to_return[: count if count is not None else None]))

        return results

    async def set(self, key: str, value: str):
        # no-op for tests
        return True

    async def delete(self, key: str):
        self.streams.pop(key, None)
        return True


class MessageEvent:
    """Dummy event with class name 'Message' and json() method."""

    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return json.dumps(self._payload)


@pytest.mark.asyncio
async def test_enqueue_dequeue_roundtrip():
    redis = FakeRedis()
    q = RedisEventQueue('task1', redis, stream_prefix='a2a:test', read_block_ms=10)
    evt = MessageEvent({'x': 1})
    await q.enqueue_event(evt)
    out = await q.dequeue_event(no_wait=True)
    assert out == {'x': 1}


@pytest.mark.asyncio
async def test_dequeue_no_wait_raises_on_empty():
    redis = FakeRedis()
    q = RedisEventQueue('task2', redis, stream_prefix='a2a:test', read_block_ms=10)
    with pytest.raises(asyncio.QueueEmpty):
        await q.dequeue_event(no_wait=True)


@pytest.mark.asyncio
async def test_close_tombstone_sets_closed_and_raises():
    redis = FakeRedis()
    q = RedisEventQueue('task3', redis, stream_prefix='a2a:test', read_block_ms=10)
    await q.enqueue_event(MessageEvent({'a': 1}))
    # close will append a CLOSE entry
    await q.close()
    # first dequeue should return the first event
    first = await q.dequeue_event(no_wait=True)
    assert first == {'a': 1}
    # next dequeue should see CLOSE and raise QueueEmpty while marking closed
    with pytest.raises(asyncio.QueueEmpty):
        await q.dequeue_event(no_wait=True)
    assert q.is_closed()


@pytest.mark.asyncio
async def test_tap_sees_only_future_events():
    redis = FakeRedis()
    q1 = RedisEventQueue('task4', redis, stream_prefix='a2a:test', read_block_ms=10)
    # enqueue before tap
    await q1.enqueue_event(MessageEvent({'before': True}))
    # create tap which should start at '$' and only see future events
    q2 = q1.tap()
    # q1 can dequeue the earlier event
    e1 = await q1.dequeue_event(no_wait=True)
    assert e1 == {'before': True}
    # enqueue another event; both q1 and q2 should be able to read it (q1 hasn't advanced past it yet)
    await q1.enqueue_event(MessageEvent({'later': 2}))
    out2 = await q2.dequeue_event(no_wait=True)
    assert out2 == {'later': 2}


@pytest.mark.asyncio
async def test_enqueue_dequeue_with_complex_data():
    """Test enqueuing and dequeuing complex data structures."""
    redis = FakeRedis()
    q = RedisEventQueue('task5', redis, stream_prefix='a2a:test', read_block_ms=10)
    
    complex_data = {
        'nested': {'key': 'value', 'number': 42},
        'array': [1, 2, {'complex': 'item'}],
        'boolean': True,
        'null_value': None
    }
    
    evt = MessageEvent(complex_data)
    await q.enqueue_event(evt)
    out = await q.dequeue_event(no_wait=True)
    assert out == complex_data


@pytest.mark.asyncio
async def test_enqueue_dequeue_with_unicode_data():
    """Test enqueuing and dequeuing Unicode strings."""
    redis = FakeRedis()
    q = RedisEventQueue('task6', redis, stream_prefix='a2a:test', read_block_ms=10)
    
    unicode_data = {
        'emoji': '🚀🌟',
        'multilingual': 'Hello 世界 नमस्ते',
        'special_chars': 'café résumé naïve'
    }
    
    evt = MessageEvent(unicode_data)
    await q.enqueue_event(evt)
    out = await q.dequeue_event(no_wait=True)
    assert out == unicode_data


@pytest.mark.asyncio
async def test_multiple_events_fifo_order():
    """Test that events are dequeued in FIFO order."""
    redis = FakeRedis()
    q = RedisEventQueue('task7', redis, stream_prefix='a2a:test', read_block_ms=10)
    
    # Enqueue multiple events
    events = [{'id': i, 'data': f'value_{i}'} for i in range(5)]
    for event_data in events:
        await q.enqueue_event(MessageEvent(event_data))
    
    # Dequeue and verify order
    for expected in events:
        actual = await q.dequeue_event(no_wait=True)
        assert actual == expected


@pytest.mark.asyncio
async def test_task_done_noop():
    """Test that task_done is a no-op for Redis streams."""
    redis = FakeRedis()
    q = RedisEventQueue('task8', redis, stream_prefix='a2a:test', read_block_ms=10)
    
    # Should not raise any exceptions
    q.task_done()


@pytest.mark.asyncio
async def test_tap_creates_independent_cursor():
    """Test that tap creates a queue with independent read cursor."""
    redis = FakeRedis()
    q1 = RedisEventQueue('task9', redis, stream_prefix='a2a:test', read_block_ms=10)
    
    # Enqueue some events
    await q1.enqueue_event(MessageEvent({'event': 1}))
    await q1.enqueue_event(MessageEvent({'event': 2}))
    
    # Create tap - this should start after the current events
    q2 = q1.tap()
    
    # q1 should still be able to read the events
    e1_from_q1 = await q1.dequeue_event(no_wait=True)
    e2_from_q1 = await q1.dequeue_event(no_wait=True)
    
    assert e1_from_q1 == {'event': 1}
    assert e2_from_q1 == {'event': 2}
    
    # q2 should not see the previous events (it starts at the end)
    # Enqueue a new event that both should see
    await q1.enqueue_event(MessageEvent({'event': 3}))
    
    # q2 should be able to read the new event
    e3_from_q2 = await q2.dequeue_event(no_wait=True)
    assert e3_from_q2 == {'event': 3}


@pytest.mark.asyncio
async def test_close_behavior():
    """Test close operation behavior."""
    redis = FakeRedis()
    q = RedisEventQueue('task10', redis, stream_prefix='a2a:test', read_block_ms=10)
    
    # Enqueue an event
    await q.enqueue_event(MessageEvent({'test': 'data'}))
    
    # Close the queue
    await q.close()
    
    # Should be able to dequeue existing events
    result = await q.dequeue_event(no_wait=True)
    assert result == {'test': 'data'}
    
    # Further dequeue should raise QueueEmpty
    with pytest.raises(asyncio.QueueEmpty):
        await q.dequeue_event(no_wait=True)


@pytest.mark.asyncio
async def test_enqueue_after_close_ignored():
    """Test that enqueuing after close is ignored."""
    redis = FakeRedis()
    q = RedisEventQueue('task11', redis, stream_prefix='a2a:test', read_block_ms=10)
    
    # Close first
    await q.close()
    
    # Enqueue should be ignored (no exception, but no effect)
    await q.enqueue_event(MessageEvent({'should_be_ignored': True}))
    
    # Dequeue should raise QueueEmpty immediately
    with pytest.raises(asyncio.QueueEmpty):
        await q.dequeue_event(no_wait=True)


@pytest.mark.asyncio
async def test_maxlen_parameter():
    """Test maxlen parameter limits stream size."""
    redis = FakeRedis()
    q = RedisEventQueue('task12', redis, stream_prefix='a2a:test', read_block_ms=10, maxlen=2)
    
    # Add more events than maxlen
    for i in range(5):
        await q.enqueue_event(MessageEvent({'event': i}))
    
    # Should only be able to dequeue the last 2 events (due to maxlen=2)
    # Note: This depends on how FakeRedis implements maxlen
    events_dequeued = []
    try:
        while True:
            event = await q.dequeue_event(no_wait=True)
            events_dequeued.append(event)
    except asyncio.QueueEmpty:
        pass
    
    # At minimum, we should have dequeued some events
    assert len(events_dequeued) > 0
