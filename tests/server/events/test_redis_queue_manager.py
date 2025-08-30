import asyncio

import pytest


@pytest.mark.asyncio
async def test_create_or_tap_creates_queue(monkeypatch):
    created = {}

    class FakeRedisEventQueue:
        def __init__(self, task_id, redis_client, stream_prefix=None):
            self.task_id = task_id

        def tap(self):
            return FakeRedisEventQueue(self.task_id, None)
            
        async def close(self):
            pass  # No-op for test

    # Monkeypatch import used in RedisQueueManager.create_or_tap by inserting
    # a proper module object with attribute `RedisEventQueue`.
    import types, sys
    fake_mod = types.ModuleType('a2a.server.events.redis_event_queue')
    fake_mod.RedisEventQueue = FakeRedisEventQueue
    monkeypatch.setitem(sys.modules, 'a2a.server.events.redis_event_queue', fake_mod)

    from a2a.server.events.redis_queue_manager import RedisQueueManager

    manager = RedisQueueManager(redis_client=None)
    q = await manager.create_or_tap('t1')
    assert hasattr(q, 'task_id')


@pytest.mark.asyncio
async def test_add_not_supported():
    """Test that add() is not supported in RedisQueueManager (distributed setup)."""
    from a2a.server.events.redis_queue_manager import RedisQueueManager

    manager = RedisQueueManager(redis_client=None)
    
    class DummyQueue:
        def __init__(self, id):
            self.id = id

        async def close(self):
            return None

    q = DummyQueue('t2')
    
    # add() should raise NotImplementedError in distributed Redis setup
    with pytest.raises(NotImplementedError, match='add\\(\\) is not supported in distributed Redis setup'):
        await manager.add('t2', q)


@pytest.mark.asyncio
async def test_create_or_tap_with_different_task_ids(monkeypatch):
    """Test create_or_tap with different task IDs creates separate queues."""
    created_queues = {}

    class FakeRedisEventQueue:
        def __init__(self, task_id, redis_client, stream_prefix=None):
            self.task_id = task_id
            created_queues[task_id] = self

        def tap(self):
            return FakeRedisEventQueue(self.task_id, None)

        async def close(self):
            pass

    # Monkeypatch
    import types, sys
    fake_mod = types.ModuleType('a2a.server.events.redis_event_queue')
    fake_mod.RedisEventQueue = FakeRedisEventQueue
    monkeypatch.setitem(sys.modules, 'a2a.server.events.redis_event_queue', fake_mod)

    from a2a.server.events.redis_queue_manager import RedisQueueManager

    manager = RedisQueueManager(redis_client=None)
    
    # Create queues for different tasks
    q1 = await manager.create_or_tap('task1')
    q2 = await manager.create_or_tap('task2')
    q3 = await manager.create_or_tap('task1')  # Same task creates new instance (no caching)
    
    assert q1.task_id == 'task1'
    assert q2.task_id == 'task2'
    assert q3.task_id == 'task1'
    assert q1 is not q3  # Different instances for same task (no caching in Redis)


@pytest.mark.asyncio
async def test_close_operation(monkeypatch):
    """Test close operation on Redis queue manager."""
    closed_queues = []

    class FakeRedisEventQueue:
        def __init__(self, task_id, redis_client, stream_prefix=None):
            self.task_id = task_id

        def tap(self):
            return FakeRedisEventQueue(self.task_id, None)
            
        async def close(self):
            closed_queues.append(self.task_id)

    # Monkeypatch
    import types, sys
    fake_mod = types.ModuleType('a2a.server.events.redis_event_queue')
    fake_mod.RedisEventQueue = FakeRedisEventQueue
    monkeypatch.setitem(sys.modules, 'a2a.server.events.redis_event_queue', fake_mod)
    
    # Also patch the RedisEventQueue reference in redis_queue_manager
    import a2a.server.events.redis_queue_manager as rqm
    monkeypatch.setattr(rqm, 'RedisEventQueue', FakeRedisEventQueue)

    from a2a.server.events.redis_queue_manager import RedisQueueManager

    manager = RedisQueueManager(redis_client=None)
    
    # Create and close a queue
    q1 = await manager.create_or_tap('task1')
    await manager.close('task1')
    
    assert 'task1' in closed_queues


@pytest.mark.asyncio
async def test_close_nonexistent_task(monkeypatch):
    """Test closing a nonexistent task."""
    class FakeRedisEventQueue:
        def __init__(self, task_id, redis_client, stream_prefix=None):
            self.task_id = task_id

        def tap(self):
            return FakeRedisEventQueue(self.task_id, None)
            
        async def close(self):
            pass  # No-op for test

    # Monkeypatch
    import types, sys
    fake_mod = types.ModuleType('a2a.server.events.redis_event_queue')
    fake_mod.RedisEventQueue = FakeRedisEventQueue
    monkeypatch.setitem(sys.modules, 'a2a.server.events.redis_event_queue', fake_mod)

    from a2a.server.events.redis_queue_manager import RedisQueueManager

    manager = RedisQueueManager(redis_client=None)
    
    # Should not raise error when closing nonexistent task
    await manager.close('nonexistent_task')


@pytest.mark.asyncio
async def test_get_operation(monkeypatch):
    """Test get operation on Redis queue manager."""
    class FakeRedisEventQueue:
        def __init__(self, task_id, redis_client, stream_prefix=None):
            self.task_id = task_id

        def tap(self):
            return FakeRedisEventQueue(self.task_id, None)

    # Monkeypatch
    import types, sys
    fake_mod = types.ModuleType('a2a.server.events.redis_event_queue')
    fake_mod.RedisEventQueue = FakeRedisEventQueue
    monkeypatch.setitem(sys.modules, 'a2a.server.events.redis_event_queue', fake_mod)
    
    # Also patch the RedisEventQueue reference in redis_queue_manager
    import a2a.server.events.redis_queue_manager as rqm
    monkeypatch.setattr(rqm, 'RedisEventQueue', FakeRedisEventQueue)

    from a2a.server.events.redis_queue_manager import RedisQueueManager

    manager = RedisQueueManager(redis_client=None)
    
    # Get operation should raise NotImplementedError in distributed Redis setup
    with pytest.raises(NotImplementedError, match='get\\(\\) is not supported in distributed Redis setup'):
        await manager.get('nonexistent')
    
    # Get existing task should also raise NotImplementedError in distributed setup
    q1 = await manager.create_or_tap('task1')
    with pytest.raises(NotImplementedError, match='get\\(\\) is not supported in distributed Redis setup'):
        await manager.get('task1')


@pytest.mark.asyncio
async def test_tap_operation(monkeypatch):
    """Test tap operation creates new queue instance with same redis_client."""
    class FakeRedisEventQueue:
        def __init__(self, task_id, redis_client, stream_prefix=None):
            self.task_id = task_id
            self.redis_client = redis_client

        def tap(self):
            # Return a new queue with the same redis_client (matching actual behavior)
            return FakeRedisEventQueue(self.task_id, self.redis_client)

    # Monkeypatch
    import types, sys
    fake_mod = types.ModuleType('a2a.server.events.redis_event_queue')
    fake_mod.RedisEventQueue = FakeRedisEventQueue
    monkeypatch.setitem(sys.modules, 'a2a.server.events.redis_event_queue', fake_mod)
    
    # Also patch the RedisEventQueue reference in redis_queue_manager
    import a2a.server.events.redis_queue_manager as rqm
    monkeypatch.setattr(rqm, 'RedisEventQueue', FakeRedisEventQueue)

    from a2a.server.events.redis_queue_manager import RedisQueueManager

    manager = RedisQueueManager(redis_client='fake_redis')
    
    # Create tap
    tapped_queue = await manager.tap('task1')
    
    assert tapped_queue.task_id == 'task1'
    assert tapped_queue.redis_client == 'fake_redis'  # Tap should have the same redis_client
