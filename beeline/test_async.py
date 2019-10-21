import asyncio
import datetime
import unittest
from mock import Mock

import beeline
import beeline.aiotrace
import beeline.trace


def async_test(fn):
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))  # pylint: disable=no-member

    return wrapper


class TestAsynchronousTracer(unittest.TestCase):
    def test_synchronous_tracer_should_be_used_by_default(self):
        _beeline = beeline.Beeline()
        self.assertIsInstance(
            _beeline.tracer_impl, beeline.trace.SynchronousTracer
        )

    @async_test
    async def test_asyncio_tracer_should_be_used_in_async_code(self):
        _beeline = beeline.Beeline()
        self.assertIsInstance(
            _beeline.tracer_impl, beeline.aiotrace.AsyncioTracer
        )

    @async_test
    async def test_tracing_in_new_tasks_should_work(self):
        _beeline = beeline.Beeline()
        _beeline.tracer_impl._run_hooks_and_send = Mock()

        trace = _beeline.tracer_impl.start_trace()
        _beeline.tracer_impl.finish_trace(trace)

        self.assertTrue(_beeline.tracer_impl._run_hooks_and_send.called)

    @async_test
    async def test_new_tasks_should_trace_in_parallel(self):
        spans = []

        def add_span(span):
            spans.append(span)

        _beeline = beeline.Beeline()
        _beeline.tracer_impl._run_hooks_and_send = add_span

        trace = _beeline.tracer_impl.start_trace(context={"id": "root"})

        async def task0():
            span0 = _beeline.tracer_impl.start_span(context={"id": "task0"})
            await asyncio.sleep(0.2)
            _beeline.tracer_impl.finish_span(span0)

        async def task1():
            await asyncio.sleep(0.1)
            span1 = _beeline.tracer_impl.start_span(context={"id": "task1"})
            await asyncio.sleep(0.2)
            _beeline.tracer_impl.finish_span(span1)

        await asyncio.gather(task0(), task1())

        _beeline.tracer_impl.finish_trace(trace)

        def event_data(span):
            name = span.event.fields()["id"]
            start = span.event.start_time
            duration = datetime.timedelta(
                milliseconds=span.event.fields()["duration_ms"]
            )
            end = start + duration
            return {
                "id": name,
                "start": start,
                "end": end,
                "span": span,
            }

        event_data = [event_data(s) for s in spans]
        task0_event, task1_event, root_event = event_data

        # Check that the spans finished in the expected order, with
        # the root span last.
        self.assertEqual(task0_event["id"], "task0")
        self.assertEqual(task1_event["id"], "task1")
        self.assertLess(task0_event["end"], task1_event["end"])
        self.assertEqual(root_event["id"], "root")
        self.assertLess(task1_event["end"], root_event["end"])

        # Check that the root span was started before the others.
        self.assertLess(root_event["start"], task0_event["start"])
        self.assertLess(root_event["start"], task1_event["start"])

        # Check that the task0 started before task1
        self.assertLess(task0_event["start"], task1_event["start"])

        # Check that the task1 span started during the task0 span
        self.assertLess(task1_event["start"], task0_event["end"])

        # Check that the task spans are both children of the root span
        self.assertEqual(root_event["span"].id, task0_event["span"].parent_id)
        self.assertEqual(root_event["span"].id, task1_event["span"].parent_id)
