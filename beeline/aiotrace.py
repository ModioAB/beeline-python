"""Asynchronous tracer implementation.

Requires Python 3.7, because it uses the contextvars module.

"""
import asyncio
import asyncio.tasks
import contextvars  # pylint: disable=import-error

from beeline.trace import Tracer

current_trace_var = contextvars.ContextVar("current_trace")


def create_task_factory(parent_factory):
    """Create a task factory that copies the current tracing context."""
    def task_factory_impl(loop, coro):
        async def coro_wrapper():  # pylint: disable=syntax-error
            trace = current_trace_var.get(None)
            if trace:
                token = current_trace_var.set(trace.copy())
            else:
                token = None

            try:
                return await coro
            finally:
                if token is not None:
                    current_trace_var.reset(token)

        if parent_factory is None:
            task = asyncio.tasks.Task(coro_wrapper(), loop=loop)
        else:
            task = parent_factory(coro_wrapper())

        return task

    task_factory_impl.__trace_task_factory__ = True
    return task_factory_impl


class AsyncioTracer(Tracer):
    def __init__(self, client):
        super().__init__(client)
        loop = asyncio.get_running_loop()  # pylint: disable=no-member

        task_factory = loop.get_task_factory()
        if task_factory is None or not task_factory.__trace_task_factory__:
            new_task_factory = create_task_factory(task_factory)
            loop.set_task_factory(new_task_factory)

    @property
    def _trace(self):
        return current_trace_var.get(None)

    @_trace.setter
    def _trace(self, new_trace):
        current_trace_var.set(new_trace)
