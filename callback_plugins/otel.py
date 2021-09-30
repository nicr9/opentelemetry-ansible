from os.path import basename
from os.path import join as path_join

from ansible.plugins.callback import CallbackBase

from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.trace.status import Status, StatusCode

from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

DOCUMENTATION = '''
callback: otel
callback_type: aggregate
requirements:
    - enable in configuration
short_description: Jaeger/OpenTelemetry instrumentation
version_added: "0.1"
description:
    - Instruments playbooks with traces/spans and sends them to Jaeger
'''


def set_play_attrs(span, play):
    span.set_attribute('play.hosts', str(play.hosts))

    environment = play.environment or {}
    for env, var in environment.items():
        span.set_attribute(f'environment.{env}', var)

    for key, val in play.vars.items():
        span.set_attribute(f'vars.{key}', str(val))

    return span


def set_task_attrs(span, task):
    span.set_attribute('task.action', task.action)

    for key, val in task.args.items():
        span.set_attribute(f'task.args.{key}', str(val))

    environment = {k: v for d in task.environment for k, v in d.items()}
    for env, var in environment.items():
        span.set_attribute(f'environment.{env}', var)

    return span


def set_runner_attrs(span, host, task):
    return span


class CallbackModule(CallbackBase):
    """
    This callback module instruments playbooks, plays, tasks, etc
    """
    CALLBACK_VERSION = 2.0
    CALLBACK_TYPE = 'aggregate'
    CALLBACK_NAME = 'otel'

    def __init__(self):
        super(CallbackModule, self).__init__()

        provider = TracerProvider(
            resource=Resource.create({SERVICE_NAME: 'ansible-playbook'}),
        )
        exporter = JaegerExporter(
            agent_host_name="localhost",
            agent_port=6831,
        )
        provider.add_span_processor(
            BatchSpanProcessor(exporter)
        )
        trace.set_tracer_provider(provider)

        self.context = trace.set_span_in_context(None)

        self.tracer = trace.get_tracer(__name__)
        self.active_spans = {}

    def v2_playbook_on_start(self, playbook):
        self.active_spans['playbook'] = self.tracer.start_span(
            path_join(basename(playbook._basedir), playbook._file_name),
            context=self.context
        )

    def v2_playbook_on_play_start(self, play):
        if 'runner' in self.active_spans:
            self.active_spans['runner'].end()
        if 'task' in self.active_spans:
            self.active_spans['task'].end()
        if 'play' in self.active_spans:
            self.active_spans['play'].end()

        span = self.tracer.start_span(
            f'PLAY: {play}',
            context=trace.set_span_in_context(self.active_spans['playbook']),
        )
        span = set_play_attrs(span, play)
        self.active_spans['play'] = span

    def v2_playbook_on_task_start(self, task, is_conditional):
        if 'runner' in self.active_spans:
            self.active_spans['runner'].end()
        if 'task' in self.active_spans:
            self.active_spans['task'].end()

        span = self.tracer.start_span(
            str(task),
            context=trace.set_span_in_context(self.active_spans['play']),
        )
        span = set_task_attrs(span, task)
        self.active_spans['task'] = span

    def v2_runner_on_start(self, host, task):
        if 'runner' in self.active_spans:
            self.active_spans['runner'].end()

        span = self.tracer.start_span(
            f'{str(task)} [{host}]',
            context=trace.set_span_in_context(self.active_spans['task']),
        )
        span = set_runner_attrs(span, host, task)
        self.active_spans['runner'] = span

    def v2_runner_on_failed(self, result, ignore_errors=False):
        attrs = {}
        attrs["exception.message"] = result._result['msg']

        span = self.active_spans['runner']
        span.add_event(name="exception", attributes=attrs)
        span.set_status(Status(status_code=StatusCode.ERROR))

        if 'runner' in self.active_spans:
            self.active_spans['runner'].end()

    def v2_playbook_on_stats(self, stats):
        if 'runner' in self.active_spans:
            self.active_spans['runner'].end()
        if 'task' in self.active_spans:
            self.active_spans['task'].end()
        if 'play' in self.active_spans:
            self.active_spans['play'].end()
        if 'playbook' in self.active_spans:
            self.active_spans['playbook'].end()

        span_context = self.active_spans['playbook'].get_span_context()
        trace_id = trace.format_trace_id(span_context.trace_id)
        self._display.banner(f"TRACE ID [{trace_id}]")
