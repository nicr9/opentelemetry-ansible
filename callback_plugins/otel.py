from os.path import basename
from os.path import join as path_join

from ansible.plugins.callback import CallbackBase

from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
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
        self.traces = {}

    def v2_playbook_on_start(self, playbook):
        self.traces['playbook'] = self.tracer.start_span(
            path_join(basename(playbook._basedir), playbook._file_name),
            context=self.context
        )

    def v2_playbook_on_stats(self, stats):
        self.traces['playbook'].end()

        span_context = self.traces['playbook'].get_span_context()
        trace_id = trace.format_trace_id(span_context.trace_id)
        self._display.banner(f"TRACE ID [{trace_id}]")
