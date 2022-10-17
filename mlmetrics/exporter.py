from prometheus_client import Gauge, Counter, Histogram, Summary, generate_latest, CollectorRegistry
from fastapi import FastAPI

app = FastAPI()
registry = CollectorRegistry()


@app.get('/metrics')
def expose_metrics():
    return generate_latest(registry)


def prepare_counter(name, description, tags, value):
    c = Counter(name, description, tags, registry=registry)
    c.set(value)


def prepare_gauges(name, description, tags, value):
    g = Gauge(name, description, tags, registry=registry)
    g.set(value)


def prepare_histograms(name, description, tags, value):
    h = Histogram(name, description, tags, registry=registry)
    h.observe(value)


def prepare_summaries(name, description, tags, value):
    g = Summary(name, description, tags, registry=registry)
    g.observe(value)
