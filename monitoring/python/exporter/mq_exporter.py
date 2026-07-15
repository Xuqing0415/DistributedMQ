import os
import time
import json
import http.client
from http.server import HTTPServer, BaseHTTPRequestHandler

METRICS = {
    'mq_broker_count': 0,
    'mq_topic_count': 0,
    'mq_message_produced_total': 0,
    'mq_message_consumed_total': 0,
    'mq_disk_usage_bytes': 0,
    'mq_consumer_lag': 0,
}


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/metrics':
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(generate_prometheus_metrics().encode())
        elif self.path == '/health':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'status': 'ok'}).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass


def generate_prometheus_metrics():
    lines = []
    lines.append('# HELP mq_broker_count Number of active brokers')
    lines.append('# TYPE mq_broker_count gauge')
    lines.append(f'mq_broker_count {METRICS["mq_broker_count"]}')

    lines.append('# HELP mq_topic_count Number of topics')
    lines.append('# TYPE mq_topic_count gauge')
    lines.append(f'mq_topic_count {METRICS["mq_topic_count"]}')

    lines.append('# HELP mq_message_produced_total Total messages produced')
    lines.append('# TYPE mq_message_produced_total counter')
    lines.append(f'mq_message_produced_total {METRICS["mq_message_produced_total"]}')

    lines.append('# HELP mq_message_consumed_total Total messages consumed')
    lines.append('# TYPE mq_message_consumed_total counter')
    lines.append(f'mq_message_consumed_total {METRICS["mq_message_consumed_total"]}')

    lines.append('# HELP mq_disk_usage_bytes Disk usage in bytes')
    lines.append('# TYPE mq_disk_usage_bytes gauge')
    lines.append(f'mq_disk_usage_bytes {METRICS["mq_disk_usage_bytes"]}')

    lines.append('# HELP mq_consumer_lag Consumer lag in messages')
    lines.append('# TYPE mq_consumer_lag gauge')
    lines.append(f'mq_consumer_lag {METRICS["mq_consumer_lag"]}')

    return '\n'.join(lines)


def fetch_nameserver_metrics(nameserver_url):
    try:
        conn = http.client.HTTPConnection(nameserver_url)
        conn.request('GET', '/broker/list')
        resp = conn.getresponse()
        if resp.status == 200:
            data = json.loads(resp.read())
            METRICS['mq_broker_count'] = len(data.get('brokers', []))
        conn.close()

        conn = http.client.HTTPConnection(nameserver_url)
        conn.request('GET', '/topic/list')
        resp = conn.getresponse()
        if resp.status == 200:
            data = json.loads(resp.read())
            METRICS['mq_topic_count'] = len(data.get('topics', []))
        conn.close()
    except Exception:
        pass


def fetch_disk_usage(data_dir):
    try:
        if os.path.exists(data_dir):
            total = 0
            for dirpath, dirnames, filenames in os.walk(data_dir):
                for filename in filenames:
                    filepath = os.path.join(dirpath, filename)
                    total += os.path.getsize(filepath)
            METRICS['mq_disk_usage_bytes'] = total
    except Exception:
        pass


def update_metrics_periodically(nameserver_url, data_dir, interval=10):
    while True:
        fetch_nameserver_metrics(nameserver_url)
        fetch_disk_usage(data_dir)
        time.sleep(interval)


def main():
    import argparse

    parser = argparse.ArgumentParser(description='MQ Exporter for Prometheus')
    parser.add_argument('--port', type=int, default=9100, help='Exporter port')
    parser.add_argument('--nameserver', type=str, default='localhost:9090', help='NameServer address')
    parser.add_argument('--data-dir', type=str, default='./data', help='MQ data directory')
    args = parser.parse_args()

    import threading
    t = threading.Thread(target=update_metrics_periodically,
                         args=(args.nameserver, args.data_dir),
                         daemon=True)
    t.start()

    server = HTTPServer(('', args.port), MetricsHandler)
    print(f'Starting MQ Exporter on port {args.port}')
    server.serve_forever()


if __name__ == '__main__':
    main()
