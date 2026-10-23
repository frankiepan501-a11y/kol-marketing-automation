"""External, read-only migration entry/TLS checks. Never restarts services."""
import hashlib
import datetime
import json
import os
from pathlib import Path
import socket
import ssl
import time
import urllib.error
import urllib.request
import uuid

from feishu_title_audit import format_title

HOSTS = ('coolify.ops.funlab.net', 'n8n.ops.funlab.net',
         'alexa-insight-cloud-center.ops.funlab.net')
APP = 'cli_a9f6ae86fce8dbd8'
RECIPIENT_SHA256 = 'f89363e19dc94cbf0efca22819046503fff16809b75b8d4e185ad1b3f5d12be4'
STATUS_URL = 'https://coolify.ops.funlab.net/migration-health/status.json'
ISSUES = {'container_state', 'disk_usage', 'inode_usage', 'memory_available', 'collector_error'}


def validate_status(data, now):
    if not isinstance(data, dict) or set(data) != {'checked_at', 'ok', 'issues'}:
        raise ValueError('schema')
    if type(data['ok']) is not bool or not isinstance(data['issues'], list):
        raise ValueError('types')
    issues = data['issues']
    if any(not isinstance(i, str) or i not in ISSUES for i in issues) or len(set(issues)) != len(issues):
        raise ValueError('issues')
    if data['ok'] != (not issues):
        raise ValueError('inconsistent')
    stamp = datetime.datetime.fromisoformat(data['checked_at'].replace('Z', '+00:00'))
    if stamp.utcoffset() != datetime.timedelta(0):
        raise ValueError('timezone')
    age = now - stamp.timestamp()
    if age < -60 or age > 600:
        return 'STATUS_STALE' if age > 600 else 'STATUS_FUTURE'
    return 'INTERNAL:' + ','.join(sorted(issues)) if issues else None


def probe_internal():
    result = {'host': STATUS_URL}
    try:
        with urllib.request.build_opener(NoRedirect).open(STATUS_URL, timeout=15) as response:
            raw = response.read(4097)
            if response.status != 200 or len(raw) > 4096:
                raise ValueError('response')
        failure = validate_status(json.loads(raw), time.time())
        if failure:
            result['failure'] = failure
    except Exception as exc:
        result['failure'] = 'STATUS_' + type(exc).__name__
    return result


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def probe(host):
    result = {'host': host}
    try:
        with socket.create_connection((host, 443), timeout=15) as raw:
            with ssl.create_default_context().wrap_socket(raw, server_hostname=host) as conn:
                expiry = ssl.cert_time_to_seconds(conn.getpeercert()['notAfter'])
        days = (expiry - time.time()) / 86400
        result['days_remaining'] = round(days, 2)
        if days <= 7:
            result['certificate'] = 'critical'
        elif days <= 21:
            result['certificate'] = 'warning'
        try:
            with urllib.request.build_opener(NoRedirect).open('https://' + host + '/', timeout=15) as response:
                status = response.status
        except urllib.error.HTTPError as exc:
            status = exc.code
            exc.close()
        result['http_status'] = status
        if status not in (200, 301, 302, 303, 307, 308):
            result['failure'] = 'HTTP_' + str(status)
    except Exception as exc:
        # Do not log response bodies, headers, tokens or arbitrary exception text.
        result['failure'] = type(exc).__name__
    return result


def transition(previous, probes, now):
    """Two consecutive transport failures; expiry warnings immediate; 24h repeat cap."""
    state, due = {}, []
    for result in probes:
        host = result['host']
        prior = previous.get(host, {})
        # A stale cached failure is not a consecutive observation.
        fresh = 0 <= now - prior.get('checked', 0) <= 3600
        count = (prior.get('count', 0) if fresh else 0) + 1 if result.get('failure') else 0
        reasons = []
        if count >= 2:
            reasons.append(('主机巡检异常:' if host == STATUS_URL else '入口连接失败:') + result['failure'])
        if result.get('certificate'):
            reasons.append('证书:' + result['certificate'])
        fingerprint = '|'.join(reasons)
        item = dict(prior, count=count, checked=now)
        if not result.get('failure') and not result.get('certificate'):
            item.pop('sent_fingerprint', None)
            item.pop('sent_at', None)
        state[host] = item
        if fingerprint and (fingerprint != prior.get('sent_fingerprint') or now-prior.get('sent_at', 0) >= 86400):
            due.append((host, fingerprint, result))
    return state, due


def api(path, payload=None, token=None):
    headers = {'Content-Type': 'application/json'}
    if token:
        headers['Authorization'] = 'Bearer ' + token
    data = None if payload is None else json.dumps(payload).encode()
    request = urllib.request.Request('https://open.feishu.cn/open-apis/' + path, data=data, headers=headers)
    with urllib.request.urlopen(request, timeout=20) as response:
        result = json.load(response)
    if result.get('code') != 0:
        raise RuntimeError('Feishu API code ' + str(result.get('code')))
    return result


def notification_token():
    if os.environ.get('FEISHU_NOTIFY_APP_ID', '').strip() != APP:
        raise RuntimeError('Unexpected notification app; no message sent')
    recipient = os.environ.get('FEISHU_NOTIFY_OPEN_ID', '').strip()
    if hashlib.sha256(recipient.encode()).hexdigest() != RECIPIENT_SHA256:
        raise RuntimeError('Unexpected recipient; no message sent')
    secret = os.environ.get('FEISHU_NOTIFY_APP_SECRET', '').strip()
    if not secret:
        raise RuntimeError('Notification secret missing')
    return api('auth/v3/tenant_access_token/internal', {'app_id': APP, 'app_secret': secret})['tenant_access_token']


def verify_message(mid, digest, token):
    readback = api('im/v1/messages/' + mid, token=token)
    items = readback['data']['items']
    text = json.loads(items[0]['body']['content']).get('text', '') if items else ''
    if hashlib.sha256(text.encode()).hexdigest() != digest:
        raise RuntimeError('Notification readback did not match')


def notify(body, incident, test=False, on_sent=None):
    token = notification_token()
    recipient = os.environ['FEISHU_NOTIFY_OPEN_ID'].strip()
    title = format_title('AUDIT', 'P3' if test else 'P1',
                         '腾讯云维护告警测试' if test else '腾讯云维护巡检异常', '迁移验收')
    text = title + '\n' + body
    sent = api('im/v1/messages?receive_id_type=open_id', {
        'receive_id': recipient, 'msg_type': 'text',
        'content': json.dumps({'text': text}, ensure_ascii=False),
        'uuid': str(uuid.uuid5(uuid.NAMESPACE_URL, incident)),
    }, token)
    mid = sent['data']['message_id']
    digest = hashlib.sha256(text.encode()).hexdigest()
    if on_sent:
        on_sent(mid, digest)
    verify_message(mid, digest, token)
    return mid


def main():
    folder = Path('.coolify-watchdog-state')
    folder.mkdir(exist_ok=True)
    path = folder / 'state.json'
    test = os.environ.get('WATCHDOG_TEST') == 'true'
    dry = os.environ.get('WATCHDOG_DRY_RUN', 'true') == 'true'
    now = time.time()
    previous = json.loads(path.read_text()) if path.exists() else {}
    if not dry and not test:
        for item in previous.values():
            if item.get('pending_readback'):
                pending = item['pending_readback']
                verify_message(pending['message_id'], pending['digest'], notification_token())
                item.pop('pending_readback')
                path.write_text(json.dumps(previous), encoding='utf-8')
    probes = [probe(host) for host in HOSTS] + [probe_internal()]
    state, due = transition(previous, probes, now)
    sent = []
    if test:
        # Synthetic failed observations exercise the same decision and send path.
        simulated = [{'host': HOSTS[0], 'failure': 'SIMULATED_TEST'}]
        first, _ = transition({}, simulated, now-60)
        _, due = transition(first, simulated, now)
    for host, fingerprint, result in due:
        body = ('这是模拟故障验收，未停止真实服务。\n' if test else '') + host + '\n' + fingerprint
        if 'days_remaining' in result:
            body += '\n证书剩余天数：' + str(result['days_remaining'])
        body += ('\n下一步：核腾讯控制台主机、内部巡检定时器及容器/磁盘状态；不要重复启用业务调度。'
                 if host == STATUS_URL else '\n下一步：核腾讯控制台主机状态、Coolify代理和DNS；不要重复启用业务调度。')
        incident = str(os.environ.get('GITHUB_RUN_ID', int(now))) + host + fingerprint
        if not dry:
            def record_sent(mid, digest):
                if not test:
                    state[host].update(sent_at=now, sent_fingerprint=fingerprint,
                                       pending_readback={'message_id': mid, 'digest': digest})
                    path.write_text(json.dumps(state), encoding='utf-8')
            mid = notify(body, incident, test, record_sent)
            sent.append({'host': host, 'message_id': mid, 'readback_verified': True})
            if not test:
                state[host].update(sent_at=now, sent_fingerprint=fingerprint)
                state[host].pop('pending_readback', None)
                path.write_text(json.dumps(state), encoding='utf-8')
    if not dry and not test:
        path.write_text(json.dumps(state), encoding='utf-8')
    summary = {'time': now, 'probes': probes, 'test': test, 'dry_run': dry,
               'alerts_due': len(due), 'sent': sent,
               'scope': 'HTTPS/TLS and sanitized host status; not business validation'}
    (folder / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(summary, ensure_ascii=True))


if __name__ == '__main__':
    main()
