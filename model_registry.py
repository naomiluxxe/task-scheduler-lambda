"""
Model registry — which model does each job, decided in one place.

Code never names a model. It names a ROLE ("self_reflection", "deliberation")
and asks the registry which model plays it:

    import model_registry
    choice = model_registry.resolve('self_reflection')
    choice['model']      # 'us.anthropic.claude-sonnet-4-6' today

Three layers, each changeable without touching code:

  RUNGS      an ordered ladder, cheapest first:  light → standard → strong → frontier
             (Haiku / Sonnet / Opus / Fable today). Add a rung at either end and
             nothing else has to change.
  PLATFORMS  what each rung MEANS on each platform, today. A new model generation
             is one edit here: the 'strong' rung on 'anthropic' becomes Opus 6,
             and every role on that rung moves with it. Platforms use
             llm_provider's provider names — 'claude' is Claude ON BEDROCK,
             'anthropic' is the first-party API.
  ROLES      each job's rung (+ effort, output limit, platform pin, and whether
             the budget dial may move it).

One global dial, `budget_shift`: -1 moves every shiftable role one rung cheaper,
+1 one rung richer. Roles with `shift: False` (latency-tuned voice) ignore it;
`floor` / `ceiling` bound how far a role can be moved.

If a platform has no model on a role's rung (Bedrock serves no Fable here), the
role steps DOWN to the nearest rung that platform has, and says so.

Source of truth: agent-config item `model-registry`. BUILTIN below is the same
document, used when that item cannot be read — so a DynamoDB outage or a missing
IAM grant degrades to yesterday's choices, never to a crash. Per-key merge: an
item that predates a role added here still resolves that role from BUILTIN.

Canonical copy: orchestrator-lambda/model_registry.py. Other repos carry a copy;
orchestrator-lambda/scripts/sync_shared.sh copies it out and checks every role
the code asks for exists. Created 2026-09-13.
"""

import copy
import os
import re
import time

REGISTRY_TABLE = os.environ.get('AGENT_CONFIG_TABLE_NAME', 'agent-config')
REGISTRY_KEY = 'model-registry'
CACHE_TTL_S = 300

BUILTIN = {
    'version': 1,
    'rungs': ['light', 'standard', 'strong', 'frontier'],
    'budget_shift': 0,

    'platforms': {
        # Claude on Bedrock. Opus 4.7/4.8 and all Claude 5 are refused on this
        # account (AWS-side, AWS_SUPPORT_CASE.md) — so no 'frontier' here.
        'claude': {
            'light': 'us.anthropic.claude-haiku-4-5-20251001-v1:0',
            'standard': 'us.anthropic.claude-sonnet-4-6',
            'strong': 'us.anthropic.claude-opus-4-6-v1',
        },
        # First-party Claude API (void-mother/anthropic-api-key).
        'anthropic': {
            'light': 'claude-haiku-4-5',
            'standard': 'claude-sonnet-5',
            'strong': 'claude-opus-5',
            'frontier': 'claude-fable-5-1',
        },
        'gemini': {
            'light': 'gemini-2.5-flash',
            'standard': 'gemini-2.5-flash',
            'strong': 'gemini-2.5-pro',
        },
        'gemini3': {
            'light': 'gemini-3-flash-preview',
            'standard': 'gemini-3-flash-preview',
            'strong': 'gemini-3.1-pro-preview',
        },
        'nova': {
            'light': 'amazon.nova-micro-v1:0',
            'standard': 'amazon.nova-lite-v1:0',
            'strong': 'amazon.nova-pro-v1:0',
        },
    },

    # Facts per model, keyed by normalized id (see normalize()). Prices are
    # $ per million tokens. cache_read defaults to 10% of input and cache_write
    # to 125% where not stated. `retired` models stay listed so the weekly audit
    # can name them if anything still calls one.
    'models': {
        'claude-fable-5-1': {'in': 10.0, 'out': 50.0, 'cache_read': 0.25,
                             'adaptive_thinking': True, 'effort': True, 'refusal_fallbacks': True},
        'claude-opus-5': {'in': 5.0, 'out': 25.0,
                          'adaptive_thinking': True, 'effort': True, 'refusal_fallbacks': True},
        'claude-sonnet-5': {'in': 2.0, 'out': 10.0, 'adaptive_thinking': True, 'effort': True},
        'claude-opus-4-8': {'in': 5.0, 'out': 25.0, 'adaptive_thinking': True, 'effort': True},
        'claude-opus-4-6': {'in': 5.0, 'out': 25.0, 'adaptive_thinking': True, 'effort': True},
        'claude-sonnet-4-6': {'in': 3.0, 'out': 15.0, 'adaptive_thinking': True, 'effort': True},
        'claude-haiku-4-5': {'in': 1.0, 'out': 5.0},
        'claude-opus-4-5': {'in': 5.0, 'out': 25.0},
        'claude-sonnet-4-5': {'in': 3.0, 'out': 15.0, 'retired': 'content filter cut her replies (2026-09-04)'},
        'claude-opus-4-1': {'in': 15.0, 'out': 75.0, 'retired': 'deprecated by AWS (2026-09-12)'},
        'gemini-2.5-flash': {'in': 0.30, 'out': 2.50},
        'gemini-2.5-pro': {'in': 1.25, 'out': 10.0},
        'gemini-3-flash-preview': {'in': 2.0, 'out': 12.0},
        'gemini-3.1-pro-preview': {'in': 2.0, 'out': 12.0},
        'amazon.nova-pro': {'in': 0.80, 'out': 3.20},
        'amazon.nova-lite': {'in': 0.06, 'out': 0.24},
        'amazon.nova-micro': {'in': 0.035, 'out': 0.14},
        'amazon.nova-canvas': {'kind': 'image'},
        'amazon.nova-sonic': {'kind': 'speech'},
        'amazon.nova-2-sonic': {'kind': 'speech'},
    },

    'roles': {
        # -- her text conversation (orchestrator). No platform pin: follows the
        #    provider chosen by /void-mother-config (global-config llm_config).
        'conversation': {'rung': 'strong', 'platform_rungs': {'anthropic': 'standard'},
                         'note': 'her replies in Discord; the anthropic tier runs Sonnet 5 for speed and cost (2026-07-19)'},
        'game': {'rung': 'strong', 'platform_rungs': {'anthropic': 'standard'},
                 'note': 'greeter / tour drone replies — inherited the conversation tier (task type was never mapped)'},
        'followup': {'rung': 'light', 'note': 'follow-up summaries'},
        'categorize': {'rung': 'light', 'note': 'message categorizer; platform follows categorizer_config'},
        'programming': {'rung': 'light', 'note': 'programming-session evaluation; platform follows programming_config'},
        'programming_script': {'rung': 'standard', 'platform': 'claude', 'note': 'writes programming scripts'},
        'guardrail_check': {'rung': 'light', 'platform': 'claude', 'note': 'did a reply break character? classifier'},
        'guardrail_fallback': {'rung': 'strong', 'platform_rungs': {'claude': 'standard'},
                               'note': 'retries a reply that broke character or was declined: Bedrock Sonnet, then Nova Pro'},
        'voice_actions': {'rung': 'standard', 'platform': 'claude', 'note': "voice-her's hands (tools mode)"},
        'image_analysis': {'rung': 'standard', 'platform': 'claude', 'note': 'reference-image analysis; images stay on Bedrock'},
        'discord_operation': {'rung': 'light', 'platform': 'claude', 'note': 'plans a Discord operation'},
        'social_snapshot': {'rung': 'light', 'platform': 'claude', 'note': 'social snapshot summary'},
        'voice_summary': {'rung': 'light', 'platform': 'claude', 'note': 'condenses a lookup for speech'},
        'scheduled_query': {'rung': 'light', 'platform': 'claude', 'note': 'task-scheduler check-in queries'},
        'transcribe': {'rung': 'light', 'platform': 'gemini', 'note': 'orchestrator audio transcription'},

        # -- memory, reflection, the nightly (memory-summarizer)
        'summarize': {'rung': 'light', 'platform': 'claude', 'note': 'per-drone memory summaries'},
        'dossier_reflection': {'rung': 'light', 'platform': 'claude', 'note': 'nightly per-drone dossier reflection'},
        'self_reflection': {'rung': 'standard', 'platform': 'claude', 'floor': 'standard',
                            'note': 'journal, mood, briefing — a failed night means no briefing'},
        'atelier_note': {'rung': 'standard', 'platform': 'claude', 'note': 'Obsidian notes in her-notes'},
        'outreach_message': {'rung': 'standard', 'platform': 'claude', 'floor': 'standard',
                             'note': 'composes unprompted outreach with the drone’s red limits in view'},
        'room_note': {'rung': 'light', 'platform': 'claude', 'note': 'per-room notes'},
        'glance': {'rung': 'light', 'platform': 'claude', 'note': 'twice-daily DS1 salience glance'},
        'dream_prompt': {'rung': 'light', 'platform': 'claude', 'note': 'the dream image prompt'},
        'dream_interpret': {'rung': 'standard', 'platform': 'claude', 'note': 'she looks at the dream and reads it'},
        'sound_compose': {'rung': 'standard', 'platform': 'claude', 'note': 'composes her standing score'},
        'sound_ear': {'rung': 'light', 'platform': 'gemini', 'note': 'listens to a render (Claude has no audio in)'},

        # -- identity packets and memory selection
        'distill': {'rung': 'light', 'platform': 'claude', 'note': 'persona distiller identity packet'},
        'memory_select': {'rung': 'light', 'platform': 'claude', 'note': 'semantic memory selection'},

        # -- voice (dronebot-voice on the box)
        'voice_reflex': {'rung': 'standard', 'platform': 'claude', 'shift': False,
                         'note': 'her spoken replies — latency-tuned (1.3s TTFT) and filter-tested; the dial never moves it'},
        'voice_recall': {'rung': 'standard', 'platform': 'claude', 'note': 'memories surfacing mid-call'},
        'voice_vision': {'rung': 'light', 'platform': 'claude',
                         'note': 'her sight; pinned to Bedrock so strangers’ faces do not leave the account'},

        # -- deliberation (first-party: needs server-side web search)
        'deliberation': {'rung': 'strong', 'platform': 'anthropic', 'effort': 'medium', 'max_tokens': 32000,
                         'note': 'her regular waking-hours sessions'},
        'deliberation_deep': {'rung': 'frontier', 'platform': 'anthropic', 'effort': 'high', 'max_tokens': 64000,
                              'note': 'the weekly deep session'},

        # -- not called from code: recorded so the audit knows who uses what
        'kb_parser': {'rung': 'standard', 'platform': 'claude', 'shift': False,
                      'managed': 'Bedrock KB 8T0D0A5EW4 / data source JSYIFJB3NE parsing model — change it with '
                                 'bedrock-agent update_data_source, then edit this role to match'},
    },
}

_cache = {'at': 0.0, 'doc': None, 'source': None}
_warned = set()


def _warn_once(key, msg):
    if key not in _warned:
        _warned.add(key)
        print(f"[model_registry] {msg}")


def _plain(obj):
    """DynamoDB Decimals → int/float, recursively."""
    try:
        from decimal import Decimal
    except Exception:  # pragma: no cover
        Decimal = ()
    if isinstance(obj, dict):
        return {k: _plain(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_plain(v) for v in obj]
    if Decimal and isinstance(obj, Decimal):
        return int(obj) if obj == obj.to_integral_value() else float(obj)
    return obj


def validate(doc):
    """Return a list of problems (empty = valid)."""
    problems = []
    rungs = doc.get('rungs') or []
    if not rungs or len(set(rungs)) != len(rungs):
        problems.append('rungs must be a non-empty list without repeats')
    for platform, ladder in (doc.get('platforms') or {}).items():
        for rung in ladder:
            if rung not in rungs:
                problems.append(f'platform {platform}: unknown rung {rung!r}')
    for name, role in (doc.get('roles') or {}).items():
        for key in ('rung', 'floor', 'ceiling'):
            if role.get(key) is not None and role[key] not in rungs:
                problems.append(f'role {name}: {key} {role[key]!r} is not a rung')
        if 'rung' not in role:
            problems.append(f'role {name}: no rung')
        for platform, rung in (role.get('platform_rungs') or {}).items():
            if rung not in rungs:
                problems.append(f'role {name}: platform_rungs[{platform}] {rung!r} is not a rung')
        if role.get('platform') and role['platform'] not in (doc.get('platforms') or {}):
            problems.append(f"role {name}: platform {role['platform']!r} has no ladder")
    try:
        int(doc.get('budget_shift', 0))
    except (TypeError, ValueError):
        problems.append('budget_shift must be an integer')
    return problems


def merge(stored):
    """BUILTIN with the stored document laid over it, one key at a time."""
    doc = copy.deepcopy(BUILTIN)
    stored = _plain(stored or {})
    for key in ('version', 'rungs', 'budget_shift'):
        if key in stored:
            doc[key] = stored[key]
    for table in ('platforms', 'models', 'roles'):
        for name, value in (stored.get(table) or {}).items():
            doc[table][name] = value
    for key in ('updated_at', 'updated_by', 'change_log'):
        if key in stored:
            doc[key] = stored[key]
    return doc


def _load():
    try:
        import boto3
        region = os.environ.get('AWS_REGION') or os.environ.get('AWS_DEFAULT_REGION') or 'us-east-1'
        item = boto3.resource('dynamodb', region_name=region).Table(REGISTRY_TABLE).get_item(
            Key={'agent_id': REGISTRY_KEY}).get('Item')
    except Exception as e:
        _warn_once('read', f"could not read agent-config/{REGISTRY_KEY} ({e}) — using the built-in registry")
        return copy.deepcopy(BUILTIN), 'builtin (read failed)'
    if not item:
        _warn_once('missing', f"no agent-config/{REGISTRY_KEY} item — using the built-in registry")
        return copy.deepcopy(BUILTIN), 'builtin (no item)'
    doc = merge(item.get('registry') or {})
    problems = validate(doc)
    if problems:
        _warn_once('invalid', f"stored registry is invalid — using the built-in registry: {problems[:5]}")
        return copy.deepcopy(BUILTIN), 'builtin (stored item invalid)'
    return doc, 'dynamodb'


def registry(force=False):
    now = time.time()
    if force or _cache['doc'] is None or now - _cache['at'] > CACHE_TTL_S:
        _cache['doc'], _cache['source'] = _load()
        _cache['at'] = now
    return _cache['doc']


def source():
    registry()
    return _cache['source']


def normalize(model_id):
    """'us.anthropic.claude-haiku-4-5-20251001-v1:0' → 'claude-haiku-4-5'."""
    m = (model_id or '').strip().lower()
    if m.startswith('arn:'):  # inference-profile / foundation-model ARNs (the KB parser reports these)
        m = m.rsplit('/', 1)[-1]
    m = re.sub(r'^(us|eu|apac|global|jp|au)\.', '', m)
    m = re.sub(r'^anthropic\.', '', m)
    m = re.sub(r'-v\d+(:\d+)?$', '', m)
    m = re.sub(r'-\d{8}$', '', m)
    m = re.sub(r'@\d{8}$', '', m)
    return m


def model_facts(model_id, doc=None):
    """The registry's entry for a model (exact, then longest known prefix), or None."""
    models = (doc or registry())['models']
    n = normalize(model_id)
    if n in models:
        return models[n]
    best = None
    for key in models:
        if n.startswith(key) and (best is None or len(key) > len(best)):
            best = key
    return models[best] if best else None


def price(model_id):
    """$/MTok {'in','out','cache_read','cache_write'} or None if the model is not priced."""
    f = model_facts(model_id)
    if not f or 'in' not in f:
        return None
    return {'in': round(float(f['in']), 6), 'out': round(float(f['out']), 6),
            'cache_read': round(float(f.get('cache_read', f['in'] * 0.1)), 6),
            'cache_write': round(float(f.get('cache_write', f['in'] * 1.25)), 6)}


def resolve(role, platform=None):
    """Which model plays `role`. `platform` is used only when the role is not pinned.

    Returns a dict: role, model, platform, rung, base_rung, effort, max_tokens,
    fallbacks (cheaper models on the same platform, nearest first), note —
    plus `stepped_down_from` / `shifted_by` when either happened.
    """
    doc = registry()
    rungs = doc['rungs']
    spec = doc['roles'].get(role)
    if spec is None:
        spec = BUILTIN['roles'].get(role)
        if spec is None:
            _warn_once(f'role:{role}', f"UNKNOWN ROLE {role!r} — add it to the registry; using 'standard' meanwhile")
            spec = {'rung': 'standard'}
        else:
            _warn_once(f'role:{role}', f"role {role!r} is not in the stored registry — using its built-in entry")

    plat = spec.get('platform') or platform or 'claude'
    if platform and spec.get('platform') and platform != spec['platform']:
        _warn_once(f'pin:{role}:{platform}', f"role {role!r} is pinned to {spec['platform']}; ignoring {platform}")
    ladder = doc['platforms'].get(plat)
    if not ladder:
        _warn_once(f'plat:{plat}', f"no ladder for platform {plat!r} — role {role!r} falls back to 'claude'")
        plat, ladder = 'claude', doc['platforms']['claude']

    base = (spec.get('platform_rungs') or {}).get(plat, spec['rung'])
    idx = rungs.index(base)
    shift = 0
    if spec.get('shift', True) is not False:
        shift = int(doc.get('budget_shift', 0) or 0)
    lo = rungs.index(spec['floor']) if spec.get('floor') else 0
    hi = rungs.index(spec['ceiling']) if spec.get('ceiling') else len(rungs) - 1
    want = max(lo, min(hi, idx + shift))

    chosen = next((i for i in range(want, -1, -1) if rungs[i] in ladder), None)
    if chosen is None:
        chosen = next(i for i in range(want, len(rungs)) if rungs[i] in ladder)
    model = ladder[rungs[chosen]]

    fallbacks = []
    for i in range(chosen - 1, -1, -1):
        m = ladder.get(rungs[i])
        if m and m != model and m not in fallbacks:
            fallbacks.append(m)

    out = {'role': role, 'model': model, 'platform': plat, 'rung': rungs[chosen], 'base_rung': base,
           'effort': spec.get('effort'), 'max_tokens': spec.get('max_tokens'),
           'fallbacks': fallbacks, 'note': spec.get('note')}
    if want != idx:
        out['shifted_by'] = want - idx
    if chosen != want:
        out['stepped_down_from'] = rungs[want]
    return out


def model_for(role, platform=None):
    return resolve(role, platform)['model']


def known_models(doc=None):
    """Every normalized model id the registry knows (ladders + facts table)."""
    doc = doc or registry()
    known = set(doc['models'])
    for ladder in doc['platforms'].values():
        known.update(normalize(m) for m in ladder.values())
    return known


def is_known(model_id, doc=None):
    return model_facts(model_id, doc) is not None or normalize(model_id) in known_models(doc)


def describe(doc=None):
    """Human-readable view: ladders, then every role and what it resolves to."""
    doc = doc or registry()
    lines = [f"rungs: {' → '.join(doc['rungs'])}    budget_shift: {doc.get('budget_shift', 0)}", '']
    for plat, ladder in doc['platforms'].items():
        lines.append(f"  {plat:10} " + '   '.join(f"{r}={ladder[r]}" for r in doc['rungs'] if r in ladder))
    lines.append('')
    for name in sorted(doc['roles']):
        r = resolve(name)
        extra = []
        if r.get('shifted_by'):
            extra.append(f"shifted {r['shifted_by']:+d}")
        if r.get('stepped_down_from'):
            extra.append(f"stepped down from {r['stepped_down_from']}")
        if r.get('effort'):
            extra.append(f"effort {r['effort']}")
        lines.append(f"  {name:20} {r['rung']:9} {r['platform']:10} {r['model']}"
                     + (f"   ({', '.join(extra)})" if extra else ''))
    return '\n'.join(lines)
