# Contributing

## local setup

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

## run checks

```bash
python3 -m py_compile fetch_bulk_posts_idempotent.py bulk_posts/*.py
python3 fetch_bulk_posts_idempotent.py --help
```

## pull requests

- Keep changes focused and small.
- Preserve idempotent behavior in `bulk_posts/state_store.py`.
- Update `README.md` when CLI or outputs change.

