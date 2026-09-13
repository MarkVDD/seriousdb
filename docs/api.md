# API reference

The server exposes a small HTTP API through FastAPI.

Interactive OpenAPI documentation is available at `http://127.0.0.1:8000/docs` while the server is running.

### PUT `/db`

Stores or updates a key-value pair.

Parameters:

- `key` - The key to store.
- `value` - The value associated with the key.
- `ttl_seconds` - Optional number of seconds before the key expires. It must be
  greater than zero.

For example:

```text
key: name
value: Alice
```

This stores:

```python
{"name": "Alice"}
```

alongside any existing key-value pairs.

To store a key for five minutes, set `ttl_seconds` to `300`:

```text
key: session
value: abc123
ttl_seconds: 300
```

Updating a key without `ttl_seconds` makes it persistent, even if it previously
had an expiration time. A zero or negative `ttl_seconds` value returns a `422`
response.

### GET `/db`

Retrieves the value associated with a key.

For example:

```text
key: name
```

returns:

```text
Alice
```

If the requested key does not exist or has expired, the API returns a `404`
response. Expired keys are removed when they are requested.
