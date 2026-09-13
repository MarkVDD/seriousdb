# Persistence

Data is stored in a local file named `.sdb` in the process working directory.

The file contains a versioned JSON object. On first startup, the application
creates it with a persistent default entry:

```json
{
  "version": 1,
  "entries": {
    "default": {
      "value": "default",
      "expires_at": null
    }
  }
}
```

Each `PUT` loads the complete dictionary, changes one key, and writes the
complete dictionary back to disk. Entries with a time to live store their
expiration as a Unix timestamp. The expiration therefore remains effective
after the server restarts.

Existing files that contain a flat JSON dictionary are read as version 1 data
with no expiration times. They are converted to the versioned format on the
next write.

## Current constraints

- The file is local to the machine running the server.
- Requests use the complete dictionary rather than a database engine.
- Concurrent writes and multi-process access are not currently coordinated.
- Expired entries are removed when requested rather than by a background task.
