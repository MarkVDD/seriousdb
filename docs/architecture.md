# Architecture

The application is currently intentionally small:

- `main.py` creates the FastAPI application and defines the HTTP routes.
- The database is represented as a Python dictionary in memory while a request is handled.
- The dictionary is loaded from and written to the local `.sdb` file.
- Each entry stores its value and an optional absolute expiration time.

The service starts with a default entry when `.sdb` does not exist. There is no separate database process or client library.

## Request flow

1. FastAPI receives a request.
2. The route loads the dictionary from `.sdb`.
3. A `PUT` updates and rewrites the file; a `GET` checks the requested value's
   expiration time.
4. An expired entry is deleted and treated as missing.
5. The route returns the value or a `404` error.
