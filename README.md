# sqlite-jsonb

Rust implementation of the internal SQLite JSONB format.

The JSONB format is stable, but you should normally not use it. SQLite provides a `json()` function that converts JSONB to JSON text
and you can then parse that text in your application. There are some cases where using JSONB may be justified:
- You are writing an extension for SQLite with a function that operates on JSON--for the best UX you should handle both JSON text and JSONB.
- You have a specific performance problem with reading JSON from your SQLite database where the columns contain JSONB.

## License

ⓒ Renée Kooi · Dual-licensed under [Apache-2.0](./LICENSE-APACHE) or [MIT](./LICENSE-MIT) at your option.
