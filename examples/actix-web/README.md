# How to run apalis together with actix-web

### Running

```bash
cargo run
```

### Test push job via curl

```bash
curl -X POST \
  http://localhost:8000/emails/push \
  -H 'cache-control: no-cache' \
  -H 'content-type: application/json' \
  -d '{
	"to": "test@gmail.com",
	"subject": "Message from Web",
	"text": "This is the text"
}'
```
