# sdk2-basics

A collection of basic SDK2 examples.

## basicPipeline.ts

Read at `src/basicPipeline.ts`. To run:
```bash
npm run basic-pipeling
```

## solanaTransfers.ts

Read at `src/solanaTransfers.ts` (uncommented). To run:
```bash
docker compose up -d
npm run build
make process
```
Read the database with
```bash
PGPASSWORD="postgres" PAGER="less -S" psql -h localhost -d defaultdb -U root -p 23751
```
