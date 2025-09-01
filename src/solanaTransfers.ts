import {HttpClient} from '@belopash/core/http-client'
import {assert, maybeLast} from '@belopash/core/internal/misc'
import {createLogger} from '@belopash/core/logger'
import {createTracker} from '@belopash/core/pipeline'
import {PortalClient} from '@belopash/core/portal'
import {createSolanaPortalSource, SolanaQueryBuilder} from '@belopash/solana-stream'
import {createTypeormTarget} from '@belopash/typeorm-target/database'
import * as tokenProgram from './abi/token-program'
import * as whirlpool from './abi/whirlpool'
import {Exchange} from './model'

const portal = new PortalClient({
    url: 'https://portal.sqd.dev/datasets/solana-mainnet',
    http: new HttpClient({
        retryAttempts: Number.POSITIVE_INFINITY,
    }),
    minBytes: 100 * 1024 * 1024,
})

async function main() {
    let head = await portal.getHead().then((h) => h?.number ?? 0)
    let fromBlock = head - 100_000
    let toBlock = undefined

    console.log(`processing range: [${fromBlock}, ${toBlock ?? null}]`)

    const whirlpoolQuery = new SolanaQueryBuilder()
        .addInstruction({
            request: {
                programId: ['whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'],
                d8: ['0xf8c69e91e17587c8'],
                isCommitted: true,
                innerInstructions: true,
                transaction: true,
                transactionTokenBalances: true,
            },
        })
        .build()

    await createSolanaPortalSource({
        portal,
        fields: {
            block: {number: true, timestamp: true, hash: true, parentHash: true},
            transaction: {signatures: true, err: true, transactionIndex: true},
            instruction: {
                programId: true,
                accounts: true,
                data: true,
                isCommitted: true,
                transactionIndex: true,
                instructionAddress: true,
            },
            tokenBalance: {
                account: true,
                preMint: true,
                preOwner: true,
                preAmount: true,
                postMint: true,
                postOwner: true,
                postAmount: true,
            },
        },
        query: whirlpoolQuery,
    })
        //.pipe(createFinalizer())
        .pipe(createProgressTracker('solana'))
        .pipe(
            createTypeormTarget({}, async (store, batch) => {
                for (let block of batch) {
                    for (let ins of block.instructions) {
                        if (ins.programId === whirlpool.programId && ins.d8 === whirlpool.instructions.swap.d8) {
                            // Transfers are temporarily called "Exchanges"
                            let exchange = new Exchange({
                                id: formatId(block.header, ins.transactionIndex, ...ins.instructionAddress),
                                slot: block.header.number,
                                tx: ins.transaction?.signatures[0] ?? 'null',
                                timestamp: new Date(block.header.timestamp * 1000),
                            })

                            assert(ins.inner.length === 2)
                            let srcTransfer = tokenProgram.transfer.decode(ins.inner[0])
                            let destTransfer = tokenProgram.transfer.decode(ins.inner[1])

                            let srcBalance = ins.transaction?.tokenBalances.find(
                                (tb) => tb.account === srcTransfer.accounts.source,
                            )
                            let destBalance = ins.transaction?.tokenBalances.find(
                                (tb) => tb.account === destTransfer.accounts.destination,
                            )

                            let srcMint = ins.transaction?.tokenBalances.find(
                                (tb) => tb.account === srcTransfer.accounts.destination,
                            )?.preMint
                            let destMint = ins.transaction?.tokenBalances.find(
                                (tb) => tb.account === destTransfer.accounts.source,
                            )?.preMint

                            assert(srcMint != null)
                            assert(destMint != null)

                            exchange.fromToken = srcMint
                            exchange.fromOwner = srcBalance?.preOwner || srcTransfer.accounts.source
                            exchange.fromAmount = srcTransfer.data.amount

                            exchange.toToken = destMint
                            exchange.toOwner =
                                destBalance?.postOwner || destBalance?.preOwner || destTransfer.accounts.destination
                            exchange.toAmount = destTransfer.data.amount

                            await store.insert(exchange)
                        }
                    }
                }
            }),
        )

    console.log('end')
}

export function createProgressTracker<
    TCursor extends {number: number; hash: string},
    TValue extends {header: {timestamp: number}}[],
    TQuery,
>(prefix: string) {
    const logger = createLogger(`sqd:${prefix}`)
    const readTimer = createTimer()
    const writeTimer = createTimer()
    const logIntervalMs = 5_000
    const emitLog = () => {
        if (!stats?.cursor || !stats.head) return
        const now = Date.now()
        const headNumber = stats.head?.number ?? stats.cursor.number
        const finalizedNumber = stats.finalizedHead?.number
        const remainingBlocks = Math.max(0, headNumber - stats.cursor.number)
        const percentVal = (1 - remainingBlocks / headNumber) * 100
        const percent = Math.max(0, Math.min(100, percentVal))
        const etaSec =
            stats.avgBlocksPerSec && stats.avgBlocksPerSec > 0 ? remainingBlocks / stats.avgBlocksPerSec : undefined
        const percentStr = `${percent.toFixed(2)}%`
        const etaStr = etaSec == null ? 'n/a' : `${etaSec.toFixed(0)}s`

        // Compute windowed throughput since last log; if nothing happened, it decays to 0
        const windowMs = now - (stats.lastLogTimeMs ?? now)
        const windowBlocks = (stats.totalBlocks ?? 0) - (stats.lastLogTotalBlocks ?? 0)
        const windowBlocksPerSec = windowMs > 0 ? (windowBlocks * 1000) / windowMs : 0
        stats.avgBlocksPerSec =
            stats.avgBlocksPerSec == null ? windowBlocksPerSec : (stats.avgBlocksPerSec + windowBlocksPerSec) / 2

        logger.info(
            {
                lag: `${((now - (stats.lastBlockTime ?? now)) / 1000).toFixed(2)}s`,
                batchSize: stats.lastBatchSize ?? 0,
                blocksPerSec: Number(windowBlocksPerSec.toFixed(2)),
                avgBlocksPerSec: Number((stats.avgBlocksPerSec ?? 0).toFixed(2)),
                avgBatchSize: Number((stats.avgBatchSize ?? 0).toFixed(2)),
                avgReadTime: `${((stats.avgReadTime ?? 0) / 1000).toFixed(2)}s`,
                lastReadTime: `${((stats.lastReadTime ?? 0) / 1000).toFixed(2)}s`,
                avgWriteTime: `${((stats.avgWriteTime ?? 0) / 1000).toFixed(2)}s`,
                lastWriteTime: `${((stats.lastWriteTime ?? 0) / 1000).toFixed(2)}s`,
                totalBlocks: stats.totalBlocks,
            },
            `progress: ${stats.cursor.number} / ${headNumber} (${finalizedNumber ?? 0}) — ${percentStr}, ETA: ${etaStr}`,
        )

        // Update window markers
        stats.lastLogTimeMs = now
        stats.lastLogTotalBlocks = stats.totalBlocks ?? 0
    }

    let stats:
        | {
              cursor: TCursor | undefined
              head: TCursor | undefined
              finalizedHead: TCursor | undefined
              lastBlockTime: number | undefined
              avgReadTime: number | undefined
              lastReadTime: number | undefined
              avgWriteTime: number | undefined
              lastWriteTime: number | undefined
              startNumber: number | undefined
              startTimeMs: number | undefined
              targetNumber: number | undefined
              lastLogTimeMs: number | undefined
              lastLogTotalBlocks: number | undefined
              totalBlocks: number | undefined
              lastBatchSize: number | undefined
              avgBatchSize: number | undefined
              avgBlocksPerSec: number | undefined
          }
        | undefined = undefined

    return createTracker<TCursor, TValue, TQuery>({
        beforeRead: () => {
            if (!stats) {
                stats = {
                    cursor: undefined,
                    head: undefined,
                    finalizedHead: undefined,
                    lastBlockTime: undefined,
                    avgReadTime: undefined,
                    avgWriteTime: undefined,
                    lastReadTime: undefined,
                    lastWriteTime: undefined,
                    startNumber: undefined,
                    startTimeMs: Date.now(),
                    targetNumber: undefined,
                    lastLogTimeMs: Date.now(),
                    lastLogTotalBlocks: 0,
                    totalBlocks: 0,
                    lastBatchSize: undefined,
                    avgBatchSize: undefined,
                    avgBlocksPerSec: undefined,
                }
                setInterval(emitLog, logIntervalMs)
            }
            readTimer.start()
        },
        afterRead: () => {
            if (!stats) return

            const elapsed = readTimer.stop()

            stats.avgReadTime = stats.avgReadTime == null ? elapsed : (stats.avgReadTime + elapsed) / 2
            stats.lastReadTime = elapsed
        },
        beforeWrite: () => {
            writeTimer.start()
        },
        afterWrite: (message) => {
            if (!stats) return

            const lastItem = maybeLast(message.data)
            const lastBlock = lastItem ? maybeLast(lastItem.value) : undefined

            const elapsed = writeTimer.stop()
            stats.avgWriteTime = stats.avgWriteTime == null ? elapsed : (stats.avgWriteTime + elapsed) / 2
            stats.lastWriteTime = elapsed
            stats.lastBlockTime = (lastBlock?.header.timestamp ?? 0) * 1000
            stats.head = message.head
            stats.finalizedHead = message.finalizedHead
            stats.cursor = lastItem?.cursor ?? stats.cursor
            if (stats.targetNumber == null) {
                stats.targetNumber = message.finalizedHead?.number ?? message.head.number
            }

            const batchSize = message.data.length
            stats.totalBlocks = (stats.totalBlocks ?? 0) + batchSize
            stats.lastBatchSize = batchSize
            stats.avgBatchSize = stats.avgBatchSize == null ? batchSize : (stats.avgBatchSize + batchSize) / 2

            const cycleMs = (stats.lastReadTime ?? 0) + (stats.lastWriteTime ?? 0)
            const instBlocksPerSec = cycleMs > 0 ? (batchSize * 1000) / cycleMs : 0
            stats.avgBlocksPerSec =
                stats.avgBlocksPerSec == null ? instBlocksPerSec : (stats.avgBlocksPerSec + instBlocksPerSec) / 2
        },
    })
}

function createTimer() {
    let start: number | undefined = undefined

    return {
        start: () => {
            start = Date.now()
        },
        stop: () => {
            if (start == null) return 0
            let elapsed = Date.now() - start
            start = undefined
            return elapsed
        },
    }
}

function formatId(block: {number: number; hash: string}, ...address: number[]): string {
    let no = block.number.toString().padStart(12, '0')
    let hash = block.hash.slice(0, 5)
    let id = `${no}-${hash}`
    for (let index of address) {
        id += `-${index.toString().padStart(6, '0')}`
    }
    return id
}

main()
