/**
 * SDK2 uses a custom data piping system designed for handling restarts and forks.
 *
 * Every data item is identified by a cursor. By comparing cursors, we can determine
 * which data item is newer or older. In systems with forks, we can also find out
 * if the data items occured simultaneously in different branches of the fork.
 * 
 * Data is streamed in form of messages that can be of one of the following types:
 *  - data - a batch of data in the following format:
 *    {
 *      type: 'data',
 *      cursor: TCursor,          // The cursor position at the end of this batch (inclusive)
 *      head: TCursor,            // The highest cursor position in the whole data stream
 *      finalizedHead?: TCursor,  // The highest cursor position that corresponds to final data, if available
 *      data: {cursor: TCursor, value: TValue}[]
 *    }
 *  - fork - a fork in the data stream in the following format:
 *    {
 *      type: 'fork',
 *      cursors: TCursor[], // Array of cursor positions valid within the post-fork data
 *    }
 * In this example we'll look at pipelines that only move around data. Basic fork processing example is TBA.
 * 
 * Developers can stream from data sources by piping to targets.
 * Targets themselves can return sources, allowing for component chaining; such targets
 * are sometimes called transformers. We'll collectively refer to sources, targets and
 * transformers as "pipeline components".
 * 
 * Sources are valid async iterators. This makes it easy to plug them into external systems.
 * (Note: this seems to be broken ATM)
 * 
 * Sources can be configured by targets in runtime. The two variables available for adjustment are:
 *  1. cursor - indicates the position up to which the source
 *              should skip the data (e.g. because it is already processed)
 *  2. query - any extra modifications of the source configuration
 * 
 * Setting initialization aside for now, here's how the main execution
 * of a simple source <-> target pipeline proceeds:
 *  - Target's write() method is called.
 *  - It calls source's read() method with the initial values of
 *    cursor and query, or without arguments if these values are not known.
 *  - It returns an async iterator yielding messages.
 *  - Target's write() method iterates over the messages and processes them.
 *    It may persist state and save the latest cursor value to be used in the
 *    read() call after a restart.
 * 
 * In addition to the main data wrangling methods read() and write(), pipeline
 * components contain some extra fields:
 *  - unfinalized - boolean indicating whether the component can work with
 *                  unfinalized data - that is, process fork messages.
 *                  These flags are compared at initialization time to ensure
 *                  that unfinalized data is not passed to components that
 *                  cannot work with it.
 *  - cursorUtils - (only in sources) an object containing
 *                  cursor comparison, serialization and deserialization methods.
 * 
 * cursorUtils are passed from sources because cursor format is customizable
 * and can change as the data is transformed. E.g. blockchain transactions grouped
 * by block (identified by number + hash) can be flattened and identified by block
 * number + transaction index + block hash. An updated standard set of utilities
 * is passed down whenever there's a transformer that changes the cursor format.
 * 
 */

// Note: we should probably rename queries to requests and vice versa, but we haven't done that yet

import {
    type DataCursorUtils,
    DataCursor,
    createSource,
    createTarget,
    DataReadRequest,
    DataWriteContext,
    createTransformer
} from '@belopash/core/pipeline'

interface SimpleCursor {
    number: number
}

// the data value type
interface SimpleBlock {
    transactionCount: number
}

// using null requests for simplicity
type SimpleQuery = null

// We won't actually use cursor utils in the primitive examples
// below, but they are required by types so we still have to define them
const cursorUtils: DataCursorUtils<SimpleCursor> = {
    compare: (a: SimpleCursor, b: SimpleCursor) => {
        if (a.number < b.number) return DataCursor.Less
        if (a.number > b.number) return DataCursor.Greater
        return DataCursor.Equal
    },
    serialize: (value: SimpleCursor) => JSON.stringify(value),
    deserialize: (value: unknown) => JSON.parse(value as string) as SimpleCursor,
}

async function main() {

    // createSource() returns an object with the same three fields
    // plus some methods for pipeline building
    const dataSource = createSource({
        unfinalized: false,
        cursorUtils,
        // devs will typically want to explicitly type the argument of read()
        // so that only the targets that feed it an argument of the same type
        // can be used with the source
        read: async function* ({cursor, query}: DataReadRequest<SimpleCursor,SimpleQuery>) {
            console.log(`Data source read called with cursor`, cursor)

            const firstBlock = cursor ? cursor.number + 1 : 1
            for (let i = firstBlock; i <= 3; i++) {

                console.log(`Yielding block ${i}`)

                const cursor = { number: i }
                yield {
                    type: 'data' as const,
                    cursor,
                    head: {number: 3},
                    finalizedHead: {number: 3},
                    data: [{
                        cursor,
                        value: {
                            transactionCount: i * 20
                        }
                    }]
                }
            }
        },
    })
    
    // A target-producing function that can change the target's starting cursor.
    // Normally the returned object would be made by a createTarget()
    // convenience function that ensures proper typing. I'm omitting it here
    // to make the structure of the target object apparent.
    const makeTarget = (startCursor?: SimpleCursor) => ({
        unfinalized: false,
        write: async ({read, cursorUtils}: DataWriteContext<SimpleCursor, SimpleBlock, SimpleQuery>) => {
            console.log('Target write called')
            for await (const message of read({cursor: startCursor, query: null})) {
                switch (message.type) {
                    case 'data':
                        console.log(`Processing batch with ${message.data.length} items: `, message.data)
                        break
                    case 'fork':
                        console.log('Processing fork:', message)
                        break
                }
            }
        },
    })

    // Iteration will start from the first block
    //
    // Equivalent to:
    //
    // console.log('Target write called')
    // for await (const message of dataSource.read({cursor: undefined, request: undefined})) {
    //     switch (message.type) {
    //         case 'batch':
    //             console.log(`Processing batch with ${message.data.length} items: `, message.data)
    //             break
    //         case 'fork':
    //             console.log('Processing fork:', message)
    //             break
    //     }
    // }

    await dataSource.pipe(makeTarget())
    console.log('--------------------------------')


    // Iteration will start from the third block
    await dataSource.pipe(makeTarget({number: 2}))
    console.log('--------------------------------')

    // Transformer example
    const transformer = {
        unfinalized: false,
        write: ({read, cursorUtils}: DataWriteContext<SimpleCursor, SimpleBlock, SimpleQuery>) => {
            console.log('Transformer write called')
            return createSource({
                unfinalized: false,
                cursorUtils: cursorUtils,
                read: async function* ({cursor, query}: DataReadRequest<SimpleCursor, SimpleQuery>) {
                    for await (const message of read({cursor, query})) {
                        console.log('Transformer read called', message)
                        yield message
                    }
                },
            })
        }
    }
    // Shorthand version - less verbose, but does the same thing
    const altTransformer = createTransformer(({unfinalized, cursorUtils, read}) => ({
        unfinalized,
        cursorUtils,
        read: async function* ({cursor, query}: DataReadRequest<SimpleCursor, SimpleQuery>) {
            for await (const message of read({cursor, query})) {
                console.log('Transformer read called', message)
                yield message
            }
        },
    }))

    await dataSource.pipe(altTransformer).pipe(makeTarget({number: 2}))
    console.log('--------------------------------')

    // Utility functions can provide special targets/transformers
    //  - finalize(): transformer that cuts the stream to finalizedHead,
    //                ensuring that all data is final. Should not be used
    //                with sources that don't provide finalizedHead, for now.
    //  - map, reduce, filter, forEach: create targets that do what these functions
    //                                  usually do to the data w/o modifying the cursor
    //  - scan: like reduce, but streams intermediate accumulator values
    //          instead of returning a single value at the end

}

main().catch(console.error)
