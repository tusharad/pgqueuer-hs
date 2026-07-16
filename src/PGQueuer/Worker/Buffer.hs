{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE ScopedTypeVariables #-}

{- |
Module      : PGQueuer.Worker.Buffer
Description : STM-backed timed overflow buffer for bulk database writes.
Copyright   : (c) Tushar Adhatrao, 2026
License     : MIT
Maintainer  : tusharadhatrao@gmail.com
Stability   : experimental

Provides a generic 'TimedOverflowBuffer' that decouples worker threads
from database round-trips. Items are pushed into a bounded STM queue.
A dedicated flusher thread wakes when either:

  1. The buffer reaches 'bcMaxSize' items (overflow trigger), or
  2. A periodic timer of 'bcFlushInterval' microseconds expires and
     the queue is non-empty.

All items are then drained and flushed to the database in a single
bulk operation, dramatically reducing per-job WAL write amplification.

The 'withBuffer' bracket ensures the flusher thread is cleanly shut
down and all remaining items are drained on exit.
-}
module PGQueuer.Worker.Buffer (
    -- * Configuration
    BufferConfig (..),
    defaultBufferConfig,

    -- * Core buffer type
    TimedOverflowBuffer,

    -- * Lifecycle
    newBuffer,
    withBuffer,

    -- * Operations
    add,
    flushBuffer,

    -- * Specialized sinks
    JobStatusLogBuffer,
    HeartbeatBuffer,
    mkJobStatusLogBuffer,
    mkHeartbeatBuffer,
) where

import Control.Concurrent.Async (async, uninterruptibleCancel)
import Control.Concurrent.STM
import Control.Exception (SomeException, bracket, catch)
import Control.Monad (unless, void, when)
import Data.Aeson (Value)
import PGQueuer.Types (JobId, JobStatus)

-- ============================================================================
-- Configuration
-- ============================================================================

-- | Configuration for a 'TimedOverflowBuffer'.
data BufferConfig = BufferConfig
    { bcMaxSize :: !Int
    -- ^ Maximum number of items before an immediate flush is triggered.
    , bcFlushInterval :: !Int
    {- ^ Flush interval in microseconds. The flusher thread will wake
    at most this often even if the buffer hasn't overflowed.
    -}
    }
    deriving (Show, Eq)

-- | Default buffer configuration: 100 items max, 100ms flush interval.
defaultBufferConfig :: BufferConfig
defaultBufferConfig =
    BufferConfig
        { bcMaxSize = 100
        , bcFlushInterval = 100_000 -- 100ms in microseconds
        }

-- ============================================================================
-- Core buffer type
-- ============================================================================

{- | A generic STM-backed timed overflow buffer.

Items of type @a@ are enqueued by worker threads via 'add'. A dedicated
background flusher thread drains the queue and calls the flush action
when either the overflow threshold or the timer fires.
-}
data TimedOverflowBuffer a = TimedOverflowBuffer
    { bufQueue :: !(TBQueue a)
    -- ^ Bounded STM queue holding buffered items
    , bufCount :: !(TVar Int)
    -- ^ Current element count (O(1) reads, avoids O(n) lengthTBQueue)
    , bufOverflow :: !(TMVar ())
    -- ^ Overflow trigger: filled when count reaches maxSize
    , bufMaxSize :: !Int
    -- ^ Maximum buffer size before overflow
    , bufFlushInterval :: !Int
    -- ^ Flush interval in microseconds
    , bufFlushAction :: [a] -> IO ()
    -- ^ Callback to flush accumulated items to the database
    , bufShutdown :: !(TVar Bool)
    -- ^ Set to True to signal the flusher thread to exit
    }

-- ============================================================================
-- Construction
-- ============================================================================

{- | Create a new 'TimedOverflowBuffer'. Does NOT start the flusher thread;
use 'withBuffer' for a fully managed lifecycle.
-}
newBuffer :: BufferConfig -> ([a] -> IO ()) -> IO (TimedOverflowBuffer a)
newBuffer config flushAction = do
    queue <- newTBQueueIO (fromIntegral $ bcMaxSize config)
    count <- newTVarIO 0
    overflow <- newEmptyTMVarIO
    shutdown <- newTVarIO False
    return
        TimedOverflowBuffer
            { bufQueue = queue
            , bufCount = count
            , bufOverflow = overflow
            , bufMaxSize = bcMaxSize config
            , bufFlushInterval = bcFlushInterval config
            , bufFlushAction = flushAction
            , bufShutdown = shutdown
            }

-- ============================================================================
-- Operations
-- ============================================================================

{- | Add an item to the buffer.

If adding this item causes the buffer count to reach 'bufMaxSize',
the overflow trigger is signalled, causing the flusher thread to
wake immediately and flush all items.

This function never blocks on a full queue — the TBQueue capacity
equals maxSize, and we flush at maxSize, so there's always room
unless the flusher is severely lagging. In that pathological case,
the STM retry on the full TBQueue provides natural backpressure.
-}
add :: TimedOverflowBuffer a -> a -> IO ()
add buf item = atomically $ do
    writeTBQueue (bufQueue buf) item
    n <- readTVar (bufCount buf)
    let n' = n + 1
    writeTVar (bufCount buf) n'
    when (n' >= bufMaxSize buf) $
        -- Signal overflow; tryPutTMVar is idempotent if already full
        void $
            tryPutTMVar (bufOverflow buf) ()

{- | Drain all items from the buffer in a single STM transaction.

Returns an empty list if the queue is empty. Resets the count to 0
and clears the overflow trigger.
-}
drainAll :: TimedOverflowBuffer a -> STM [a]
drainAll buf = do
    items <- drainQueue (bufQueue buf)
    writeTVar (bufCount buf) 0
    -- Clear overflow trigger if it was set
    _ <- tryTakeTMVar (bufOverflow buf)
    return items

-- | Drain a TBQueue into a list (oldest first).
drainQueue :: TBQueue a -> STM [a]
drainQueue q = go []
  where
    go acc = do
        mItem <- tryReadTBQueue q
        case mItem of
            Nothing -> return (reverse acc)
            Just item -> go (item : acc)

{- | Flush the buffer: drain all items and call the flush action.
Does nothing if the buffer is empty.
-}
flushBuffer :: TimedOverflowBuffer a -> IO ()
flushBuffer buf = do
    items <- atomically (drainAll buf)
    unless (null items) $
        bufFlushAction buf items

-- ============================================================================
-- Periodic flusher
-- ============================================================================

{- | Background flusher loop.

Waits for either:
  1. The overflow trigger (immediate flush), or
  2. The timer to expire AND the queue to be non-empty.

On shutdown signal, performs one final drain and exits.
-}
periodicFlusher :: TimedOverflowBuffer a -> IO ()
periodicFlusher buf = loop
  where
    loop = do
        -- Create a fresh timer for this iteration
        timerVar <- registerDelay (bufFlushInterval buf)

        -- Wait for a wake-up condition
        shouldExit <- atomically $ do
            isShutdown <- readTVar (bufShutdown buf)
            if isShutdown
                then return True
                else do
                    -- Try overflow trigger first
                    let overflowWake = takeTMVar (bufOverflow buf) >> return False
                    -- Or wait for timer + non-empty queue
                    let timerWake = do
                            expired <- readTVar timerVar
                            check expired
                            cnt <- readTVar (bufCount buf)
                            check (cnt > 0)
                            return False
                    -- Or shutdown
                    let shutdownWake = do
                            s <- readTVar (bufShutdown buf)
                            check s
                            return True
                    overflowWake `orElse` timerWake `orElse` shutdownWake

        -- Flush whatever we have
        flushBuffer buf
            `catch` (\(_ :: SomeException) -> return ())

        unless shouldExit loop

-- ============================================================================
-- Managed lifecycle
-- ============================================================================

{- | Run an action with a managed buffer.

Starts the periodic flusher in a background thread, runs the provided
action, and on exit:

  1. Signals the flusher to shut down.
  2. Cancels the flusher thread.
  3. Performs a final drain to ensure no items are lost.

This provides clean graceful shutdown via 'bracket'.
-}
withBuffer ::
    BufferConfig ->
    ([a] -> IO ()) ->
    (TimedOverflowBuffer a -> IO b) ->
    IO b
withBuffer config flushAction action =
    bracket
        ( do
            buf <- newBuffer config flushAction
            flusherThread <- async (periodicFlusher buf)
            return (buf, flusherThread)
        )
        ( \(buf, flusherThread) -> do
            -- Signal shutdown
            atomically $ writeTVar (bufShutdown buf) True
            -- Cancel the flusher thread
            uninterruptibleCancel flusherThread
            -- Final drain — don't lose any buffered items
            flushBuffer buf
        )
        (\(buf, _) -> action buf)

-- ============================================================================
-- Specialized sinks
-- ============================================================================

{- | A buffer specialized for job status logging.
Buffers @(JobId, JobStatus, Maybe Value)@ tuples for bulk ACK.
-}
type JobStatusLogBuffer = TimedOverflowBuffer (JobId, JobStatus, Maybe Value)

{- | A buffer specialized for heartbeat updates.
Buffers @JobId@s for bulk heartbeat refresh.
-}
type HeartbeatBuffer = TimedOverflowBuffer JobId

-- | Create a 'JobStatusLogBuffer' with the given flush action.
mkJobStatusLogBuffer ::
    BufferConfig ->
    ([(JobId, JobStatus, Maybe Value)] -> IO ()) ->
    IO JobStatusLogBuffer
mkJobStatusLogBuffer = newBuffer

-- | Create a 'HeartbeatBuffer' with the given flush action.
mkHeartbeatBuffer ::
    BufferConfig ->
    ([JobId] -> IO ()) ->
    IO HeartbeatBuffer
mkHeartbeatBuffer = newBuffer
