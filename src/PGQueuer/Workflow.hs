{-# LANGUAGE DerivingStrategies #-}

{- |
Module      : PGQueuer.Workflow
Description : Directed Acyclic Graph (DAG) workflows and dependency job trees.
Copyright   : (c) Tushar Adhatrao, 2026
License     : MIT
Maintainer  : tusharadhatrao@gmail.com
Stability   : experimental

Provides support for hierarchical, tree-structured job dependencies.
A parent job can be submitted along with a set of dependent child jobs;
the parent is atomically held until all children reach 'Successful' completion,
at which point the parent is automatically released into the active queue and
can access its children's aggregated results.
-}
module PGQueuer.Workflow
  ( JobNode (..)
  , JobTree (..)
  , (<~~)
  , insertJobTree
  )
where

import Control.Monad (foldM_)
import Data.Aeson (Value)
import qualified Data.ByteString.Lazy as BL
import Data.Maybe (listToMaybe)
import Data.Text (Text)
import Data.Time (NominalDiffTime)
import PGQueuer.Core.Monad (MonadPGQueuer (..))
import PGQueuer.Types (Entrypoint, JobId (..), JobStatus (..))

-- | A single node representing a job's parameters within a 't:JobTree'.
data JobNode = JobNode
  { jnEntrypoint :: Entrypoint
  -- ^ Target entrypoint to process this job.
  , jnPayload :: Maybe BL.ByteString
  -- ^ Optional binary or JSON payload.
  , jnPriority :: Int
  -- ^ Execution priority (lower values dequeue earlier).
  , jnExecuteAfter :: Maybe NominalDiffTime
  -- ^ Optional execution delay.
  , jnDedupeKey :: Maybe Text
  -- ^ Optional deduplication key.
  , jnHeaders :: Maybe Value
  -- ^ Optional metadata headers.
  , jnParentState :: Maybe Value
  -- ^ Initial state passed down from a parent job.
  }
  deriving stock (Show, Eq)

-- | A tree of jobs where the parent node waits for all child subtrees to complete.
data JobTree = JobTree JobNode [JobTree]
  deriving stock (Show, Eq)

-- | Infix operator to construct a 't:JobTree' from a parent node and list of child trees.
(<~~) :: JobNode -> [JobTree] -> JobTree
(<~~) = JobTree

infixr 5 <~~

{- | Insert a 't:JobTree' into the database queue.
If the tree has child jobs, the parent is inserted with 'Held' status and
will only transition to 'Queued' when all children have completed.
Entire tree insertion occurs within a single atomic database transaction.
-}
insertJobTree :: (MonadPGQueuer m) => JobTree -> m ()
insertJobTree tree = withTransaction $ go Nothing tree
  where
    go :: (MonadPGQueuer m) => Maybe JobId -> JobTree -> m ()
    go mParent (JobTree node children) = do
      let status = if null children then Queued else Held
      jobIds <-
        enqueue
          (jnEntrypoint node)
          (jnPayload node)
          (jnPriority node)
          (jnExecuteAfter node)
          (jnDedupeKey node)
          (jnHeaders node)
          mParent
          (jnParentState node)
          status
      case listToMaybe jobIds of
        Nothing -> pure ()
        Just parentId -> foldM_ (\_ child -> go (Just parentId) child) () children
