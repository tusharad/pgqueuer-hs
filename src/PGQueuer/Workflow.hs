{-# LANGUAGE DerivingStrategies #-}

module PGQueuer.Workflow (
    JobNode (..),
    JobTree (..),
    (<~~),
    insertJobTree,
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

-- | A single node in a JobTree, containing parameters for a job.
data JobNode = JobNode
    { jnEntrypoint :: Entrypoint
    , jnPayload :: Maybe BL.ByteString
    , jnPriority :: Int
    , jnExecuteAfter :: Maybe NominalDiffTime
    , jnDedupeKey :: Maybe Text
    , jnHeaders :: Maybe Value
    , jnParentState :: Maybe Value
    }
    deriving stock (Show, Eq)

-- | A tree of jobs, where a parent waits for all its children to complete.
data JobTree = JobTree JobNode [JobTree]
    deriving stock (Show, Eq)

-- | Infix operator to create a JobTree from a parent node and a list of children.
(<~~) :: JobNode -> [JobTree] -> JobTree
(<~~) = JobTree

infixr 5 <~~

{- | Insert a JobTree into the queue.
If the tree has children, the parent is inserted as 'Held', and will be
moved to 'Queued' once all children have completed.
Entire insertion happens within a single transaction.
-}
insertJobTree :: (MonadPGQueuer m) => JobTree -> m ()
insertJobTree tree = withTransaction $ go Nothing tree
  where
    go :: (MonadPGQueuer m) => Maybe JobId -> JobTree -> m ()
    go mbParentId (JobTree node children) = do
        let isParent = not (null children)
            initialStatus = if isParent then Held else Queued

        -- Insert current node
        ids <-
            enqueue
                (jnEntrypoint node)
                (jnPayload node)
                (jnPriority node)
                (jnExecuteAfter node)
                (jnDedupeKey node)
                (jnHeaders node)
                mbParentId
                (jnParentState node)
                initialStatus

        case listToMaybe ids of
            Nothing -> pure ()
            Just jobId -> foldM_ (\_ child -> go (Just jobId) child) () children
